//! Kafka I/O — потребляет `stage.policy`, публикует `stage.completed`
//! (platform_contracts.md). Тот же паттерн, что
//! `services/destination-resolution-service/src/kafka_io.rs`: бизнес-логика
//! (`handle_command`) — чистая функция, тестируется без сети; `run_loop` —
//! реальная Kafka-обвязка, не интеграционно протестированная в этом окружении.
//!
//! Отличие от Destination Resolution: `PolicyExtension` несёт только
//! `resolved_operator_id` — `body`/`msisdn`/`sender_id` НЕ дублируются в
//! stage-командах (service_internal_methods.md §0), их обязан прочитать
//! `fetch_message_context` из Runtime Redis по `message_id`
//! (`msgctx:{message_id}`). `MessageContextStore` — трейт с реальной
//! Redis-реализацией (`RedisMessageContextStore`, компилируется, live не
//! проверена) и мок-реализацией для тестов.
//!
//! Осознанно не входит в этот срез: `RuntimeState` (consent-блэклисты,
//! spam-счётчик) — в реальном проде это тоже Runtime Redis, разделяемый
//! между репликами; здесь остаётся in-memory (см. `policy_engine.rs`) —
//! перенос на Redis такого же типа, что и у `MessageContextStore`, не
//! сделан в этом срезе ради объёма.

use crate::banwords::BanwordChecker;
use crate::policy_engine::{self, MessageContext, PolicyOutcome, PolicyRulesetConfig, RuntimeState};
use crate::proto::stage_completed_event::StageResult;
use crate::proto::stage_execute_command::StageExtension;
use crate::proto::{Outcome, PolicyResult, StageCompletedEvent, StageExecuteCommand};
use crate::template_matching::CompiledRuleset;
use async_trait::async_trait;
use chrono::Utc;
use prost::Message;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message as _;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::time::Duration;

pub const INPUT_TOPIC: &str = "stage.policy";
pub const OUTPUT_TOPIC: &str = "stage.completed";

#[async_trait]
pub trait MessageContextStore: Send + Sync {
    async fn fetch(&self, message_id: &str) -> Option<MessageContext>;
}

/// Реальная реализация — компилируется против настоящего `redis` крейта,
/// не проверена против живого Runtime Redis в этом окружении (см. README).
pub struct RedisMessageContextStore {
    client: redis::Client,
}

impl RedisMessageContextStore {
    pub fn new(redis_url: &str) -> Self {
        Self { client: redis::Client::open(redis_url).expect("невалидный REDIS_RUNTIME URL") }
    }
}

#[async_trait]
impl MessageContextStore for RedisMessageContextStore {
    async fn fetch(&self, message_id: &str) -> Option<MessageContext> {
        let mut conn = self.client.get_multiplexed_async_connection().await.ok()?;
        let key = format!("msgctx:{message_id}");
        let fields: std::collections::HashMap<String, String> =
            redis::AsyncCommands::hgetall(&mut conn, &key).await.ok()?;
        Some(MessageContext {
            msisdn: fields.get("msisdn")?.clone(),
            sender_id: fields.get("sender")?.clone(),
            body: fields.get("body")?.clone(),
        })
    }
}

pub fn build_consumer(bootstrap_servers: &str, group_id: &str) -> StreamConsumer {
    ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .set("group.id", group_id)
        .set("enable.auto.commit", "false")
        .create()
        .expect("не удалось создать Kafka consumer")
}

pub fn build_producer(bootstrap_servers: &str) -> FutureProducer {
    ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("не удалось создать Kafka producer")
}

/// Ядро обработки — чистая функция, тестируется без сети/Redis/Kafka.
pub fn handle_command(
    command: &StageExecuteCommand,
    ctx: &MessageContext,
    ruleset: &PolicyRulesetConfig,
    templates: &CompiledRuleset,
    banwords: &BanwordChecker,
    runtime: &mut RuntimeState,
) -> StageCompletedEvent {
    let now = Utc::now().naive_utc();
    let outcome = policy_engine::evaluate_policy(ctx, ruleset, templates, banwords, runtime, now);
    build_event(command, outcome)
}

fn build_event(command: &StageExecuteCommand, outcome: PolicyOutcome) -> StageCompletedEvent {
    let (kafka_outcome, reason_code) = match outcome.outcome {
        "SUCCEEDED" => (Outcome::Succeeded, String::new()),
        _ => (Outcome::Rejected, outcome.reason_code.unwrap_or_default().to_string()),
    };
    StageCompletedEvent {
        event_id: format!("evt-{}", command.stage_execution_id),
        message_id: command.message_id.clone(),
        stage_execution_id: command.stage_execution_id.clone(),
        attempt: command.attempt,
        stage_name: command.stage_name,
        outcome: kafka_outcome as i32,
        reason_code,
        retryable: false,
        retry_after: None,
        traceparent: command.traceparent.clone(),
        completed_at: None,
        // category всегда заполнена — и на SUCCEEDED (шаблон/UNTEMPLATED), и на REJECTED (BLOCKED),
        // Billing получает непустую category в обоих случаях (service_internal_methods.md §1.5).
        stage_result: Some(StageResult::Policy(PolicyResult { category: outcome.category })),
    }
}

pub async fn run_loop(
    consumer: StreamConsumer,
    producer: FutureProducer,
    context_store: Box<dyn MessageContextStore>,
    ruleset: PolicyRulesetConfig,
    templates: CompiledRuleset,
    banwords: BanwordChecker,
    mut runtime: RuntimeState,
) {
    consumer.subscribe(&[INPUT_TOPIC]).expect("не удалось подписаться на stage.policy");

    loop {
        match consumer.recv().await {
            Ok(msg) => {
                let Some(payload) = msg.payload() else {
                    tracing::warn!("получено сообщение без payload, пропущено");
                    continue;
                };
                let command = match StageExecuteCommand::decode(payload) {
                    Ok(c) => c,
                    Err(e) => {
                        tracing::error!("не удалось декодировать StageExecuteCommand: {e}");
                        continue;
                    }
                };
                if !matches!(command.stage_extension, Some(StageExtension::Policy(_))) {
                    tracing::error!("stage.policy команда без PolicyExtension, пропущена");
                    continue;
                }

                let Some(ctx) = context_store.fetch(&command.message_id).await else {
                    tracing::error!("не удалось получить MessageContext для {}", command.message_id);
                    continue; // не коммитим — at-least-once, переобработается
                };

                let event = handle_command(&command, &ctx, &ruleset, &templates, &banwords, &mut runtime);
                let bytes = event.encode_to_vec();
                let record = FutureRecord::to(OUTPUT_TOPIC).key(&event.message_id).payload(&bytes);
                if let Err((e, _)) = producer.send(record, Duration::from_secs(5)).await {
                    tracing::error!("не удалось опубликовать StageCompletedEvent: {e}");
                    continue;
                }

                if let Err(e) = consumer.commit_message(&msg, rdkafka::consumer::CommitMode::Async) {
                    tracing::error!("не удалось закоммитить offset: {e}");
                }
            }
            Err(e) => tracing::error!("ошибка Kafka consumer: {e}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::template_matching::Template;

    fn command(message_id: &str) -> StageExecuteCommand {
        StageExecuteCommand {
            event_id: "e1".into(),
            message_id: message_id.into(),
            channel: 0,
            pipeline_id: "p1".into(),
            pipeline_version: "1".into(),
            node_id: "n1".into(),
            stage_name: 2, // STAGE_NAME_POLICY
            stage_execution_id: "se1".into(),
            attempt: 1,
            deadline: None,
            message_ttl: None,
            config_versions: Default::default(),
            traceparent: "tp1".into(),
            payload_ref: None,
            stage_extension: Some(StageExtension::Policy(crate::proto::PolicyExtension {
                resolved_operator_id: "beeline".into(),
            })),
        }
    }

    fn env() -> (PolicyRulesetConfig, CompiledRuleset, BanwordChecker, RuntimeState) {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config_schemas/examples/policy_ruleset.valid.json");
        let json_str = std::fs::read_to_string(&path).unwrap();
        let ruleset = PolicyRulesetConfig::from_config_schema_json(&json_str);
        let template = Template {
            template_id: "tpl-contract-payment".into(),
            pattern: "%w shartnoma bo'yicha %d{1,6} so'm to'lovni bugun amalga oshiring".into(),
            category: "TRANSACTION".into(),
        };
        let templates = CompiledRuleset::new(vec![template]);
        let banwords = BanwordChecker::new(&ruleset.banwords);
        (ruleset, templates, banwords, RuntimeState::default())
    }

    #[test]
    fn resolved_match_produces_succeeded_with_category() {
        let (ruleset, templates, banwords, mut runtime) = env();
        let ctx = MessageContext {
            msisdn: "998901331835".into(),
            sender_id: "Click".into(),
            body: "Hello1238!@* shartnoma bo'yicha 123456 so'm to'lovni bugun amalga oshiring".into(),
        };
        let event = handle_command(&command("m1"), &ctx, &ruleset, &templates, &banwords, &mut runtime);
        assert_eq!(event.outcome, Outcome::Succeeded as i32);
        match event.stage_result {
            Some(StageResult::Policy(r)) => assert_eq!(r.category, "TRANSACTION"),
            other => panic!("ожидали PolicyResult, получили {other:?}"),
        }
        assert_eq!(event.reason_code, "");
    }

    #[test]
    fn rejected_carries_blocked_category_and_reason() {
        let (ruleset, templates, banwords, mut runtime) = env();
        let ctx = MessageContext { msisdn: "998901331835".into(), sender_id: "NotClick".into(), body: "irrelevant".into() };
        let event = handle_command(&command("m2"), &ctx, &ruleset, &templates, &banwords, &mut runtime);
        assert_eq!(event.outcome, Outcome::Rejected as i32);
        assert_eq!(event.reason_code, "INVALID_SENDER");
        match event.stage_result {
            Some(StageResult::Policy(r)) => assert_eq!(r.category, "BLOCKED"),
            other => panic!("ожидали PolicyResult(BLOCKED), получили {other:?}"),
        }
    }

    #[test]
    fn stage_execution_id_propagates_for_idempotency() {
        let (ruleset, templates, banwords, mut runtime) = env();
        let ctx = MessageContext { msisdn: "998901331835".into(), sender_id: "Click".into(), body: "irrelevant".into() };
        let event = handle_command(&command("m3"), &ctx, &ruleset, &templates, &banwords, &mut runtime);
        assert_eq!(event.stage_execution_id, "se1");
        assert_eq!(event.message_id, "m3");
    }
}
