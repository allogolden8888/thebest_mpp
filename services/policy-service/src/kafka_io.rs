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
use crate::config_reload::PolicyLiveState;
use crate::offset_tracker::{OffsetTracker, PartitionKey};
use crate::policy_engine::{self, MessageContext, PolicyOutcome, PolicyRulesetConfig, RuntimeState};
use crate::proto::stage_completed_event::StageResult;
use crate::proto::stage_execute_command::StageExtension;
use crate::proto::{Outcome, PolicyResult, StageCompletedEvent, StageExecuteCommand};
use crate::template_matching::CompiledRuleset;
use arc_swap::ArcSwap;
use async_trait::async_trait;
use chrono::{FixedOffset, Utc};
use prost::Message;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message as _;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::{Offset, TopicPartitionList};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{OnceCell, Semaphore};

fn concurrency_limit(env_var: &str, default: usize) -> usize {
    std::env::var(env_var).ok().and_then(|s| s.parse().ok()).filter(|n| *n > 0).unwrap_or(default)
}

/// Реальная находка (нагрузочный прогон, thread dump + `redis-cli`
/// подтвердили): без этого потолка одна зависшая задача (например
/// осиротевший Redis-future после разрыва/переподключения соединения)
/// НИКОГДА не вызывает `mark_done` — `OffsetTracker` навсегда замирает на
/// этом offset для этой партиции, ПОЛНОСТЬЮ МОЛЧА (ни одной строки в логах,
/// несмотря на `tracing::error!` в каждом другом пути отказа), при этом
/// сервис остаётся живым членом consumer group и `/readyz` продолжает
/// отвечать 200 — единственный способ узнать снаружи был вручную сравнить
/// lag по партициям. Таймаут не "чинит" сам зависший Redis-вызов, но
/// гарантирует, что задача завершится, освободит семафор и ГРОМКО
/// залогируется вместо бесконечного молчания — офсет всё равно не
/// коммитится (та же at-least-once семантика), просто следующий
/// ребаланс/рестарт увидит реальную причину в логах, а не тишину.
fn timeout_secs(env_var: &str, default: u64) -> Duration {
    Duration::from_secs(std::env::var(env_var).ok().and_then(|s| s.parse().ok()).filter(|n| *n > 0).unwrap_or(default))
}

fn commit_watermark(consumer: &StreamConsumer, key: &PartitionKey, next_offset: i64) {
    let mut tpl = TopicPartitionList::new();
    if let Err(e) = tpl.add_partition_offset(&key.topic, key.partition, Offset::Offset(next_offset)) {
        tracing::error!("commit_watermark: add_partition_offset {}:{} -> {next_offset}: {e}", key.topic, key.partition);
        return;
    }
    if let Err(e) = consumer.commit(&tpl, rdkafka::consumer::CommitMode::Async) {
        tracing::error!("commit_watermark: commit {}:{} -> {next_offset}: {e}", key.topic, key.partition);
    }
}

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
    // Реальная находка (нагрузочный прогон, тот же класс, что уже был
    // исправлен в pipeline-engine/redis_cas.rs и billing-service/
    // BillingAccountStore): fetch() раньше открывал НОВОЕ соединение на
    // КАЖДЫЙ вызов через get_multiplexed_async_connection() — под
    // конкурентной обработкой (см. run_loop) это стало бы TCP-хендшейком
    // на каждое сообщение вместо мультиплексирования через одно соединение.
    connection: OnceCell<redis::aio::MultiplexedConnection>,
}

impl RedisMessageContextStore {
    pub fn new(redis_url: &str) -> Self {
        Self {
            client: redis::Client::open(redis_url).expect("невалидный REDIS_RUNTIME URL"),
            connection: OnceCell::new(),
        }
    }

    async fn connection(&self) -> Option<redis::aio::MultiplexedConnection> {
        self.connection
            .get_or_try_init(|| async { self.client.get_multiplexed_async_connection().await })
            .await
            .ok()
            .cloned()
    }
}

#[async_trait]
impl MessageContextStore for RedisMessageContextStore {
    async fn fetch(&self, message_id: &str) -> Option<MessageContext> {
        let mut conn = self.connection().await?;
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

/// Реальная находка (то же расследование, что уже закрыло идентичный баг в
/// `pipeline-engine/src/kafka_io.rs::build_producer`, см. диагностику там):
/// без явного `queue.buffering.max.messages`/`queue.buffering.max.kbytes`
/// librdkafka использует свои дефолты (существенно ниже, чем нужно под
/// конкурентной нагрузкой этого сервиса — до `POLICY_CONCURRENCY` задач
/// одновременно зовут `producer.send`), и `FutureProducer::send()`
/// внутренне ретраит на `QueueFull` каждые ~100мс до `queue_timeout` —
/// под нагрузкой это давало на pipeline-engine стабильные ~1700мс (≈17
/// ретраев) НА КАЖДЫЙ produce, отдельно от message.timeout.ms. policy-service
/// использует ровно тот же паттерн `FutureProducer` + пул конкурентных задач
/// (см. `run_loop`), поэтому фикс применён здесь превентивно, а не только
/// по результатам instrumentation ниже.
pub fn build_producer(bootstrap_servers: &str) -> FutureProducer {
    ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .set("message.timeout.ms", "5000")
        .set("queue.buffering.max.messages", "1000000")
        .set("queue.buffering.max.kbytes", "2097151")
        .create()
        .expect("не удалось создать Kafka producer")
}

/// Asia/Tashkent — постоянный UTC+5, без перехода на летнее время с 1992 года
/// (в отличие от большинства зон, здесь не нужна полноценная tz-база вроде
/// `chrono-tz`, фиксированного смещения достаточно и корректно навсегда).
fn tashkent_offset() -> FixedOffset {
    FixedOffset::east_opt(5 * 3600).expect("+5:00 — валидное смещение")
}

/// Найдено кодревью, исправлено здесь: `policy_ruleset.valid.json` документирует
/// окна check_time_of_day в Asia/Tashkent local time (например "ADVERTISING
/// 09:00-20:00"), но раньше `handle_command` передавал в `evaluate_policy` чистый
/// UTC без конвертации — сдвиг на 5 часов, реально блокировавший/пропускавший
/// сообщения не в то окно суток. `evaluate_policy` использует `now` и для
/// check_time_of_day, и для окна анти-спама — постоянное смещение не меняет
/// разницу между двумя моментами времени, так что анти-спам-логика этим не
/// затронута.
pub fn now_tashkent() -> chrono::NaiveDateTime {
    Utc::now().with_timezone(&tashkent_offset()).naive_local()
}

/// Ядро обработки — чистая функция, тестируется без сети/Redis/Kafka.
/// `now` — Asia/Tashkent local time (см. `now_tashkent`), не UTC.
pub fn handle_command(
    command: &StageExecuteCommand,
    ctx: &MessageContext,
    ruleset: &PolicyRulesetConfig,
    templates: &CompiledRuleset,
    banwords: &BanwordChecker,
    runtime: &mut RuntimeState,
    now: chrono::NaiveDateTime,
) -> StageCompletedEvent {
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

/// Обработка одной записи — вынесена из цикла для конкурентной
/// spawned-задачи. `runtime` — `Mutex`, не `tokio::sync::Mutex`: критическая
/// секция (anti-spam счётчик, блэклисты) чисто in-memory, без `.await`
/// внутри, короткий std-lock безопасен и дешевле async-версии. Возвращает
/// `true`, если offset безопасно продвинуть.
async fn process_one_record(
    payload: &[u8],
    producer: &FutureProducer,
    context_store: &dyn MessageContextStore,
    policy: &PolicyLiveState,
    runtime: &Mutex<RuntimeState>,
) -> bool {
    let command = match StageExecuteCommand::decode(payload) {
        Ok(c) => c,
        Err(e) => {
            tracing::error!("не удалось декодировать StageExecuteCommand: {e}");
            return false;
        }
    };
    if !matches!(command.stage_extension, Some(StageExtension::Policy(_))) {
        tracing::error!("stage.policy команда без PolicyExtension, пропущена");
        return false;
    }

    let Some(ctx) = context_store.fetch(&command.message_id).await else {
        tracing::error!("не удалось получить MessageContext для {}", command.message_id);
        return false; // не коммитим — at-least-once, переобработается
    };

    let event = {
        let mut runtime = runtime.lock().expect("runtime mutex poisoned");
        handle_command(&command, &ctx, &policy.ruleset, &policy.templates, &policy.banwords, &mut runtime, now_tashkent())
    };

    let bytes = event.encode_to_vec();
    let record = FutureRecord::to(OUTPUT_TOPIC).key(&event.message_id).payload(&bytes);
    if let Err((e, _)) = producer.send(record, Duration::from_secs(5)).await {
        tracing::error!("не удалось опубликовать StageCompletedEvent: {e}");
        return false;
    }
    true
}

pub async fn run_loop(
    consumer: StreamConsumer,
    producer: FutureProducer,
    context_store: Arc<dyn MessageContextStore>,
    live_policy: Arc<ArcSwap<PolicyLiveState>>,
    runtime: RuntimeState,
) {
    consumer.subscribe(&[INPUT_TOPIC]).expect("не удалось подписаться на stage.policy");

    // Реальная находка (нагрузочный прогон): та же последовательная
    // обработка (Redis fetch -> Kafka produce -> следующая запись), что уже
    // была найдена и исправлена в pipeline-engine/destination-resolution-
    // service/billing-service. RuntimeState — единственное состояние
    // платформы этого среза, которое реально МУТИРУЕТСЯ на каждое
    // сообщение (anti-spam счётчик, блэклисты) — обёрнуто в Mutex, не
    // распараллелено само по себе (короткая in-memory критическая секция,
    // не узкое место), конкурентность даёт выигрыш на Redis fetch + Kafka
    // produce вокруг неё.
    let consumer = Arc::new(consumer);
    let semaphore = Arc::new(Semaphore::new(concurrency_limit("POLICY_CONCURRENCY", 256)));
    let tracker = Arc::new(OffsetTracker::new());
    let runtime = Arc::new(Mutex::new(runtime));
    let process_timeout = timeout_secs("POLICY_PROCESS_TIMEOUT_SECS", 30);

    loop {
        match consumer.recv().await {
            Ok(msg) => {
                let key = PartitionKey { topic: msg.topic().to_string(), partition: msg.partition() };
                let offset = msg.offset();
                tracker.observe_received(&key, offset);

                let Some(payload) = msg.payload() else {
                    tracing::warn!("получено сообщение без payload, пропущено");
                    if let Some(commit_to) = tracker.mark_done(&key, offset) {
                        commit_watermark(&consumer, &key, commit_to);
                    }
                    continue;
                };
                let payload = payload.to_vec();

                let permit = semaphore.clone().acquire_owned().await.expect("semaphore не должен закрываться");
                let consumer_task = consumer.clone();
                let producer_task = producer.clone();
                let context_store_task = context_store.clone();
                // load_full() — один атомарный снапшот на сообщение: ruleset/
                // templates/banwords обязаны быть согласованы друг с другом
                // (banwords производный от ruleset.banwords) — раздельные
                // ArcSwap на каждое поле рисковали бы увидеть новый ruleset
                // с ещё не пересобранными banwords в узком окне между двумя
                // независимыми store().
                let policy = live_policy.load_full();
                let runtime_task = runtime.clone();
                let tracker_task = tracker.clone();
                let key_task = key.clone();

                tokio::spawn(async move {
                    let _permit = permit;
                    let done = match tokio::time::timeout(
                        process_timeout,
                        process_one_record(&payload, &producer_task, context_store_task.as_ref(), &policy, &runtime_task),
                    )
                    .await
                    {
                        Ok(done) => done,
                        Err(_) => {
                            tracing::error!(
                                "process_one_record завис дольше {:?} (partition={} offset={offset}) — офсет НЕ коммитится, переобработается при следующем ребалансе/рестарте",
                                process_timeout, key_task.partition
                            );
                            false
                        }
                    };
                    if done {
                        if let Some(commit_to) = tracker_task.mark_done(&key_task, offset) {
                            commit_watermark(&consumer_task, &key_task, commit_to);
                        }
                    }
                });
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
        let event = handle_command(&command("m1"), &ctx, &ruleset, &templates, &banwords, &mut runtime, test_now());
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
        let event = handle_command(&command("m2"), &ctx, &ruleset, &templates, &banwords, &mut runtime, test_now());
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
        let event = handle_command(&command("m3"), &ctx, &ruleset, &templates, &banwords, &mut runtime, test_now());
        assert_eq!(event.stage_execution_id, "se1");
        assert_eq!(event.message_id, "m3");
    }

    fn test_now() -> chrono::NaiveDateTime {
        use chrono::NaiveDate;
        NaiveDate::from_ymd_opt(2026, 7, 22).unwrap().and_hms_opt(12, 0, 0).unwrap()
    }

    // Регрессия на находку кодревью: раньше `handle_command` передавал в
    // `evaluate_policy` чистый `Utc::now()`, а `policy_ruleset.valid.json`
    // документирует окна check_time_of_day в Asia/Tashkent (UTC+5) — 5-часовой
    // сдвиг мог и пропустить сообщение не в то окно, и заблокировать легитимное.
    // Здесь доказывается сама конвертация `now_tashkent()`, не бизнес-логика
    // check_time_of_day (та уже покрыта `policy_engine::tests::outside_time_window`
    // через инъекцию `now` напрямую).
    #[test]
    fn now_tashkent_is_five_hours_ahead_of_utc() {
        let utc_now = Utc::now();
        let tashkent_now = now_tashkent();
        let expected = utc_now.naive_utc() + chrono::Duration::hours(5);
        let diff = (tashkent_now - expected).num_seconds().abs();
        assert!(diff <= 2, "конвертация в Asia/Tashkent должна давать UTC+5, разница {diff}с слишком велика для дрожания часов теста");
    }
}
