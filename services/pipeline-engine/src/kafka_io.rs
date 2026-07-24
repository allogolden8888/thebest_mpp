//! Kafka I/O — двойной консьюмер: `incoming.messages` (запускает пайплайн)
//! и `stage.completed` (продвигает его). Публикует в `stage.<name>`
//! (динамический топик по имени стадии, infra/kafka/generate_kafka_topics.py)
//! либо ничего не публикует при `Decision::Terminal`/`Decision::Ignored`.
//!
//! **Явное упрощение этого среза:** `ExecutionState` хранится в
//! `Arc<Mutex<HashMap>>` в памяти процесса, не в Runtime Redis с атомарным
//! CAS (`cas_transition_and_track_deadline`, service_internal_methods.md
//! §1.4) — то же самое ограничение, что `BillingAccountStore` в Billing
//! Service (read-then-write, не одна атомарная операция), тот же
//! Lua-скрипт из `development_plan.md` 4.2 закрыл бы оба сразу. При
//! нескольких репликах Pipeline Engine (`k8s/generate_manifests.py`: 17
//! инстансов) состояние **не разделяется** между ними — сообщение,
//! обработанное одной репликой, невидимо для другой. Это не тихий
//! пробел: без Redis-бэкенда сервис в этом виде физически не может
//! работать с более чем одной репликой корректно — зафиксировано как
//! приоритетный блокер перед Фазой 2.3/2.4 в README, не спрятано.

use crate::build_stage_execute::build_stage_execute;
use crate::execution_state::{Decision, ExecutionState, NextStageDecision, handle_stage_completed};
use crate::pipeline_graph::PipelineDefinition;
use crate::proto::common::StageCompletedEvent;
use crate::proto::events::IncomingMessage;
use crate::proto::events::incoming_message::Body as IncomingBody;
use prost::Message;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message as _;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use uuid::Uuid;

pub const INCOMING_TOPIC: &str = "incoming.messages";
pub const COMPLETED_TOPIC: &str = "stage.completed";

pub type StateStore = Arc<Mutex<HashMap<String, ExecutionState>>>;

fn stage_topic(stage_name: &str) -> Option<&'static str> {
    match stage_name {
        "DESTINATION_RESOLUTION" => Some("stage.destination-resolution"),
        "POLICY" => Some("stage.policy"),
        "BILLING" => Some("stage.billing"),
        "ROUTING" => Some("stage.routing"),
        "DELIVERY" => Some("stage.delivery"),
        "DELIVERY_RECONCILIATION" => Some("stage.delivery-reconciliation"),
        _ => None,
    }
}

pub fn build_consumer(bootstrap_servers: &str, group_id: &str, topic: &str) -> StreamConsumer {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .set("group.id", group_id)
        .set("enable.auto.commit", "false")
        .create()
        .expect("не удалось создать Kafka consumer");
    consumer.subscribe(&[topic]).expect("не удалось подписаться");
    consumer
}

pub fn build_producer(bootstrap_servers: &str) -> FutureProducer {
    ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("не удалось создать Kafka producer")
}

/// `handle_incoming` + `cache_message_context` (упрощённо, без Runtime Redis —
/// destination_address несётся напрямую, не читается по ссылке) + первая
/// диспетчеризация. Чистая функция — тестируется без сети.
///
/// **Идемпотентность на входе — вызывающая сторона (`run_incoming_loop`)
/// обязана проверить, что для этого `message_id` ещё нет состояния, ДО
/// вызова этой функции.** Сама функция всегда строит состояние с нуля —
/// найдено кодревью: если бы эта проверка не делалась нигде, редоставленный
/// (at-least-once) `incoming.messages` откатывал бы уже продвинувшийся
/// пайплайн обратно на `DestinationResolution` со свежим `stage_execution_id`,
/// что могло привести к повторному списанию в Billing (тот дедуплицирует по
/// `stage_execution_id`, а он был бы уже другим).
pub fn handle_incoming(incoming: &IncomingMessage, pipeline: &PipelineDefinition) -> Result<(ExecutionState, String, crate::proto::common::StageExecuteCommand), String> {
    let (destination_address, segment_count) = match &incoming.body {
        Some(IncomingBody::Sms(sms)) => (sms.msisdn.clone(), sms.segment_count),
        _ => return Err("только SMS поддержан в этом срезе (channel != SMS вне скоупа, hld.md §2)".to_string()),
    };

    let mut state = ExecutionState::new_from_incoming(incoming.message_id.clone(), pipeline, segment_count);
    let entry = pipeline.entry_node();
    let stage_execution_id = Uuid::new_v4().to_string();
    state.awaiting_stage_execution_id = Some(stage_execution_id.clone());
    let decision = NextStageDecision { node_id: entry.node_id.clone(), stage_name: entry.stage_name.clone() };
    let command = build_stage_execute(&decision, &state, &destination_address, stage_execution_id)?;
    Ok((state, destination_address, command))
}

#[derive(Debug, PartialEq)]
pub enum AdvanceOutcome {
    Next(String, crate::proto::common::StageExecuteCommand),
    Terminal,
    /// Устаревшее/дублирующееся событие — состояние не тронуто, ничего не публикуется,
    /// пайплайн НЕ считается завершённым (в отличие от Terminal).
    Ignored,
}

/// `handle_stage_completed` + `build_stage_execute` для следующего шага, либо
/// `finalize_pipeline` (Terminal), либо игнорирование устаревшего события.
/// Чистая функция — состояние передаётся по ссылке явно, не читается из
/// глобального стора внутри.
pub fn advance(
    state: &mut ExecutionState,
    pipeline: &PipelineDefinition,
    event: &StageCompletedEvent,
    destination_address: &str,
) -> Result<AdvanceOutcome, String> {
    match handle_stage_completed(state, pipeline, event) {
        Decision::Next(decision) => {
            let topic = stage_topic(&decision.stage_name).ok_or_else(|| format!("неизвестный stage_name в графе: {}", decision.stage_name))?;
            let stage_execution_id = Uuid::new_v4().to_string();
            state.awaiting_stage_execution_id = Some(stage_execution_id.clone());
            let command = build_stage_execute(&decision, state, destination_address, stage_execution_id)?;
            Ok(AdvanceOutcome::Next(topic.to_string(), command))
        }
        Decision::Terminal => Ok(AdvanceOutcome::Terminal), // finalize_pipeline — освобождение состояния (см. run_completed_loop)
        Decision::Ignored { reason } => {
            tracing::warn!("StageCompletedEvent проигнорирован для message_id={}: {reason}", state.message_id);
            Ok(AdvanceOutcome::Ignored)
        }
    }
}

/// message_id -> destination_address — упрощённый "msgctx" (в проде это
/// поле Runtime Redis `msgctx:{message_id}`, читаемое по ссылке
/// (`StagePayloadRef`), не хранится вторым отдельным местом — см. README).
pub type DestinationStore = Arc<Mutex<HashMap<String, String>>>;

/// Потребляет `incoming.messages`, запускает пайплайн для каждого сообщения.
pub async fn run_incoming_loop(
    consumer: StreamConsumer,
    producer: FutureProducer,
    pipeline: Arc<PipelineDefinition>,
    store: StateStore,
    destinations: DestinationStore,
) {
    loop {
        match consumer.recv().await {
            Ok(msg) => {
                let Some(payload) = msg.payload() else { continue };
                let Ok(incoming) = IncomingMessage::decode(payload) else {
                    tracing::error!("не удалось декодировать IncomingMessage");
                    continue;
                };

                // Идемпотентность: если для этого message_id уже есть состояние
                // (пайплайн уже запущен и ещё не завершился), это — редоставленное
                // at-least-once сообщение, не новое. Пропускаем, не перезапускаем.
                // Остаточный пробел (см. README): если пайплайн УЖЕ завершился и
                // состояние удалено (finalize_pipeline), редоставленное сообщение
                // после этого не будет отловлено этой проверкой — нужен персистентный
                // журнал "уже обработано", не только in-memory карта активных.
                if store.lock().unwrap().contains_key(&incoming.message_id) {
                    tracing::warn!("incoming.messages повтор для уже активного message_id={}, пропущено", incoming.message_id);
                    if let Err(e) = consumer.commit_message(&msg, rdkafka::consumer::CommitMode::Async) {
                        tracing::error!("не удалось закоммитить offset (incoming.messages, дубликат): {e}");
                    }
                    continue;
                }

                let (state, destination_address, command) = match handle_incoming(&incoming, &pipeline) {
                    Ok(v) => v,
                    Err(e) => {
                        tracing::error!("не удалось обработать IncomingMessage message_id={}: {e}", incoming.message_id);
                        continue;
                    }
                };
                let Some(topic) = stage_topic(&pipeline.entry_node().stage_name) else {
                    tracing::error!("entry_node несёт неизвестный stage_name — конфигурация пайплайна повреждена");
                    continue;
                };
                destinations.lock().unwrap().insert(state.message_id.clone(), destination_address);
                store.lock().unwrap().insert(state.message_id.clone(), state);

                let bytes = command.encode_to_vec();
                let record = FutureRecord::to(topic).key(&command.message_id).payload(&bytes);
                if let Err((e, _)) = producer.send(record, Duration::from_secs(5)).await {
                    tracing::error!("не удалось опубликовать первую StageExecuteCommand: {e}");
                    continue;
                }
                if let Err(e) = consumer.commit_message(&msg, rdkafka::consumer::CommitMode::Async) {
                    tracing::error!("не удалось закоммитить offset (incoming.messages): {e}");
                }
            }
            Err(e) => tracing::error!("ошибка incoming.messages consumer: {e}"),
        }
    }
}

/// Потребляет `stage.completed`, продвигает состояние существующих пайплайнов.
pub async fn run_completed_loop(
    consumer: StreamConsumer,
    producer: FutureProducer,
    pipeline: Arc<PipelineDefinition>,
    store: StateStore,
    destinations: DestinationStore,
) {
    loop {
        match consumer.recv().await {
            Ok(msg) => {
                let Some(payload) = msg.payload() else { continue };
                let Ok(event) = StageCompletedEvent::decode(payload) else {
                    tracing::error!("не удалось декодировать StageCompletedEvent");
                    continue;
                };

                let mut state = match store.lock().unwrap().get(&event.message_id).cloned() {
                    Some(s) => s,
                    None => {
                        tracing::error!("нет ExecutionState для message_id={} (in-memory store, см. README про ограничение)", event.message_id);
                        continue;
                    }
                };
                let destination_address = destinations.lock().unwrap().get(&event.message_id).cloned().unwrap_or_default();

                match advance(&mut state, &pipeline, &event, &destination_address) {
                    Ok(AdvanceOutcome::Next(topic, command)) => {
                        store.lock().unwrap().insert(state.message_id.clone(), state);
                        let bytes = command.encode_to_vec();
                        let record = FutureRecord::to(&topic).key(&command.message_id).payload(&bytes);
                        if let Err((e, _)) = producer.send(record, Duration::from_secs(5)).await {
                            tracing::error!("не удалось опубликовать StageExecuteCommand: {e}");
                            continue;
                        }
                    }
                    Ok(AdvanceOutcome::Terminal) => {
                        // finalize_pipeline — освобождение состояния (TTL в проде, здесь — немедленно).
                        store.lock().unwrap().remove(&event.message_id);
                        destinations.lock().unwrap().remove(&event.message_id);
                    }
                    Ok(AdvanceOutcome::Ignored) => {
                        // Устаревшее/дублирующееся событие — состояние не трогаем, просто коммитим offset ниже.
                    }
                    Err(e) => {
                        tracing::error!("не удалось продвинуть пайплайн для message_id={}: {e}", event.message_id);
                        continue; // offset не коммитится — переобработается
                    }
                }

                if let Err(e) = consumer.commit_message(&msg, rdkafka::consumer::CommitMode::Async) {
                    tracing::error!("не удалось закоммитить offset (stage.completed): {e}");
                }
            }
            Err(e) => tracing::error!("ошибка stage.completed consumer: {e}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::common::SmsPayload;
    use crate::proto::common::stage_completed_event::StageResult;
    use crate::proto::common::{DestinationResolutionResult, Outcome, StageName};

    fn pipeline() -> PipelineDefinition {
        serde_json::from_str(include_str!("../../../config_schemas/examples/pipeline.valid.json")).unwrap()
    }

    fn incoming_sms(message_id: &str, msisdn: &str, segment_count: i32) -> IncomingMessage {
        IncomingMessage {
            message_id: message_id.into(),
            trace_id: "t1".into(),
            channel: 0,
            partner_id: "click_uz".into(),
            application_id: "app1".into(),
            received_at: None,
            message_ttl: None,
            body: Some(IncomingBody::Sms(SmsPayload {
                msisdn: msisdn.into(),
                sender: "Click".into(),
                body: "hello".into(),
                encoding: "GSM7".into(),
                segment_count,
            })),
        }
    }

    #[test]
    fn handle_incoming_starts_at_destination_resolution_with_msisdn() {
        let pipeline = pipeline();
        let incoming = incoming_sms("m1", "998901331835", 2);
        let (state, destination_address, command) = handle_incoming(&incoming, &pipeline).unwrap();
        assert_eq!(state.current_node_id, "n1_destination_resolution");
        assert_eq!(state.segment_count, 2);
        assert_eq!(destination_address, "998901331835");
        assert!(state.awaiting_stage_execution_id.is_some(), "должны ждать конкретный stage_execution_id, не любое событие");
        match command.stage_extension {
            Some(crate::proto::common::stage_execute_command::StageExtension::DestinationResolution(ext)) => {
                assert_eq!(ext.destination_address, "998901331835");
            }
            other => panic!("ожидали DestinationResolutionExtension, получили {other:?}"),
        }
    }

    #[test]
    fn advance_after_destination_resolution_targets_stage_policy_topic() {
        let pipeline = pipeline();
        let incoming = incoming_sms("m1", "998901331835", 1);
        let (mut state, destination_address, _first_command) = handle_incoming(&incoming, &pipeline).unwrap();
        let dispatched_id = state.awaiting_stage_execution_id.clone().unwrap();

        let event = StageCompletedEvent {
            event_id: "e2".into(), message_id: "m1".into(), stage_execution_id: dispatched_id, attempt: 1,
            stage_name: StageName::DestinationResolution as i32, outcome: Outcome::Succeeded as i32, reason_code: String::new(), retryable: false,
            retry_after: None, traceparent: "tp1".into(), completed_at: None,
            stage_result: Some(StageResult::DestinationResolution(DestinationResolutionResult { resolved_operator_id: "beeline".into() })),
        };

        match advance(&mut state, &pipeline, &event, &destination_address).unwrap() {
            AdvanceOutcome::Next(topic, command) => {
                assert_eq!(topic, "stage.policy");
                assert_eq!(state.current_node_id, "n2_policy");
                match command.stage_extension {
                    Some(crate::proto::common::stage_execute_command::StageExtension::Policy(ext)) => assert_eq!(ext.resolved_operator_id, "beeline"),
                    other => panic!("ожидали PolicyExtension, получили {other:?}"),
                }
            }
            other => panic!("ожидали Next, получили {other:?}"),
        }
    }

    #[test]
    fn advance_at_terminal_node_returns_terminal() {
        let pipeline = pipeline();
        let mut state = ExecutionState::new_from_incoming("m1".into(), &pipeline, 1);
        state.current_node_id = "n_billing_blocked".into();
        state.category = Some("BLOCKED".into());
        state.awaiting_stage_execution_id = Some("se1".into());

        let event = StageCompletedEvent {
            event_id: "e1".into(), message_id: "m1".into(), stage_execution_id: "se1".into(), attempt: 1,
            stage_name: StageName::Billing as i32, outcome: Outcome::Succeeded as i32, reason_code: String::new(), retryable: false,
            retry_after: None, traceparent: "tp1".into(), completed_at: None, stage_result: None,
        };
        assert_eq!(advance(&mut state, &pipeline, &event, "998901331835").unwrap(), AdvanceOutcome::Terminal);
    }

    #[test]
    fn advance_with_stale_event_returns_ignored_not_terminal() {
        // Найдено кодревью: устаревшее событие не должно ни продвигать, ни
        // завершать пайплайн — Ignored отличим от Terminal именно поэтому.
        let pipeline = pipeline();
        let mut state = ExecutionState::new_from_incoming("m1".into(), &pipeline, 1);
        state.awaiting_stage_execution_id = Some("se-real".into());

        let stale_event = StageCompletedEvent {
            event_id: "e-stale".into(), message_id: "m1".into(), stage_execution_id: "se-old".into(), attempt: 1,
            stage_name: StageName::DestinationResolution as i32, outcome: Outcome::Succeeded as i32, reason_code: String::new(), retryable: false,
            retry_after: None, traceparent: "tp1".into(), completed_at: None, stage_result: None,
        };
        assert_eq!(advance(&mut state, &pipeline, &stale_event, "998901331835").unwrap(), AdvanceOutcome::Ignored);
        assert_eq!(state.current_node_id, "n1_destination_resolution", "состояние не должно было измениться");
    }
}
