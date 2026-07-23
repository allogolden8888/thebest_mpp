//! Kafka I/O — двойной консьюмер: `incoming.messages` (запускает пайплайн)
//! и `stage.completed` (продвигает его). Публикует в `stage.<name>`
//! (динамический топик по имени стадии, infra/kafka/generate_kafka_topics.py)
//! либо ничего не публикует при `Decision::Terminal`.
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
use crate::execution_state::{Decision, ExecutionState, handle_stage_completed};
use crate::pipeline_graph::PipelineDefinition;
use crate::proto::common::StageCompletedEvent;
use crate::proto::events::incoming_message::Body as IncomingBody;
use crate::proto::events::IncomingMessage;
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

fn stage_topic(stage_name: &str) -> &'static str {
    match stage_name {
        "DESTINATION_RESOLUTION" => "stage.destination-resolution",
        "POLICY" => "stage.policy",
        "BILLING" => "stage.billing",
        "ROUTING" => "stage.routing",
        "DELIVERY" => "stage.delivery",
        "DELIVERY_RECONCILIATION" => "stage.delivery-reconciliation",
        other => panic!("неизвестный stage_name: {other}"),
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

/// `handle_incoming` + `cache_message_context`(упрощённо, без Runtime Redis —
/// destination_address несётся напрямую, не читается по ссылке) + первая
/// диспетчеризация. Чистая функция — тестируется без сети.
pub fn handle_incoming(incoming: &IncomingMessage, pipeline: &PipelineDefinition) -> (ExecutionState, String, crate::proto::common::StageExecuteCommand) {
    let (destination_address, segment_count) = match &incoming.body {
        Some(IncomingBody::Sms(sms)) => (sms.msisdn.clone(), sms.segment_count),
        _ => panic!("только SMS поддержан в этом срезе (channel != SMS вне скоупа, hld.md §2)"),
    };

    let state = ExecutionState::new_from_incoming(incoming.message_id.clone(), pipeline, segment_count);
    let entry = pipeline.entry_node();
    let stage_execution_id = Uuid::new_v4().to_string();
    let decision = crate::execution_state::NextStageDecision { node_id: entry.node_id.clone(), stage_name: entry.stage_name.clone() };
    let command = build_stage_execute(&decision, &state, &destination_address, stage_execution_id);
    (state, destination_address, command)
}

/// `handle_stage_completed` + `build_stage_execute` для следующего шага, либо
/// `finalize_pipeline` (Terminal). Чистая функция — состояние передаётся по
/// значению/ссылке явно, не читается из глобального стора внутри.
pub fn advance(
    state: &mut ExecutionState,
    pipeline: &PipelineDefinition,
    event: &StageCompletedEvent,
    destination_address: &str,
) -> Option<(String, crate::proto::common::StageExecuteCommand)> {
    match handle_stage_completed(state, pipeline, event) {
        Decision::Next(decision) => {
            let topic = stage_topic(&decision.stage_name).to_string();
            let stage_execution_id = Uuid::new_v4().to_string();
            let command = build_stage_execute(&decision, state, destination_address, stage_execution_id);
            Some((topic, command))
        }
        Decision::Terminal => None, // finalize_pipeline — освобождение состояния (см. run_loop)
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
                let (state, destination_address, command) = handle_incoming(&incoming, &pipeline);
                let topic = stage_topic(&pipeline.entry_node().stage_name);
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
                    Some((topic, command)) => {
                        store.lock().unwrap().insert(state.message_id.clone(), state);
                        let bytes = command.encode_to_vec();
                        let record = FutureRecord::to(&topic).key(&command.message_id).payload(&bytes);
                        if let Err((e, _)) = producer.send(record, Duration::from_secs(5)).await {
                            tracing::error!("не удалось опубликовать StageExecuteCommand: {e}");
                            continue;
                        }
                    }
                    None => {
                        // finalize_pipeline — освобождение состояния (TTL в проде, здесь — немедленно).
                        store.lock().unwrap().remove(&event.message_id);
                        destinations.lock().unwrap().remove(&event.message_id);
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
    use crate::proto::common::stage_completed_event::StageResult;
    use crate::proto::common::{DestinationResolutionResult, Outcome};
    use crate::proto::common::SmsPayload;

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
        let (state, destination_address, command) = handle_incoming(&incoming, &pipeline);
        assert_eq!(state.current_node_id, "n1_destination_resolution");
        assert_eq!(state.segment_count, 2);
        assert_eq!(destination_address, "998901331835");
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
        let (mut state, destination_address, _first_command) = handle_incoming(&incoming, &pipeline);

        let event = StageCompletedEvent {
            event_id: "e2".into(), message_id: "m1".into(), stage_execution_id: "se1".into(), attempt: 1,
            stage_name: 0, outcome: Outcome::Succeeded as i32, reason_code: String::new(), retryable: false,
            retry_after: None, traceparent: "tp1".into(), completed_at: None,
            stage_result: Some(StageResult::DestinationResolution(DestinationResolutionResult { resolved_operator_id: "beeline".into() })),
        };

        let (topic, command) = advance(&mut state, &pipeline, &event, &destination_address).expect("должен быть следующий шаг, не Terminal");
        assert_eq!(topic, "stage.policy");
        assert_eq!(state.current_node_id, "n2_policy");
        match command.stage_extension {
            Some(crate::proto::common::stage_execute_command::StageExtension::Policy(ext)) => assert_eq!(ext.resolved_operator_id, "beeline"),
            other => panic!("ожидали PolicyExtension, получили {other:?}"),
        }
    }

    #[test]
    fn advance_at_terminal_node_returns_none() {
        let pipeline = pipeline();
        let mut state = ExecutionState::new_from_incoming("m1".into(), &pipeline, 1);
        state.current_node_id = "n_billing_blocked".into();
        state.category = Some("BLOCKED".into());

        let event = StageCompletedEvent {
            event_id: "e1".into(), message_id: "m1".into(), stage_execution_id: "se1".into(), attempt: 1,
            stage_name: 0, outcome: Outcome::Succeeded as i32, reason_code: String::new(), retryable: false,
            retry_after: None, traceparent: "tp1".into(), completed_at: None, stage_result: None,
        };
        assert_eq!(advance(&mut state, &pipeline, &event, "998901331835"), None);
    }
}
