//! Kafka I/O — потребляет `stage.routing`, публикует `stage.completed`.
//! Тот же паттерн, что у остальных сервисов этого среза: `handle_command` —
//! чистая функция (тестируется без сети), `run_loop` — реальная Kafka-обвязка,
//! не интеграционно проверенная в этом окружении.

use crate::offset_tracker::{OffsetTracker, PartitionKey};
use crate::proto::stage_completed_event::StageResult;
use crate::proto::stage_execute_command::StageExtension;
use crate::proto::{Outcome, Protocol, RoutingResult, StageCompletedEvent, StageExecuteCommand};
use crate::route_table::RouteTableSnapshot;
use crate::routing::{ControlSnapshot, RoutingError, resolve_final_route};
use arc_swap::ArcSwap;
use prost::Message;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message as _;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::{Offset, TopicPartitionList};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;

fn concurrency_limit(env_var: &str, default: usize) -> usize {
    std::env::var(env_var).ok().and_then(|s| s.parse().ok()).filter(|n| *n > 0).unwrap_or(default)
}

/// См. то же обоснование в policy-service/src/kafka_io.rs — без потолка
/// зависшая задача навсегда, ПОЛНОСТЬЮ МОЛЧА замирает watermark
/// `OffsetTracker` на этой партиции.
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

pub const INPUT_TOPIC: &str = "stage.routing";
pub const OUTPUT_TOPIC: &str = "stage.completed";

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
        // Тот же паттерн, что уже был найден и исправлен в pipeline-engine/src/kafka_io.rs
        // build_producer: run_loop здесь тоже гоняет до 256 конкурентных задач ("в полёте",
        // см. concurrency_limit("ROUTING_CONCURRENCY", 256) ниже) через ОДИН общий
        // FutureProducer. С librdkafka-дефолтом queue.buffering.max.messages (100000)
        // локальная очередь под такой конкурентностью упирается в QueueFull, а
        // rdkafka-rust's FutureProducer::send ретраит на QueueFull каждые 100мс вплоть до
        // queue_timeout — в pipeline-engine это давало стабильные ~1700мс на каждый
        // .send() под нагрузкой. Поднимаем превентивно, тем же значением, не дожидаясь
        // отдельного подтверждения на этом сервисе.
        .set("queue.buffering.max.messages", "1000000")
        .set("queue.buffering.max.kbytes", "2097151")
        .create()
        .expect("не удалось создать Kafka producer")
}

fn protocol_to_proto(protocol: &str) -> Protocol {
    match protocol {
        "SMPP" => Protocol::Smpp,
        "HTTP" => Protocol::Http,
        other => panic!("неизвестный protocol в routing_table: {other}"),
    }
}

pub fn handle_command(
    command: &StageExecuteCommand,
    snapshot: &RouteTableSnapshot,
    control: &ControlSnapshot,
) -> StageCompletedEvent {
    let resolved_operator_id = match &command.stage_extension {
        Some(StageExtension::Routing(ext)) => ext.resolved_operator_id.clone(),
        _ => return build_event(command, Outcome::Rejected, "MISSING_ROUTING_EXTENSION", None),
    };

    match resolve_final_route(snapshot, control, &resolved_operator_id) {
        Ok(route) => {
            let result = RoutingResult {
                route_id: route.route_id,
                protocol: protocol_to_proto(&route.protocol) as i32,
                route_version: route.route_version,
            };
            build_event(command, Outcome::Succeeded, "", Some(result))
        }
        // NO_HEALTHY_ROUTE ретраябельно — маршруты могут восстановиться (DEGRADED->ACTIVE,
        // PAUSED снят вручную); UNKNOWN_OPERATOR — нет, это конфигурационная ошибка.
        Err(RoutingError::NoHealthyRoute) => build_event(command, Outcome::Rejected, "NO_HEALTHY_ROUTE", None),
        Err(RoutingError::UnknownOperator) => build_event(command, Outcome::Rejected, "UNKNOWN_OPERATOR", None),
    }
}

fn build_event(command: &StageExecuteCommand, outcome: Outcome, reason_code: &str, result: Option<RoutingResult>) -> StageCompletedEvent {
    StageCompletedEvent {
        event_id: format!("evt-{}", command.stage_execution_id),
        message_id: command.message_id.clone(),
        stage_execution_id: command.stage_execution_id.clone(),
        attempt: command.attempt,
        stage_name: command.stage_name,
        outcome: outcome as i32,
        reason_code: reason_code.to_string(),
        retryable: matches!(outcome, Outcome::Rejected) && reason_code != "UNKNOWN_OPERATOR",
        retry_after: None,
        traceparent: command.traceparent.clone(),
        completed_at: None,
        stage_result: result.map(StageResult::Routing),
    }
}

/// Обработка одной записи — вынесена из цикла для конкурентной spawned-задачи.
/// Возвращает `true`, если offset безопасно продвинуть.
async fn process_one_record(
    payload: &[u8],
    producer: &FutureProducer,
    snapshot: &RouteTableSnapshot,
    control: &ControlSnapshot,
) -> bool {
    let command = match StageExecuteCommand::decode(payload) {
        Ok(c) => c,
        Err(e) => {
            tracing::error!("не удалось декодировать StageExecuteCommand: {e}");
            return false;
        }
    };

    let event = handle_command(&command, snapshot, control);

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
    live_snapshot: Arc<ArcSwap<RouteTableSnapshot>>,
    control: ControlSnapshot,
) {
    consumer.subscribe(&[INPUT_TOPIC]).expect("не удалось подписаться на stage.routing");

    // Реальная находка (нагрузочный прогон 1000 msg/s): handle_command здесь
    // чисто in-memory (ArcSwap-снапшот + ControlSnapshot), ни одного Redis
    // round-trip — но последовательный Kafka produce+commit (одна запись за
    // раз) сам по себе оказался узким местом, тот же класс находки, что уже
    // был исправлен в destination-resolution-service/kafka_io.rs.
    let consumer = Arc::new(consumer);
    let control = Arc::new(control);
    let semaphore = Arc::new(Semaphore::new(concurrency_limit("ROUTING_CONCURRENCY", 256)));
    let tracker = Arc::new(OffsetTracker::new());
    let process_timeout = timeout_secs("ROUTING_PROCESS_TIMEOUT_SECS", 30);

    loop {
        match consumer.recv().await {
            Ok(msg) => {
                let key = PartitionKey { topic: msg.topic().to_string(), partition: msg.partition() };
                let offset = msg.offset();
                tracker.observe_received(&key, offset);

                let Some(payload) = msg.payload() else {
                    if let Some(commit_to) = tracker.mark_done(&key, offset) {
                        commit_watermark(&consumer, &key, commit_to);
                    }
                    continue;
                };
                let payload = payload.to_vec();

                let permit = semaphore.clone().acquire_owned().await.expect("semaphore не должен закрываться");
                let consumer_task = consumer.clone();
                let producer_task = producer.clone();
                let snapshot = live_snapshot.load_full();
                let control_task = control.clone();
                let tracker_task = tracker.clone();
                let key_task = key.clone();

                tokio::spawn(async move {
                    let _permit = permit;
                    let done = match tokio::time::timeout(
                        process_timeout,
                        process_one_record(&payload, &producer_task, &snapshot, &control_task),
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
    use crate::route_table::RouteTable;

    fn snapshot() -> RouteTableSnapshot {
        let json = include_str!("../../../config_schemas/examples/routing_table.valid.json");
        let table: RouteTable = serde_json::from_str(json).unwrap();
        RouteTableSnapshot::from_tables(vec![table])
    }

    fn command(resolved_operator_id: &str) -> StageExecuteCommand {
        StageExecuteCommand {
            event_id: "e1".into(),
            message_id: "m1".into(),
            channel: 0,
            pipeline_id: "p1".into(),
            pipeline_version: "1".into(),
            node_id: "n1".into(),
            stage_name: 4, // STAGE_NAME_ROUTING
            stage_execution_id: "se1".into(),
            attempt: 1,
            deadline: None,
            message_ttl: None,
            config_versions: Default::default(),
            traceparent: "tp1".into(),
            payload_ref: None,
            stage_extension: Some(StageExtension::Routing(crate::proto::RoutingExtension {
                resolved_operator_id: resolved_operator_id.to_string(),
            })),
        }
    }

    #[test]
    fn healthy_operator_produces_succeeded_with_route() {
        let event = handle_command(&command("beeline_uz"), &snapshot(), &ControlSnapshot::new());
        assert_eq!(event.outcome, Outcome::Succeeded as i32);
        match event.stage_result {
            Some(StageResult::Routing(r)) => {
                assert_eq!(r.route_id, "beeline_smpp_primary");
                assert_eq!(r.protocol, Protocol::Smpp as i32);
                assert_eq!(r.route_version, "3");
            }
            other => panic!("ожидали RoutingResult, получили {other:?}"),
        }
    }

    #[test]
    fn unknown_operator_produces_non_retryable_rejection() {
        let event = handle_command(&command("no_such_operator"), &snapshot(), &ControlSnapshot::new());
        assert_eq!(event.outcome, Outcome::Rejected as i32);
        assert_eq!(event.reason_code, "UNKNOWN_OPERATOR");
        assert!(!event.retryable, "конфигурационная ошибка — повтор не поможет без изменения конфигурации");
    }

    #[test]
    fn all_routes_down_produces_retryable_rejection() {
        let mut control = ControlSnapshot::new();
        control.insert("beeline_smpp_primary".to_string(), crate::routing::ControlState::Paused);
        control.insert("beeline_http_reserve".to_string(), crate::routing::ControlState::Paused);
        let event = handle_command(&command("beeline_uz"), &snapshot(), &control);
        assert_eq!(event.outcome, Outcome::Rejected as i32);
        assert_eq!(event.reason_code, "NO_HEALTHY_ROUTE");
        assert!(event.retryable, "маршруты могут восстановиться — стоит повторить");
    }
}
