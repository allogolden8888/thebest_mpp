//! Kafka I/O — потребляет `stage.destination-resolution`, публикует
//! `stage.completed` (platform_contracts.md, топик→сообщение таблица).
//! Партиции/repl.factor этих топиков — infra/kafka/generate_kafka_topics.py.
//!
//! Осознанно не входит в этот срез: retry/backoff поверх транзиентных ошибок
//! брокера, DLQ на poison-message (stage.destination-resolution.dlq уже
//! спланирован в infra/kafka/, но producer сюда не подключён), обработка
//! consumer group rebalance за пределами того, что StreamConsumer делает
//! по умолчанию. Это первый реально компилируемый и тестируемый срез
//! (Фаза 2 development_plan.md), не готовая к продакшену реализация всех
//! отказоустойчивых сценариев.

use crate::offset_tracker::{OffsetTracker, PartitionKey};
use crate::proto::stage_completed_event::StageResult;
use crate::proto::stage_execute_command::StageExtension;
use crate::proto::{DestinationResolutionResult, Outcome, StageCompletedEvent, StageExecuteCommand};
use crate::resolver::{ResolveResult, Snapshot};
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

/// См. тот же таймаут в policy-service/src/kafka_io.rs для полного
/// обоснования: без него зависшая задача (например осиротевший
/// Kafka-produce future) навсегда, ПОЛНОСТЬЮ МОЛЧА замирает watermark
/// `OffsetTracker` на этой партиции — реальная находка нагрузочного
/// прогона (policy-service, тот же паттерн кода).
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

pub const INPUT_TOPIC: &str = "stage.destination-resolution";
pub const OUTPUT_TOPIC: &str = "stage.completed";

pub fn build_consumer(bootstrap_servers: &str, group_id: &str) -> StreamConsumer {
    ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .set("group.id", group_id)
        .set("enable.auto.commit", "false") // commit только после успешной публикации результата — at-least-once, не at-most-once
        .create()
        .expect("не удалось создать Kafka consumer")
}

pub fn build_producer(bootstrap_servers: &str) -> FutureProducer {
    ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .set("message.timeout.ms", "5000")
        // Превентивно, зеркалируя находку в pipeline-engine/src/kafka_io.rs
        // build_producer: тот же rdkafka::producer::FutureProducer, тот же
        // конкурентный пул задач через Semaphore (см. run_loop ниже, до 256
        // "в полёте" по умолчанию) на ОДИН общий producer. Там librdkafka-
        // дефолт queue.buffering.max.messages (100000) под такой
        // конкурентностью упирался в QueueFull, а FutureProducer::send
        // ретраит на QueueFull каждые 100мс вплоть до queue_timeout — давало
        // стабильные ~1700мс лишних на КАЖДОМ producer.send(). Здесь то же
        // сочетание (FutureProducer + Semaphore-пул), под нагрузкой этот
        // сервис — сильный кандидат на идентичный баг, даже если ручной
        // тест этого не показал. Подняли явно, с запасом.
        .set("queue.buffering.max.messages", "1000000")
        .set("queue.buffering.max.kbytes", "2097151")
        // NEXT_STEPS_1500TPS.md 1.1: тот же линг, что у остальных Rust-сервисов.
        .set("linger.ms", "5")
        .create()
        .expect("не удалось создать Kafka producer")
}

/// Ядро обработки одного сообщения — вынесено отдельно от Kafka-цикла,
/// чтобы быть тестируемым без реального брокера (см. tests ниже).
pub fn handle_command(snapshot: &Snapshot, command: &StageExecuteCommand) -> StageCompletedEvent {
    let destination_address = match &command.stage_extension {
        Some(StageExtension::DestinationResolution(ext)) => ext.destination_address.clone(),
        _ => {
            return build_rejected(command, "MISSING_DESTINATION_RESOLUTION_EXTENSION");
        }
    };

    match snapshot.resolve_operator_by_range(&destination_address) {
        ResolveResult::Resolved(operator_id) => StageCompletedEvent {
            event_id: format!("evt-{}", command.stage_execution_id),
            message_id: command.message_id.clone(),
            stage_execution_id: command.stage_execution_id.clone(),
            attempt: command.attempt,
            stage_name: command.stage_name,
            outcome: Outcome::Succeeded as i32,
            reason_code: String::new(),
            retryable: false,
            retry_after: None,
            traceparent: command.traceparent.clone(),
            completed_at: None,
            stage_result: Some(StageResult::DestinationResolution(DestinationResolutionResult {
                resolved_operator_id: operator_id,
            })),
        },
        ResolveResult::NotFound => build_rejected(command, "OPERATOR_NOT_FOUND"),
    }
}

fn build_rejected(command: &StageExecuteCommand, reason_code: &str) -> StageCompletedEvent {
    StageCompletedEvent {
        event_id: format!("evt-{}", command.stage_execution_id),
        message_id: command.message_id.clone(),
        stage_execution_id: command.stage_execution_id.clone(),
        attempt: command.attempt,
        stage_name: command.stage_name,
        outcome: Outcome::Rejected as i32,
        reason_code: reason_code.to_string(),
        retryable: false,
        retry_after: None,
        traceparent: command.traceparent.clone(),
        completed_at: None,
        stage_result: None,
    }
}

/// Обработка одной записи — вынесена из цикла, чтобы уходить в конкурентную
/// spawned-задачу без заимствования `consumer`/msg. Возвращает `true`, если
/// offset безопасно продвинуть.
async fn process_one_record(payload: &[u8], producer: &FutureProducer, snapshot: &Snapshot) -> bool {
    let command = match StageExecuteCommand::decode(payload) {
        Ok(c) => c,
        Err(e) => {
            tracing::error!("не удалось декодировать StageExecuteCommand: {e}");
            return false;
        }
    };

    let event = handle_command(snapshot, &command);
    let bytes = event.encode_to_vec();

    let record = FutureRecord::to(OUTPUT_TOPIC).key(&event.message_id).payload(&bytes);
    if let Err((e, _)) = producer.send(record, Duration::from_secs(5)).await {
        tracing::error!("не удалось опубликовать StageCompletedEvent: {e}");
        return false; // не коммитим offset — at-least-once, сообщение будет переобработано
    }
    true
}

pub async fn run_loop(consumer: StreamConsumer, producer: FutureProducer, live_snapshot: Arc<ArcSwap<Snapshot>>) {
    consumer
        .subscribe(&[INPUT_TOPIC])
        .expect("не удалось подписаться на stage.destination-resolution");

    // Реальная находка (нагрузочный прогон): раньше обработка была строго
    // последовательной — decode -> resolve (in-memory, быстро) -> Kafka
    // produce -> commit -> следующая запись. Этот сервис не делает ни
    // одного Redis round-trip (весь resolve — чистая память), но
    // последовательный Kafka produce+commit сам по себе оказался узким
    // местом на реальной нагрузке (после того, как pipeline-engine и
    // billing-service перестали быть боттлнеком первыми) — тот же паттерн,
    // что уже был найден и исправлен в pipeline-engine/kafka_io.rs.
    let consumer = Arc::new(consumer);
    let semaphore = Arc::new(Semaphore::new(concurrency_limit("DESTINATION_RESOLUTION_CONCURRENCY", 256)));
    let tracker = Arc::new(OffsetTracker::new());
    let process_timeout = timeout_secs("DESTINATION_RESOLUTION_PROCESS_TIMEOUT_SECS", 30);

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
                let snapshot = live_snapshot.load_full();
                let tracker_task = tracker.clone();
                let key_task = key.clone();

                tokio::spawn(async move {
                    let _permit = permit;
                    let done = match tokio::time::timeout(process_timeout, process_one_record(&payload, &producer_task, &snapshot)).await {
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

    fn snapshot() -> Snapshot {
        Snapshot::from_json_str(include_str!("../data/number_range_snapshot.json")).unwrap()
    }

    fn command_with_destination(destination_address: &str) -> StageExecuteCommand {
        StageExecuteCommand {
            event_id: "e1".into(),
            message_id: "m1".into(),
            channel: 0,
            pipeline_id: "p1".into(),
            pipeline_version: "1".into(),
            node_id: "n1".into(),
            stage_name: 1, // STAGE_NAME_DESTINATION_RESOLUTION
            stage_execution_id: "se1".into(),
            attempt: 1,
            deadline: None,
            message_ttl: None,
            config_versions: Default::default(),
            traceparent: "tp1".into(),
            payload_ref: None,
            stage_extension: Some(StageExtension::DestinationResolution(
                crate::proto::DestinationResolutionExtension {
                    destination_address: destination_address.to_string(),
                },
            )),
        }
    }

    #[test]
    fn resolved_destination_produces_succeeded_with_operator_id() {
        let event = handle_command(&snapshot(), &command_with_destination("998901331835"));
        assert_eq!(event.outcome, Outcome::Succeeded as i32);
        match event.stage_result {
            Some(StageResult::DestinationResolution(r)) => assert_eq!(r.resolved_operator_id, "beeline_uz"),
            other => panic!("ожидали DestinationResolution, получили {other:?}"),
        }
    }

    #[test]
    fn unresolved_destination_produces_rejected() {
        let event = handle_command(&snapshot(), &command_with_destination("998770000000"));
        assert_eq!(event.outcome, Outcome::Rejected as i32);
        assert_eq!(event.reason_code, "OPERATOR_NOT_FOUND");
        assert!(event.stage_result.is_none());
    }

    #[test]
    fn missing_extension_produces_rejected_not_panic() {
        let mut command = command_with_destination("998901331835");
        command.stage_extension = None;
        let event = handle_command(&snapshot(), &command);
        assert_eq!(event.outcome, Outcome::Rejected as i32);
        assert_eq!(event.reason_code, "MISSING_DESTINATION_RESOLUTION_EXTENSION");
    }

    #[test]
    fn stage_execution_id_and_message_id_propagate_for_idempotency_tracking() {
        let event = handle_command(&snapshot(), &command_with_destination("998901331835"));
        assert_eq!(event.stage_execution_id, "se1");
        assert_eq!(event.message_id, "m1");
    }
}
