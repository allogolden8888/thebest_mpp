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
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
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


/// `completed_at` — момент завершения стадии. Раньше все шесть
/// сервисов-стадий писали сюда `None`: поле объявлено в контракте, но не
/// заполнялось никем, из-за чего `analytics.stage_events` получала
/// `occurred_at = 1970-01-01` и пер-стадийные длительности были
/// структурно невычислимы (все разницы нулевые).
fn now_timestamp() -> Option<prost_types::Timestamp> {
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).ok()?;
    Some(prost_types::Timestamp { seconds: now.as_secs() as i64, nanos: now.subsec_nanos() as i32 })
}

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

/// Пул независимых producer'ов вместо одного общего.
///
/// ИЗМЕРЕНО на тихом стенде (Ryzen 5 7600X, 12 CPU; RYZEN_500TPS_FINDINGS.md):
/// в pipeline-engine один `FutureProducer`, обслуживавший ~768 конкурентных
/// tokio-тасков, стал последовательной точкой — на 500 TPS 79% вызовов
/// `send()` занимали >50мс (118344 из ~150000), e2e p95 2934мс. Пул из 4
/// независимых клиентов снизил долю медленных до 2.1%, e2e p95 до 940мс.
///
/// Причина не в переполнении локальной очереди (это чинилось отдельно через
/// `queue.buffering.max.messages`, см. `build_producer`), а в том, что
/// librdkafka держит ОДНО TCP-соединение и один внутренний I/O-поток на
/// брокер НА КЛИЕНТА, а `FutureProducer::clone()` — Arc на тот же клиент,
/// то есть параллелизма не добавляет. Простаивающие ядра не могут
/// распараллелить трафик через одно соединение — этим объясняется
/// наблюдавшаяся ранее полка по CPU при незакрытом бюджете латентности.
///
/// Здесь тот же паттерн и КРАТНО большая конкурентность: `DESTINATION_RESOLUTION_CONCURRENCY` по
/// умолчанию 1024 против 768 у pipeline-engine, и через этот сервис
/// проходит каждое сообщение.
///
/// Размер по умолчанию 4, а не больше: на 8 измерена РЕГРЕССИЯ (доля
/// медленных send выросла 2.1% -> 18%) — каждый нативный клиент librdkafka
/// приносит свои OS-потоки, и после некоторого порога это даёт
/// scheduling-контеншн внутри контейнера вместо параллелизма. Тот же класс,
/// что задокументирован для MAX_CONCURRENT_SUBMITS в
/// operator-smpp-session-manager.
pub struct ProducerPool {
    producers: Arc<Vec<FutureProducer>>,
    next: Arc<AtomicUsize>,
}

impl ProducerPool {
    pub fn new(bootstrap_servers: &str, size: usize) -> Self {
        let size = size.max(1);
        let producers = (0..size).map(|_| build_producer(bootstrap_servers)).collect();
        Self { producers: Arc::new(producers), next: Arc::new(AtomicUsize::new(0)) }
    }

    /// Round-robin. Упорядоченность в партиции не страдает: сервис публикует
    /// РОВНО ОДНО событие на входящую запись, поэтому двух одновременных
    /// публикаций с одним ключом не бывает по построению.
    pub fn pick(&self) -> FutureProducer {
        let idx = self.next.fetch_add(1, AtomicOrdering::Relaxed) % self.producers.len();
        self.producers[idx].clone()
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.producers.len()
    }
}

pub fn producer_pool_size() -> usize {
    std::env::var("DESTINATION_RESOLUTION_PRODUCER_POOL_SIZE").ok().and_then(|v| v.parse().ok()).filter(|v| *v > 0).unwrap_or(4)
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
            completed_at: now_timestamp(),
            // Фаза 11 плана закрытия API-пробелов: эхо command.sandbox.
            sandbox: command.sandbox,
            // Эхо command.partner_id — сервис не резолвит партнёра заново,
            // копирует из обрабатываемой команды. Без этого поля
            // analytics.stage_events писала пустой partner_id и отчёты не
            // группировались по партнёру.
            partner_id: command.partner_id.clone(),
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
        completed_at: now_timestamp(),
        sandbox: command.sandbox,
            // Эхо command.partner_id — сервис не резолвит партнёра заново,
            // копирует из обрабатываемой команды. Без этого поля
            // analytics.stage_events писала пустой partner_id и отчёты не
            // группировались по партнёру.
            partner_id: command.partner_id.clone(),
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

pub async fn run_loop(consumer: StreamConsumer, producer_pool: ProducerPool, live_snapshot: Arc<ArcSwap<Snapshot>>) {
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
                let producer_task = producer_pool.pick();
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
            sandbox: false, partner_id: String::new(),
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

    /// Фаза 11 плана закрытия API-пробелов: sandbox эхом переносится в
    /// событие для ОБОИХ путей (resolved и rejected) — build_rejected тоже
    /// проверяется, не только happy path.
    #[test]
    fn sandbox_flag_is_echoed_on_both_resolved_and_rejected_paths() {
        let mut resolved_command = command_with_destination("998901331835");
        resolved_command.sandbox = true;
        let event = handle_command(&snapshot(), &resolved_command);
        assert!(event.sandbox, "sandbox=true обязан попасть в событие на resolved-пути");

        let mut rejected_command = command_with_destination("998770000000");
        rejected_command.sandbox = true;
        let event = handle_command(&snapshot(), &rejected_command);
        assert!(event.sandbox, "sandbox=true обязан попасть в событие на rejected-пути (build_rejected)");
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
