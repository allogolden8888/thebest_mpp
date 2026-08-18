//! Kafka I/O — двойной консьюмер: `incoming.messages` (запускает пайплайн)
//! и `stage.completed` (продвигает его). Публикует в `stage.<name>`
//! (динамический топик по имени стадии, infra/kafka/generate_kafka_topics.py)
//! либо ничего не публикует при `Decision::Terminal`/`Decision::Ignored`.
//!
//! **development_plan.md 4.2 закрыто:** `ExecutionState` теперь хранится в
//! Runtime Redis через `cas_transition_and_track_deadline`
//! (`src/redis_cas.rs`), не в `Arc<Mutex<HashMap>>` — тот блокер
//! "не работает с более чем одной репликой", который был здесь
//! задокументирован, снят: состояние видимо любой реплике Pipeline Engine
//! одинаково, CAS-guard (`expected_awaiting_stage_execution_id`) защищает от
//! двух реплик, одновременно продвигающих один и тот же `message_id`. Сама
//! бизнес-логика (`handle_incoming`/`advance`, чистые функции) не изменилась
//! ни на строку — только слой хранения вокруг них.

use crate::build_stage_execute::build_stage_execute;
use crate::execution_state::{Decision, ExecutionState, NextStageDecision, handle_stage_completed};
use crate::offset_tracker::{OffsetTracker, PartitionKey};
use crate::pipeline_graph::PipelineDefinition;
use crate::proto::common::StageCompletedEvent;
use crate::proto::events::IncomingMessage;
use crate::proto::events::incoming_message::Body as IncomingBody;
use crate::redis_cas::{CasOutcome, RedisStateStore};
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
use uuid::Uuid;

/// Сколько записей одновременно "в полёте" (Redis round-trip + Kafka produce)
/// на каждый из двух consumer-циклов — реальная находка (нагрузочный
/// прогон): раньше обработка была строго последовательной (одна запись
/// полностью, включая сетевой round-trip, прежде чем читалась следующая),
/// что на практике давало ~6 msg/s пропускной способности вместо
/// ожидаемых сотен/тысяч — ни партиции (26), ни реплики этого не лечат,
/// раз узкое место — однопоточная обработка внутри ОДНОГО процесса.
/// Дефолт подобран с запасом (Redis+Kafka round-trip обычно <5мс локально),
/// переопределяем через env для тюнинга под реальную инфраструктуру.
fn concurrency_limit(env_var: &str, default: usize) -> usize {
    std::env::var(env_var).ok().and_then(|s| s.parse().ok()).filter(|n| *n > 0).unwrap_or(default)
}

/// См. тот же таймаут в policy-service/src/kafka_io.rs для полного
/// обоснования: без него зависшая задача (например осиротевший
/// Redis/Kafka future) навсегда, ПОЛНОСТЬЮ МОЛЧА замирает watermark
/// `OffsetTracker` на этой партиции — реальная находка нагрузочного
/// прогона (policy-service, тот же паттерн кода, здесь применён
/// превентивно к обоим циклам).
fn timeout_secs(env_var: &str, default: u64) -> Duration {
    Duration::from_secs(std::env::var(env_var).ok().and_then(|s| s.parse().ok()).filter(|n| *n > 0).unwrap_or(default))
}

/// `Offset::Offset(N)` — Kafka commit semantics: "следующая запись, которую
/// нужно прочитать", не "последняя обработанная" (то, что уже отражено в
/// `OffsetTracker::mark_done`, здесь только сам API-вызов).
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

pub const INCOMING_TOPIC: &str = "incoming.messages";
pub const COMPLETED_TOPIC: &str = "stage.completed";

/// Дефолт для `deadline_ms` — не задокументирован дословно нигде (сама
/// wire-таблица `StageExecuteCommand.deadline` тоже ещё не заполняется, это
/// отдельный, отдельно задокументированный пробел, см. README) — разумное
/// значение для внутреннего Critical Sweep трекинга: с запасом дольше
/// типичного round-trip любой стадии, короче, чем стоит ждать перед тем,
/// как считать стадию зависшей.
const DEFAULT_STAGE_TIMEOUT_MS: i64 = 30_000;

pub(crate) fn now_ms() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}

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
        // Реальная находка нагрузочного теста: этот producer обслуживает ДВА
        // высококонкурентных цикла (run_incoming_loop + run_completed_loop,
        // до 256 задач "в полёте" каждый — см. concurrency_limit) через ОДИН
        // общий producer. С librdkafka-дефолтом queue.buffering.max.messages
        // (100000) локальная очередь librdkafka под этой конкурентностью
        // упиралась в QueueFull чаще, чем казалось по абсолютным числам —
        // rdkafka-rust's FutureProducer::send ретраит с интервалом 100мс
        // (см. producer/future_producer.rs) вплоть до queue_timeout (5с,
        // второй аргумент .send() в этом файле), так что каждое попадание в
        // QueueFull стоило кратно 100мс — измеренный produce_ms стабильно
        // ~1700мс (17 ретраев) на КАЖДОМ вызове .send() под реальной
        // нагрузкой, что и было настоящей причиной многосекундной задержки
        // на каждом хопе пайплайна (не Kafka broker и не Redis — оба
        // измерены отдельно и быстрые). Подняли явно, с запасом.
        .set("queue.buffering.max.messages", "1000000")
        .set("queue.buffering.max.kbytes", "2097151")
        // NEXT_STEPS_1500TPS.md 1.1: linger.ms=0 (librdkafka default) значит
        // каждый produce() — отдельный запрос брокеру, даже под высокой
        // конкурентностью, где несколько вызовов реально готовы уйти вместе.
        // 5мс даёт брокеру собирать пачки без заметного вклада в p50/p95.
        .set("linger.ms", "5")
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

    let message_ttl_ms = incoming
        .message_ttl
        .as_ref()
        .map(|t| t.seconds * 1000 + (t.nanos as i64) / 1_000_000)
        .unwrap_or(i64::MAX); // отсутствующий TTL — не считаем сообщение истёкшим никогда
    let mut state =
        ExecutionState::new_from_incoming(incoming.message_id.clone(), pipeline, segment_count, incoming.priority_flag, message_ttl_ms);
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
    /// См. `Decision::RetryLater` — DELIVERY транзиентно отказал, TTL ещё
    /// не истёк. `state.attempt` уже увеличен, `awaiting_stage_execution_id`
    /// уже очищен (`handle_stage_completed`) — вызывающая сторона обязана
    /// CAS-сохранить это состояние (как при `Next`, но БЕЗ публикации в
    /// `stage.delivery` напрямую) и поставить задачу отложенного редиспатча
    /// в Scheduler Background Lane (см. run_completed_loop).
    RetryLater,
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
    now_ms: i64,
) -> Result<AdvanceOutcome, String> {
    match handle_stage_completed(state, pipeline, event, now_ms) {
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
        Decision::RetryLater => Ok(AdvanceOutcome::RetryLater),
    }
}

/// Потребляет `incoming.messages`, запускает пайплайн для каждого сообщения.
///
/// Идемпотентность на входе теперь обеспечивает сам CAS-вызов
/// (`cas_advance(None, ...)` — "ожидаем, что состояния ещё нет"), не
/// отдельная (потенциально гоняющаяся) проверка `contains_key` перед ним —
/// см. `cas_transition.lua`. Остаточный пробел прежний (см. README): если
/// пайплайн УЖЕ завершился и состояние удалено (`finalize_pipeline`),
/// редоставленное `incoming.messages` после этого не будет отловлено —
/// нужен персистентный журнал "уже обработано", не только текущее
/// активное состояние.
/// Обработка одной записи `incoming.messages` — вынесено из цикла, чтобы
/// уходить в конкурентную spawned-задачу без заимствования `consumer`/msg
/// (владеемые байты, не `BorrowedMessage`). Возвращает `true`, если запись
/// обработана до конца (успех ИЛИ окончательный отказ, не транзиентный) и
/// offset безопасно продвинуть, `false` — оставить неподтверждённым для
/// at-least-once переобработки (в точности прежняя семантика `continue`
/// без commit, просто теперь явно, не через управление потоком).
async fn process_one_incoming_record(
    payload: &[u8],
    producer: &FutureProducer,
    pipeline: &PipelineDefinition,
    store: &RedisStateStore,
) -> bool {
    let Ok(incoming) = IncomingMessage::decode(payload) else {
        tracing::error!("не удалось декодировать IncomingMessage");
        return false;
    };

    let (mut state, destination_address, command) = match handle_incoming(&incoming, pipeline) {
        Ok(v) => v,
        Err(e) => {
            tracing::error!("не удалось обработать IncomingMessage message_id={}: {e}", incoming.message_id);
            return false;
        }
    };
    let Some(topic) = stage_topic(&pipeline.entry_node().stage_name) else {
        tracing::error!("entry_node несёт неизвестный stage_name — конфигурация пайплайна повреждена");
        return false;
    };
    state.destination_address = destination_address;
    state.deadline_ms = now_ms() + DEFAULT_STAGE_TIMEOUT_MS;
    let stage_execution_id = state.awaiting_stage_execution_id.clone().unwrap_or_default();

    match store.cas_advance(None, None, &state).await {
        Ok(CasOutcome::Ok) => {}
        Ok(CasOutcome::Conflict { .. }) => {
            tracing::warn!("incoming.messages повтор для уже активного message_id={}, пропущено", incoming.message_id);
            return true; // как и раньше — коммитим, дубликат безопасно пропущен
        }
        Err(e) => {
            tracing::error!("не удалось записать начальное состояние в Runtime Redis для message_id={}: {e}", incoming.message_id);
            return false;
        }
    }

    let bytes = command.encode_to_vec();
    let record = FutureRecord::to(topic).key(&command.message_id).payload(&bytes);
    if let Err((e, _)) = producer.send(record, Duration::from_secs(5)).await {
        tracing::error!("не удалось опубликовать первую StageExecuteCommand: {e}");
        // Тот же откат, что был в последовательной версии: CAS уже создал
        // состояние с expected=None, но команда не опубликована — без
        // отката повторная обработка того же incoming.messages увидела бы
        // это состояние как "уже активное" и никогда не опубликовала бы
        // первую команду. CAS только что создал ИМЕННО этот ключ с нуля
        // (никто другой не мог успеть на него опереться), откат безопасен.
        if let Err(finalize_err) = store.finalize(&state.message_id, &stage_execution_id).await {
            tracing::error!("не удалось откатить состояние message_id={} после неудачной публикации: {finalize_err}", state.message_id);
        }
        return false;
    }
    true
}

pub async fn run_incoming_loop(
    consumer: StreamConsumer,
    producer: FutureProducer,
    pipeline: Arc<ArcSwap<PipelineDefinition>>,
    store: Arc<RedisStateStore>,
) {
    let consumer = Arc::new(consumer);
    let semaphore = Arc::new(Semaphore::new(concurrency_limit("PIPELINE_INCOMING_CONCURRENCY", 256)));
    let tracker = Arc::new(OffsetTracker::new());
    let process_timeout = timeout_secs("PIPELINE_INCOMING_PROCESS_TIMEOUT_SECS", 30);

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
                let payload = payload.to_vec(); // владеемые байты — переживают конец этой итерации в spawned-задаче

                                let permit = semaphore.clone().acquire_owned().await.expect("semaphore не должен закрываться");
                let consumer_task = consumer.clone();
                let producer_task = producer.clone();
                // Один снапшот на сообщение — все обращения к графу внутри
                // задачи (entry_node дважды: handle_incoming + stage_topic)
                // видят ровно ту же версию, даже если config_reload.rs
                // подменит граф между ними.
                let pipeline_snapshot = pipeline.load_full();
                let store_task = store.clone();
                let tracker_task = tracker.clone();
                let key_task = key.clone();

                tokio::spawn(async move {
                    let _permit = permit;
                    let done = match tokio::time::timeout(
                        process_timeout,
                        process_one_incoming_record(&payload, &producer_task, &pipeline_snapshot, &store_task),
                    )
                    .await
                    {
                        Ok(done) => done,
                        Err(_) => {
                            tracing::error!(
                                "process_one_incoming_record завис дольше {:?} (partition={} offset={offset}) — офсет НЕ коммитится, переобработается при следующем ребалансе/рестарте",
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
                    // done=false — не помечаем: watermark для этой партиции
                    // не продвигается дальше этого offset, at-least-once,
                    // запись переобработается после рестарта/rebalance —
                    // ровно та же семантика, что была у `continue` без
                    // commit в последовательной версии.
                });
            }
            Err(e) => tracing::error!("ошибка incoming.messages consumer: {e}"),
        }
    }
}

/// Потребляет `stage.completed`, продвигает состояние существующих пайплайнов.
///
/// **Известный, не новый пробел** (был и в in-memory версии, здесь не
/// решается — увеличение объёма правки за пределы `development_plan.md` 4.2):
/// если CAS-запись новой стадии в Redis проходит успешно, но последующая
/// публикация `StageExecuteCommand` для НЕЁ проваливается, откатить CAS
/// небезопасно (в отличие от `run_incoming_loop` — здесь между CAS и
/// публикацией другая реплика могла уже успеть опереться на новое
/// состояние), а редоставленное исходное `stage.completed`-событие на
/// повторной обработке уже не совпадёт с новым `awaiting_stage_execution_id`
/// и будет проигнорировано как устаревшее — тот же класс пробела, что уже
/// был в in-memory версии (там мутация в `HashMap` происходила так же
/// безусловно до публикации), не новый регресс от Redis-переноса.
/// Обработка одной записи `stage.completed` — та же логика, что раньше жила
/// прямо в цикле, вынесена в функцию по тем же причинам, что
/// `process_one_incoming_record` (конкурентная spawned-задача, владеемые
/// байты). Возвращает `true`, если offset безопасно продвинуть.
async fn process_one_completed_record(
    payload: &[u8],
    producer: &FutureProducer,
    pipeline: &PipelineDefinition,
    store: &RedisStateStore,
) -> bool {
    let Ok(event) = StageCompletedEvent::decode(payload) else {
        tracing::error!("не удалось декодировать StageCompletedEvent");
        return false;
    };

    let mut state = match store.load(&event.message_id).await {
        Ok(Some(s)) => s,
        Ok(None) => {
            tracing::error!("нет ExecutionState в Runtime Redis для message_id={} — либо ещё не создано, либо уже финализировано", event.message_id);
            return false;
        }
        Err(e) => {
            tracing::error!("не удалось прочитать ExecutionState для message_id={}: {e}", event.message_id);
            return false; // не коммитим — переобработается
        }
    };
    let expected_before = state.awaiting_stage_execution_id.clone();
    let destination_address = state.destination_address.clone();

    match advance(&mut state, pipeline, &event, &destination_address, now_ms()) {
        Ok(AdvanceOutcome::RetryLater) => {
            // `handle_stage_completed` уже увеличил attempt и очистил
            // awaiting_stage_execution_id (новый stage_execution_id
            // минтится позже, самим редиспатчем — см. Decision::RetryLater).
            // Здесь просто персистим это состояние (ZREM старого дедлайна,
            // ZADD нового пока нет — сейчас ничего не ожидается) и ставим
            // задачу отложенного редиспатча в Scheduler Background Lane.
            match store.cas_advance(expected_before.as_deref(), expected_before.as_deref(), &state).await {
                Ok(CasOutcome::Ok) => {}
                Ok(CasOutcome::Conflict { actual_awaiting }) => {
                    tracing::warn!(
                        "CAS-конфликт при retry-later для message_id={}: ожидали awaiting={:?}, реально={actual_awaiting}",
                        event.message_id, expected_before
                    );
                    return true;
                }
                Err(e) => {
                    tracing::error!("не удалось атомарно записать retry-later состояние для message_id={}: {e}", event.message_id);
                    return false;
                }
            }
            if let Err(e) = schedule_stage_retry(producer, &event.message_id, state.attempt).await {
                tracing::error!("не удалось поставить задачу отложенного retry для message_id={}: {e}", event.message_id);
                return false; // не коммитим — переобработается, попробуем поставить задачу снова
            }
            true
        }
        Ok(AdvanceOutcome::Next(topic, command)) => {
            state.deadline_ms = now_ms() + DEFAULT_STAGE_TIMEOUT_MS;
            match store.cas_advance(expected_before.as_deref(), expected_before.as_deref(), &state).await {
                Ok(CasOutcome::Ok) => {}
                Ok(CasOutcome::Conflict { actual_awaiting }) => {
                    tracing::warn!(
                        "CAS-конфликт при продвижении message_id={}: ожидали awaiting={:?}, реально={actual_awaiting} — другая реплика уже продвинула это состояние, событие проигнорировано",
                        event.message_id, expected_before
                    );
                    return true; // событие устарело, но обработка завершена — коммитим
                }
                Err(e) => {
                    tracing::error!("не удалось атомарно записать новое состояние для message_id={}: {e}", event.message_id);
                    return false; // не коммитим — переобработается
                }
            }
            let bytes = command.encode_to_vec();
            let record = FutureRecord::to(&topic).key(&command.message_id).payload(&bytes);
            if let Err((e, _)) = producer.send(record, Duration::from_secs(5)).await {
                tracing::error!("не удалось опубликовать StageExecuteCommand: {e}");
                return false;
            }
            true
        }
        Ok(AdvanceOutcome::Terminal) => {
            let expected = expected_before.as_deref().unwrap_or("");
            if let Err(e) = store.finalize(&event.message_id, expected).await {
                tracing::error!("не удалось финализировать message_id={}: {e}", event.message_id);
                return false; // не коммитим — переобработается
            }
            true
        }
        // Устаревшее/дублирующееся событие — состояние не трогаем, offset безопасно продвинуть.
        Ok(AdvanceOutcome::Ignored) => true,
        Err(e) => {
            tracing::error!("не удалось продвинуть пайплайн для message_id={}: {e}", event.message_id);
            false // offset не коммитится — переобработается
        }
    }
}

pub async fn run_completed_loop(
    consumer: StreamConsumer,
    producer: FutureProducer,
    pipeline: Arc<ArcSwap<PipelineDefinition>>,
    store: Arc<RedisStateStore>,
) {
    // Реальная находка (нагрузочный прогон 100 msg/s ingest): этот цикл
    // видит В ПЯТЬ РАЗ больше трафика, чем run_incoming_loop (одно
    // stage.completed на каждую из 5 реальных стадий на одно входящее
    // сообщение) — при последовательной обработке (recv -> Redis CAS ->
    // Kafka produce -> commit, одна запись за раз) именно этот цикл был
    // фактическим потолком пропускной способности всей платформы
    // (~6 msg/s вместо целевых десятков тысяч), несмотря на 26 партиций
    // топика и незадействованный запас Redis/Kafka.
    let consumer = Arc::new(consumer);
    let semaphore = Arc::new(Semaphore::new(concurrency_limit("PIPELINE_COMPLETED_CONCURRENCY", 256)));
    let tracker = Arc::new(OffsetTracker::new());
    let process_timeout = timeout_secs("PIPELINE_COMPLETED_PROCESS_TIMEOUT_SECS", 30);

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
                let pipeline_snapshot = pipeline.load_full();
                let store_task = store.clone();
                let tracker_task = tracker.clone();
                let key_task = key.clone();

                tokio::spawn(async move {
                    let _permit = permit;
                    let done = match tokio::time::timeout(
                        process_timeout,
                        process_one_completed_record(&payload, &producer_task, &pipeline_snapshot, &store_task),
                    )
                    .await
                    {
                        Ok(done) => done,
                        Err(_) => {
                            tracing::error!(
                                "process_one_completed_record завис дольше {:?} (partition={} offset={offset}) — офсет НЕ коммитится, переобработается при следующем ребалансе/рестарте",
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
            Err(e) => tracing::error!("ошибка stage.completed consumer: {e}"),
        }
    }
}

pub const SCHEDULER_BACKGROUND_COMMANDS_TOPIC: &str = "scheduler.background.commands";
pub const PIPELINE_RETRY_TRIGGERS_TOPIC: &str = "pipeline.retry.triggers";

fn env_i64(env_var: &str, default: i64) -> i64 {
    std::env::var(env_var).ok().and_then(|s| s.parse().ok()).filter(|n| *n > 0).unwrap_or(default)
}

/// Экспоненциальный backoff для STAGE_RETRY — `min(2^attempt * base, max)`.
/// `attempt` — уже увеличенный `ExecutionState.attempt` (см. Decision::RetryLater,
/// handle_stage_completed) — на первый ретрай приходит attempt=2 (исходная
/// попытка была attempt=1), так что первый бэкофф уже 4x базового, не 2x —
/// осознанно: TPS_THROTTLED/PACER_QUEUE_FULL обычно требуют больше, чем
/// одного base_delay, чтобы освободилось место в туннеле.
fn stage_retry_backoff_ms(attempt: i32) -> i64 {
    let base = env_i64("STAGE_RETRY_BASE_DELAY_MS", 1000);
    let max = env_i64("STAGE_RETRY_MAX_DELAY_MS", 30_000);
    2i64.saturating_pow(attempt.max(0) as u32).saturating_mul(base).min(max)
}

fn to_timestamp(ms: i64) -> prost_types::Timestamp {
    prost_types::Timestamp { seconds: ms / 1000, nanos: ((ms % 1000) * 1_000_000) as i32 }
}

/// Ставит `SchedulerBackgroundTask` (task_type=STAGE_RETRY) в
/// `scheduler.background.commands` — Scheduler Background Lane продержит
/// её до `due_at`, затем перепубликует в `pipeline.retry.triggers`
/// (см. `run_retry_trigger_loop` ниже), которую этот же сервис и
/// потребляет. `message_id`, не `source_event_id` — см.
/// `SchedulerBackgroundTask.message_id` в platform-contracts.
async fn schedule_stage_retry(producer: &FutureProducer, message_id: &str, attempt: i32) -> Result<(), String> {
    let due_at_ms = now_ms() + stage_retry_backoff_ms(attempt);
    let task = crate::proto::events::SchedulerBackgroundTask {
        task_type: crate::proto::common::BackgroundTaskType::StageRetry as i32,
        source_event_id: String::new(), // не используется для STAGE_RETRY — см. message_id ниже
        attempt,
        due_at: Some(to_timestamp(due_at_ms)),
        deadline: None,
        target_topic: PIPELINE_RETRY_TRIGGERS_TOPIC.to_string(),
        message_id: message_id.to_string(),
    };
    let bytes = task.encode_to_vec();
    let record = FutureRecord::to(SCHEDULER_BACKGROUND_COMMANDS_TOPIC).key(message_id).payload(&bytes);
    producer.send(record, Duration::from_secs(5)).await.map(|_| ()).map_err(|(e, _)| e.to_string())
}

/// Обработка одного `SchedulerBackgroundTask` из `pipeline.retry.triggers` —
/// когда Background Lane решил, что настало время повторить DELIVERY.
/// Возвращает `true`, если offset безопасно продвинуть.
///
/// Три защитные проверки перед реальным редиспатчем (в порядке важности):
/// 1. `current_node_id` всё ещё соответствует DELIVERY-стадии — пайплайн не
///    мог уйти дальше без нового `stage_execution_id`, но состояние в Redis
///    могло быть удалено (`finalize`) по совсем другой причине.
/// 2. `awaiting_stage_execution_id` пуст — уже НЕ retry-pending, значит эта
///    задача сработала ПОВТОРНО (Kafka at-least-once) после того, как более
///    ранняя копия УЖЕ произвела редиспатч. Не редиспатчим снова.
/// 3. `state.attempt == task.attempt` — задача от СТАРОЙ, уже суперсиженной
///    попытки (например задача A от attempt=2 и задача B от attempt=3 обе
///    в полёте одновременно из-за редоставки) не должна триггерить лишний
///    редиспатч поверх уже более новой попытки.
async fn process_one_retry_trigger_record(
    payload: &[u8],
    producer: &FutureProducer,
    pipeline: &PipelineDefinition,
    store: &RedisStateStore,
) -> bool {
    let Ok(task) = crate::proto::events::SchedulerBackgroundTask::decode(payload) else {
        tracing::error!("не удалось декодировать SchedulerBackgroundTask из pipeline.retry.triggers");
        return false;
    };
    if task.task_type != crate::proto::common::BackgroundTaskType::StageRetry as i32 {
        // Топик выделен исключительно под STAGE_RETRY (allowlist в
        // scheduler_events.proto) — чужой task_type здесь означает
        // рассинхронизацию конфигурации Background Lane, не наши данные.
        tracing::error!("неожиданный task_type={} в pipeline.retry.triggers для message_id={}", task.task_type, task.message_id);
        return false;
    }

    let mut state = match store.load(&task.message_id).await {
        Ok(Some(s)) => s,
        Ok(None) => {
            tracing::warn!("нет ExecutionState для retry-triggered message_id={} — уже финализировано (например TTL истёк раньше)", task.message_id);
            return true; // не переобрабатываем вечно — состояния больше нет ни при каком исходе
        }
        Err(e) => {
            tracing::error!("не удалось прочитать ExecutionState для retry message_id={}: {e}", task.message_id);
            return false;
        }
    };

    let Some(node) = pipeline.node(&state.current_node_id) else {
        tracing::error!("retry-triggered message_id={} ссылается на несуществующий узел {}", task.message_id, state.current_node_id);
        return true;
    };
    if node.stage_name != "DELIVERY" || state.awaiting_stage_execution_id.is_some() || state.attempt != task.attempt {
        tracing::info!(
            "retry-триггер для message_id={} проигнорирован — состояние уже продвинулось (node={} awaiting={:?} attempt={} vs task.attempt={})",
            task.message_id, node.stage_name, state.awaiting_stage_execution_id, state.attempt, task.attempt
        );
        return true;
    }

    let stage_execution_id = Uuid::new_v4().to_string(); // НОВЫЙ id — см. Decision::RetryLater про SubmitIdempotencyStore
    state.awaiting_stage_execution_id = Some(stage_execution_id.clone());
    let decision = NextStageDecision { node_id: node.node_id.clone(), stage_name: node.stage_name.clone() };
    let command = match build_stage_execute(&decision, &state, &state.destination_address.clone(), stage_execution_id) {
        Ok(c) => c,
        Err(e) => {
            tracing::error!("не удалось построить StageExecuteCommand для retry message_id={}: {e}", task.message_id);
            return false;
        }
    };
    state.deadline_ms = now_ms() + DEFAULT_STAGE_TIMEOUT_MS;

    // expected=None: только что проверили awaiting_stage_execution_id.is_some()==false
    // выше — CAS должен увидеть ровно то же самое, иначе это гонка с ещё
    // одной копией этого же триггера (Kafka at-least-once) — Conflict ниже
    // ловит именно этот случай.
    match store.cas_advance(None, None, &state).await {
        Ok(CasOutcome::Ok) => {}
        Ok(CasOutcome::Conflict { actual_awaiting }) => {
            tracing::warn!(
                "CAS-конфликт при retry-редиспатче message_id={}: реально уже awaiting={actual_awaiting} — другая копия триггера успела раньше",
                task.message_id
            );
            return true;
        }
        Err(e) => {
            tracing::error!("не удалось атомарно записать retry-редиспатч для message_id={}: {e}", task.message_id);
            return false;
        }
    }

    let Some(topic) = stage_topic(&node.stage_name) else {
        tracing::error!("DELIVERY без известного топика — быть не может, но defensive check");
        return false;
    };
    let bytes = command.encode_to_vec();
    let record = FutureRecord::to(topic).key(&command.message_id).payload(&bytes);
    if let Err((e, _)) = producer.send(record, Duration::from_secs(5)).await {
        tracing::error!("не удалось опубликовать retry StageExecuteCommand для message_id={}: {e}", task.message_id);
        return false;
    }
    true
}

pub async fn run_retry_trigger_loop(
    consumer: StreamConsumer,
    producer: FutureProducer,
    pipeline: Arc<ArcSwap<PipelineDefinition>>,
    store: Arc<RedisStateStore>,
) {
    // build_consumer уже подписал на топик (см. main.rs) — та же
    // конвенция, что у run_incoming_loop/run_completed_loop, повторный
    // subscribe() здесь не нужен.
    let consumer = Arc::new(consumer);
    let semaphore = Arc::new(Semaphore::new(concurrency_limit("PIPELINE_RETRY_TRIGGER_CONCURRENCY", 64)));
    let tracker = Arc::new(OffsetTracker::new());
    let process_timeout = timeout_secs("PIPELINE_RETRY_TRIGGER_PROCESS_TIMEOUT_SECS", 30);

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
                let pipeline_snapshot = pipeline.load_full();
                let store_task = store.clone();
                let tracker_task = tracker.clone();
                let key_task = key.clone();

                tokio::spawn(async move {
                    let _permit = permit;
                    let done = match tokio::time::timeout(
                        process_timeout,
                        process_one_retry_trigger_record(&payload, &producer_task, &pipeline_snapshot, &store_task),
                    )
                    .await
                    {
                        Ok(done) => done,
                        Err(_) => {
                            tracing::error!(
                                "process_one_retry_trigger_record завис дольше {:?} (partition={} offset={offset})",
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
            Err(e) => tracing::error!("ошибка pipeline.retry.triggers consumer: {e}"),
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
            priority_flag: 2,
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
    fn handle_incoming_sets_priority_and_ttl_directly_from_incoming_message_not_derived() {
        // В отличие от `category` (приходит из PolicyResult в середине
        // пайплайна) — priority_flag и message_ttl_ms устанавливаются РОВНО
        // один раз, здесь, из IncomingMessage напрямую.
        let pipeline = pipeline();
        let mut incoming = incoming_sms("m1", "998901331835", 1);
        incoming.priority_flag = 3;
        incoming.message_ttl = Some(prost_types::Timestamp { seconds: 1_700_000_000, nanos: 0 });
        let (state, _, _) = handle_incoming(&incoming, &pipeline).unwrap();
        assert_eq!(state.priority_flag, 3);
        assert_eq!(state.message_ttl_ms, 1_700_000_000_000);
    }

    #[test]
    fn handle_incoming_missing_ttl_never_expires() {
        let pipeline = pipeline();
        let mut incoming = incoming_sms("m1", "998901331835", 1);
        incoming.message_ttl = None;
        let (state, _, _) = handle_incoming(&incoming, &pipeline).unwrap();
        assert_eq!(state.message_ttl_ms, i64::MAX);
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

        match advance(&mut state, &pipeline, &event, &destination_address, 0).unwrap() {
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
        let mut state = ExecutionState::new_from_incoming("m1".into(), &pipeline, 1, 2, i64::MAX);
        state.current_node_id = "n_billing_blocked".into();
        state.category = Some("BLOCKED".into());
        state.awaiting_stage_execution_id = Some("se1".into());

        let event = StageCompletedEvent {
            event_id: "e1".into(), message_id: "m1".into(), stage_execution_id: "se1".into(), attempt: 1,
            stage_name: StageName::Billing as i32, outcome: Outcome::Succeeded as i32, reason_code: String::new(), retryable: false,
            retry_after: None, traceparent: "tp1".into(), completed_at: None, stage_result: None,
        };
        assert_eq!(advance(&mut state, &pipeline, &event, "998901331835", 0).unwrap(), AdvanceOutcome::Terminal);
    }

    #[test]
    fn advance_with_stale_event_returns_ignored_not_terminal() {
        // Найдено кодревью: устаревшее событие не должно ни продвигать, ни
        // завершать пайплайн — Ignored отличим от Terminal именно поэтому.
        let pipeline = pipeline();
        let mut state = ExecutionState::new_from_incoming("m1".into(), &pipeline, 1, 2, i64::MAX);
        state.awaiting_stage_execution_id = Some("se-real".into());

        let stale_event = StageCompletedEvent {
            event_id: "e-stale".into(), message_id: "m1".into(), stage_execution_id: "se-old".into(), attempt: 1,
            stage_name: StageName::DestinationResolution as i32, outcome: Outcome::Succeeded as i32, reason_code: String::new(), retryable: false,
            retry_after: None, traceparent: "tp1".into(), completed_at: None, stage_result: None,
        };
        assert_eq!(advance(&mut state, &pipeline, &stale_event, "998901331835", 0).unwrap(), AdvanceOutcome::Ignored);
        assert_eq!(state.current_node_id, "n1_destination_resolution", "состояние не должно было измениться");
    }
}
