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
use crate::pipeline_graph::PipelineDefinition;
use crate::proto::common::StageCompletedEvent;
use crate::proto::events::IncomingMessage;
use crate::proto::events::incoming_message::Body as IncomingBody;
use crate::redis_cas::{CasOutcome, RedisStateStore};
use prost::Message;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message as _;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

pub const INCOMING_TOPIC: &str = "incoming.messages";
pub const COMPLETED_TOPIC: &str = "stage.completed";

/// Дефолт для `deadline_ms` — не задокументирован дословно нигде (сама
/// wire-таблица `StageExecuteCommand.deadline` тоже ещё не заполняется, это
/// отдельный, отдельно задокументированный пробел, см. README) — разумное
/// значение для внутреннего Critical Sweep трекинга: с запасом дольше
/// типичного round-trip любой стадии, короче, чем стоит ждать перед тем,
/// как считать стадию зависшей.
const DEFAULT_STAGE_TIMEOUT_MS: i64 = 30_000;

fn now_ms() -> i64 {
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
pub async fn run_incoming_loop(
    consumer: StreamConsumer,
    producer: FutureProducer,
    pipeline: Arc<PipelineDefinition>,
    store: Arc<RedisStateStore>,
) {
    loop {
        match consumer.recv().await {
            Ok(msg) => {
                let Some(payload) = msg.payload() else { continue };
                let Ok(incoming) = IncomingMessage::decode(payload) else {
                    tracing::error!("не удалось декодировать IncomingMessage");
                    continue;
                };

                let (mut state, destination_address, command) = match handle_incoming(&incoming, &pipeline) {
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
                state.destination_address = destination_address;
                state.deadline_ms = now_ms() + DEFAULT_STAGE_TIMEOUT_MS;
                let stage_execution_id = state.awaiting_stage_execution_id.clone().unwrap_or_default();

                match store.cas_advance(None, None, &state).await {
                    Ok(CasOutcome::Ok) => {}
                    Ok(CasOutcome::Conflict { .. }) => {
                        tracing::warn!("incoming.messages повтор для уже активного message_id={}, пропущено", incoming.message_id);
                        if let Err(e) = consumer.commit_message(&msg, rdkafka::consumer::CommitMode::Async) {
                            tracing::error!("не удалось закоммитить offset (incoming.messages, дубликат): {e}");
                        }
                        continue;
                    }
                    Err(e) => {
                        tracing::error!("не удалось записать начальное состояние в Runtime Redis для message_id={}: {e}", incoming.message_id);
                        continue; // не коммитим — at-least-once, переобработается
                    }
                }

                let bytes = command.encode_to_vec();
                let record = FutureRecord::to(topic).key(&command.message_id).payload(&bytes);
                if let Err((e, _)) = producer.send(record, Duration::from_secs(5)).await {
                    tracing::error!("не удалось опубликовать первую StageExecuteCommand: {e}");
                    // Реальная находка при этом рефакторинге (не новая, была
                    // и в in-memory версии, там просто не откатывалась):
                    // CAS уже создал состояние с expected=None, но команда не
                    // опубликована — без отката повторная обработка того же
                    // incoming.messages увидела бы это состояние как "уже
                    // активное" и никогда не опубликовала бы первую команду.
                    // Здесь, раз CAS только что создал ИМЕННО этот ключ с нуля
                    // (никто другой не мог успеть на него опереться), откат
                    // безопасен и однозначен.
                    if let Err(finalize_err) = store.finalize(&state.message_id, &stage_execution_id).await {
                        tracing::error!("не удалось откатить состояние message_id={} после неудачной публикации: {finalize_err}", state.message_id);
                    }
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
pub async fn run_completed_loop(
    consumer: StreamConsumer,
    producer: FutureProducer,
    pipeline: Arc<PipelineDefinition>,
    store: Arc<RedisStateStore>,
) {
    loop {
        match consumer.recv().await {
            Ok(msg) => {
                let Some(payload) = msg.payload() else { continue };
                let Ok(event) = StageCompletedEvent::decode(payload) else {
                    tracing::error!("не удалось декодировать StageCompletedEvent");
                    continue;
                };

                let mut state = match store.load(&event.message_id).await {
                    Ok(Some(s)) => s,
                    Ok(None) => {
                        tracing::error!("нет ExecutionState в Runtime Redis для message_id={} — либо ещё не создано, либо уже финализировано", event.message_id);
                        continue;
                    }
                    Err(e) => {
                        tracing::error!("не удалось прочитать ExecutionState для message_id={}: {e}", event.message_id);
                        continue; // не коммитим — переобработается
                    }
                };
                let expected_before = state.awaiting_stage_execution_id.clone();
                let destination_address = state.destination_address.clone();

                match advance(&mut state, &pipeline, &event, &destination_address) {
                    Ok(AdvanceOutcome::Next(topic, command)) => {
                        state.deadline_ms = now_ms() + DEFAULT_STAGE_TIMEOUT_MS;
                        match store.cas_advance(expected_before.as_deref(), expected_before.as_deref(), &state).await {
                            Ok(CasOutcome::Ok) => {}
                            Ok(CasOutcome::Conflict { actual_awaiting }) => {
                                tracing::warn!(
                                    "CAS-конфликт при продвижении message_id={}: ожидали awaiting={:?}, реально={actual_awaiting} — другая реплика уже продвинула это состояние, событие проигнорировано",
                                    event.message_id, expected_before
                                );
                                if let Err(e) = consumer.commit_message(&msg, rdkafka::consumer::CommitMode::Async) {
                                    tracing::error!("не удалось закоммитить offset (stage.completed, CAS-конфликт): {e}");
                                }
                                continue;
                            }
                            Err(e) => {
                                tracing::error!("не удалось атомарно записать новое состояние для message_id={}: {e}", event.message_id);
                                continue; // не коммитим — переобработается
                            }
                        }
                        let bytes = command.encode_to_vec();
                        let record = FutureRecord::to(&topic).key(&command.message_id).payload(&bytes);
                        if let Err((e, _)) = producer.send(record, Duration::from_secs(5)).await {
                            tracing::error!("не удалось опубликовать StageExecuteCommand: {e}");
                            continue;
                        }
                    }
                    Ok(AdvanceOutcome::Terminal) => {
                        let expected = expected_before.as_deref().unwrap_or("");
                        if let Err(e) = store.finalize(&event.message_id, expected).await {
                            tracing::error!("не удалось финализировать message_id={}: {e}", event.message_id);
                            continue; // не коммитим — переобработается
                        }
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
