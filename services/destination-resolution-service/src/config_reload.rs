//! `config.changes` consumer — реальный hot-reload вместо статичного файла,
//! запечённого в образ (Cargo.toml объявлял `arc-swap`, но до этого файла ни
//! один Rust-сервис платформы никогда его реально не подключал к
//! Kafka-консьюмеру — см. README "Что НЕ реализовано").
//!
//! Отдельная consumer group от `stage.destination-resolution` (kafka_io.rs)
//! — независимое партиционирование/офсеты, падение одного цикла не должно
//! останавливать другой. `config.changes` — compacted-топик с несколькими
//! entity_type в одном топике (см. platform-contracts/events/config_and_control.proto);
//! этот сервис фильтрует только NUMBER_RANGE, остальные entity_type просто
//! коммитятся и пропускаются.

use crate::proto::ConfigEntityType;
use crate::proto::events_v1::ConfigChangeEvent;
use crate::resolver::{ConfigOverlay, NumberRangeConfigPayload, Snapshot};
use arc_swap::ArcSwap;
use prost::Message;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message as _;
use std::sync::Arc;

pub const TOPIC: &str = "config.changes";

/// Отдельная от `kafka_io::build_consumer` фабрика: `auto.offset.reset=earliest`
/// обязателен именно здесь — compacted-топик, и на каждом свежем старте (новая
/// consumer group, ещё не тот же ID, что был раньше) нужно увидеть ВСЮ
/// текущую историю активных entity_id, не только события с этого момента.
/// `kafka_io::build_consumer` этого не делает (полагается на default,
/// который для `stage.*` топиков — верно: живой трафик, не восстановление
/// состояния).
/// Реальная находка (проверено на policy-service, тот же паттерн кода —
/// см. policy-service/src/config_reload.rs для полного разбора): раньше
/// `group_id` был ФИКСИРОВАННЫМ и переживал рестарт процесса, из-за чего
/// `auto.offset.reset=earliest` реально срабатывал только на самом первом
/// запуске — на каждом следующем рестарте свежий (пустой in-memory)
/// `ConfigOverlay` резюмировал с уже закоммиченной прошлым процессом
/// позиции и никогда не переигрывал историю `config.changes` заново,
/// молча теряя hot-reload состояние без единой строки в логах. Уникальный
/// `group_id` на каждый процесс гарантирует полный переигрыш compacted-
/// топика на каждом старте.
pub fn build_config_consumer(bootstrap_servers: &str, group_id: &str) -> StreamConsumer {
    let unique_group_id = format!(
        "{group_id}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or(0)
    );
    ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .set("group.id", &unique_group_id)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create()
        .expect("не удалось создать Kafka consumer (config.changes)")
}

/// Ядро обработки одного `ConfigChangeEvent` — вынесено отдельно от
/// Kafka-цикла ради тестируемости без реального брокера (тот же принцип,
/// что `kafka_io::handle_command`). Возвращает `true`, если событие
/// относилось к NUMBER_RANGE и снапшот нужно пересобрать/опубликовать.
pub fn handle_config_change(overlay: &ConfigOverlay, event: &ConfigChangeEvent) -> bool {
    if event.entity_type != ConfigEntityType::NumberRange as i32 {
        return false;
    }
    let payload: NumberRangeConfigPayload = match serde_json::from_slice(&event.payload_json) {
        Ok(p) => p,
        Err(e) => {
            tracing::error!(
                "config.changes: entity_id={} payload_json не парсится как number_range: {e}",
                event.entity_id
            );
            return false;
        }
    };
    overlay.apply(&event.entity_id, &payload);
    true
}

pub async fn run_loop(consumer: StreamConsumer, overlay: Arc<ConfigOverlay>, live: Arc<ArcSwap<Snapshot>>) {
    consumer.subscribe(&[TOPIC]).expect("не удалось подписаться на config.changes");

    loop {
        match consumer.recv().await {
            Ok(msg) => {
                let Some(payload) = msg.payload() else {
                    let _ = consumer.commit_message(&msg, rdkafka::consumer::CommitMode::Async);
                    continue;
                };
                let event = match ConfigChangeEvent::decode(payload) {
                    Ok(e) => e,
                    Err(e) => {
                        tracing::error!("не удалось декодировать ConfigChangeEvent: {e}");
                        let _ = consumer.commit_message(&msg, rdkafka::consumer::CommitMode::Async);
                        continue;
                    }
                };

                if handle_config_change(&overlay, &event) {
                    live.store(Arc::new(overlay.build_snapshot()));
                    tracing::info!(
                        "config.changes: number_range обновлён (entity_id={}, status={}) — снапшот пересобран",
                        event.entity_id,
                        event.status
                    );
                }

                if let Err(e) = consumer.commit_message(&msg, rdkafka::consumer::CommitMode::Async) {
                    tracing::error!("не удалось закоммитить offset (config.changes): {e}");
                }
            }
            Err(e) => tracing::error!("ошибка Kafka consumer (config.changes): {e}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resolver::ResolveResult;

    fn range_event(entity_id: &str, range_start: u64, range_end: u64, operator_id: &str, status: &str) -> ConfigChangeEvent {
        let payload = serde_json::json!({
            "range_start": range_start,
            "range_end": range_end,
            "operator_id": operator_id,
            "version": 1,
            "status": status,
        });
        ConfigChangeEvent {
            entity_type: ConfigEntityType::NumberRange as i32,
            entity_id: entity_id.to_string(),
            version: 1,
            payload_json: serde_json::to_vec(&payload).unwrap(),
            status: status.to_string(),
            created_at: None,
        }
    }

    fn empty_overlay() -> ConfigOverlay {
        ConfigOverlay::new(Snapshot { number_ranges: vec![], portability_overrides: Default::default() })
    }

    #[test]
    fn ignores_events_for_other_entity_types() {
        let overlay = empty_overlay();
        let mut event = range_event("cfg-1", 998770000000, 998779999999, "some_uz", "active");
        event.entity_type = ConfigEntityType::PolicyRuleset as i32;
        assert!(!handle_config_change(&overlay, &event));
        assert_eq!(
            overlay.build_snapshot().resolve_operator_by_range("998770000000"),
            ResolveResult::NotFound
        );
    }

    #[test]
    fn malformed_payload_json_is_rejected_not_panicking() {
        let overlay = empty_overlay();
        let mut event = range_event("cfg-1", 998770000000, 998779999999, "some_uz", "active");
        event.payload_json = b"{not valid json".to_vec();
        assert!(!handle_config_change(&overlay, &event));
    }

    #[test]
    fn active_number_range_event_is_applied_and_resolvable() {
        let overlay = empty_overlay();
        let event = range_event("cfg-1", 998770000000, 998779999999, "fresh_uz", "active");
        assert!(handle_config_change(&overlay, &event));
        assert_eq!(
            overlay.build_snapshot().resolve_operator_by_range("998770000000"),
            ResolveResult::Resolved("fresh_uz".to_string())
        );
    }
}
