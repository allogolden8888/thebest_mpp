//! `config.changes` consumer — реальный hot-reload вместо статичного файла,
//! запечённого в образ (см. destination-resolution-service/src/config_reload.rs
//! для полного обоснования паттерна). Отдельная consumer group от
//! `stage.routing` — независимые офсеты/партиционирование.

use crate::proto::ConfigEntityType;
use crate::proto::events_v1::ConfigChangeEvent;
use crate::route_table::{ConfigOverlay, RouteTableConfigPayload, RouteTableSnapshot};
use arc_swap::ArcSwap;
use prost::Message;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message as _;
use std::sync::Arc;

pub const TOPIC: &str = "config.changes";

/// `auto.offset.reset=earliest` — compacted-топик, на каждом свежем старте
/// нужна вся история активных entity_id, не только новые события "с этого
/// момента" (см. destination-resolution-service/config_reload.rs).
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

/// Ядро обработки одного `ConfigChangeEvent` — тестируется без брокера.
/// Возвращает `true`, если событие относилось к ROUTING_TABLE и снапшот
/// нужно пересобрать/опубликовать.
pub fn handle_config_change(overlay: &ConfigOverlay, event: &ConfigChangeEvent) -> bool {
    if event.entity_type != ConfigEntityType::RoutingTable as i32 {
        return false;
    }
    let payload: RouteTableConfigPayload = match serde_json::from_slice(&event.payload_json) {
        Ok(p) => p,
        Err(e) => {
            tracing::error!(
                "config.changes: entity_id={} payload_json не парсится как routing_table: {e}",
                event.entity_id
            );
            return false;
        }
    };
    overlay.apply(&event.entity_id, &payload);
    true
}

pub async fn run_loop(consumer: StreamConsumer, overlay: Arc<ConfigOverlay>, live: Arc<ArcSwap<RouteTableSnapshot>>) {
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
                        "config.changes: routing_table обновлён (entity_id={}, status={}) — снапшот пересобран",
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

    fn route_event(entity_id: &str, operator_id: &str, active_route_id: &str, status: &str) -> ConfigChangeEvent {
        let payload = serde_json::json!({
            "operator_id": operator_id,
            "version": 2,
            "status": status,
            "active_route_id": active_route_id,
            "routes": [{"route_id": active_route_id, "protocol": "SMPP", "failover_priority": 1, "tps_limit": 100}],
        });
        ConfigChangeEvent {
            entity_type: ConfigEntityType::RoutingTable as i32,
            entity_id: entity_id.to_string(),
            version: 2,
            payload_json: serde_json::to_vec(&payload).unwrap(),
            status: status.to_string(),
            created_at: None,
        }
    }

    fn empty_overlay() -> ConfigOverlay {
        ConfigOverlay::new(RouteTableSnapshot::from_tables(vec![]))
    }

    #[test]
    fn ignores_events_for_other_entity_types() {
        let overlay = empty_overlay();
        let mut event = route_event("beeline_uz", "beeline_uz", "primary", "active");
        event.entity_type = ConfigEntityType::NumberRange as i32;
        assert!(!handle_config_change(&overlay, &event));
        assert!(overlay.build_snapshot().for_operator("beeline_uz").is_none());
    }

    #[test]
    fn malformed_payload_json_is_rejected_not_panicking() {
        let overlay = empty_overlay();
        let mut event = route_event("beeline_uz", "beeline_uz", "primary", "active");
        event.payload_json = b"{not valid json".to_vec();
        assert!(!handle_config_change(&overlay, &event));
    }

    #[test]
    fn active_routing_table_event_is_applied_and_resolvable() {
        let overlay = empty_overlay();
        let event = route_event("beeline_uz", "beeline_uz", "fresh_primary", "active");
        assert!(handle_config_change(&overlay, &event));
        assert_eq!(
            overlay.build_snapshot().for_operator("beeline_uz").unwrap().active_route_id,
            "fresh_primary"
        );
    }
}
