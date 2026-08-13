//! `config.changes` consumer — реальный hot-reload вместо статичного файла,
//! запечённого в образ (см. destination-resolution-service/src/config_reload.rs
//! для полного обоснования паттерна). Отдельная consumer group от
//! `incoming.messages`/`stage.completed` — независимые офсеты/партиционирование.

use crate::pipeline_graph::{ConfigOverlay, PipelineConfigPayload, PipelineDefinition};
use crate::proto::common::ConfigEntityType;
use crate::proto::events::ConfigChangeEvent;
use arc_swap::ArcSwap;
use prost::Message;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message as _;
use std::sync::Arc;

pub const TOPIC: &str = "config.changes";

/// `auto.offset.reset=earliest` — compacted-топик, на каждом свежем старте
/// нужна вся история (в частности последнее active-состояние), не только
/// новые события "с этого момента".
/// Реальная находка (проверено на policy-service, тот же паттерн кода —
/// см. policy-service/src/config_reload.rs для полного разбора): раньше
/// `group_id` был ФИКСИРОВАННЫМ и переживал рестарт процесса, из-за чего
/// `auto.offset.reset=earliest` реально срабатывал только на самом первом
/// запуске — на каждом следующем рестарте свежий (пустой in-memory)
/// overlay резюмировал с уже закоммиченной прошлым процессом позиции и
/// никогда не переигрывал историю `config.changes` заново, молча теряя
/// hot-reload состояние без единой строки в логах. Уникальный `group_id`
/// на каждый процесс гарантирует полный переигрыш compacted-топика на
/// каждом старте.
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
/// Возвращает `true`, если событие относилось к PIPELINE и снапшот нужно
/// пересобрать/опубликовать.
pub fn handle_config_change(overlay: &ConfigOverlay, event: &ConfigChangeEvent) -> bool {
    if event.entity_type != ConfigEntityType::Pipeline as i32 {
        return false;
    }
    let payload: PipelineConfigPayload = match serde_json::from_slice(&event.payload_json) {
        Ok(p) => p,
        Err(e) => {
            tracing::error!(
                "config.changes: entity_id={} payload_json не парсится как pipeline: {e}",
                event.entity_id
            );
            return false;
        }
    };
    overlay.apply(payload);
    true
}

pub async fn run_loop(consumer: StreamConsumer, overlay: Arc<ConfigOverlay>, live: Arc<ArcSwap<PipelineDefinition>>) {
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
                    live.store(Arc::new(overlay.current()));
                    tracing::info!(
                        "config.changes: pipeline обновлён (entity_id={}, status={}) — снапшот пересобран",
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

    fn base_pipeline() -> PipelineDefinition {
        serde_json::from_str(include_str!("../../../config_schemas/examples/pipeline.valid.json")).unwrap()
    }

    fn pipeline_event(entity_id: &str, pipeline_id: &str, status: &str) -> ConfigChangeEvent {
        let payload = serde_json::json!({
            "pipeline_id": pipeline_id,
            "version": 2,
            "status": status,
            "entry_node_id": "n1",
            "nodes": [{"node_id": "n1", "stage_name": "DESTINATION_RESOLUTION", "next": {}}],
        });
        ConfigChangeEvent {
            entity_type: ConfigEntityType::Pipeline as i32,
            entity_id: entity_id.to_string(),
            version: 2,
            payload_json: serde_json::to_vec(&payload).unwrap(),
            status: status.to_string(),
            created_at: None,
        }
    }

    #[test]
    fn ignores_events_for_other_entity_types() {
        let overlay = ConfigOverlay::new(base_pipeline());
        let mut event = pipeline_event("default", "hot-reloaded", "active");
        event.entity_type = ConfigEntityType::PolicyRuleset as i32;
        assert!(!handle_config_change(&overlay, &event));
        assert_eq!(overlay.current().pipeline_id, base_pipeline().pipeline_id);
    }

    #[test]
    fn malformed_payload_json_is_rejected_not_panicking() {
        let overlay = ConfigOverlay::new(base_pipeline());
        let mut event = pipeline_event("default", "hot-reloaded", "active");
        event.payload_json = b"{not valid json".to_vec();
        assert!(!handle_config_change(&overlay, &event));
    }

    #[test]
    fn active_pipeline_event_is_applied() {
        let overlay = ConfigOverlay::new(base_pipeline());
        let event = pipeline_event("default", "hot-reloaded", "active");
        assert!(handle_config_change(&overlay, &event));
        assert_eq!(overlay.current().pipeline_id, "hot-reloaded");
        assert_eq!(overlay.current().node("n1").map(|n| n.stage_name.as_str()), Some("DESTINATION_RESOLUTION"));
    }
}
