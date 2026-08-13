//! Загрузка графа пайплайна — форма ровно та, что уже провалидирована в
//! `config_schemas/pipeline.schema.json` (включая инварианты, проверенные
//! там `validate_pipeline_graph()`: entry_node_id всегда DESTINATION_RESOLUTION,
//! POLICY.REJECTED всегда ведёт в BILLING). Здесь граф только загружается и
//! обходится в рантайме — те два хардкод-правила уже закодированы в САМИХ
//! ДАННЫХ реального `pipeline.valid.json`, resolve_next_stage не содержит
//! про них никакой специальной логики, только у Billing-BLOCKED override
//! (см. execution_state.rs) есть отдельная runtime-проверка, потому что
//! service_internal_methods.md §1.4 явно требует её "независимо от того,
//! что граф конфигурации мог бы предписать".

use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Debug, Clone, Deserialize)]
pub struct Node {
    pub node_id: String,
    pub stage_name: String, // "DESTINATION_RESOLUTION" | "POLICY" | "BILLING" | "ROUTING" | "DELIVERY" | "DELIVERY_RECONCILIATION"
    #[serde(default)]
    pub next: HashMap<String, Option<String>>, // Outcome (без префикса) -> node_id | null
}

#[derive(Debug, Clone, Deserialize)]
pub struct PipelineDefinition {
    pub pipeline_id: String,
    pub version: u32,
    pub entry_node_id: String,
    nodes: Vec<Node>,
}

impl PipelineDefinition {
    /// Только для тестов — конструирование патологических графов, доказывающих,
    /// что рантайм-инварианты (Billing-BLOCKED override) не зависят от данных графа.
    #[cfg(test)]
    pub fn nodes_mut(&mut self) -> impl Iterator<Item = &mut Node> {
        self.nodes.iter_mut()
    }

    pub fn node(&self, node_id: &str) -> Option<&Node> {
        self.nodes.iter().find(|n| n.node_id == node_id)
    }

    pub fn entry_node(&self) -> &Node {
        self.node(&self.entry_node_id).expect("entry_node_id обязан ссылаться на существующий узел (config_schemas/pipeline.schema.json уже это проверяет)")
    }
}

/// `config.changes` (entity_type=PIPELINE) — `config_schemas/pipeline.schema.json`
/// целиком. `partner_id`/`application_id` сознательно проигнорированы
/// (`#[serde(default)]`, не читаются) — этот сервис резолвит один
/// платформенный дефолт-пайплайн, не per-partner (та же упрощённая
/// семантика, что уже была у статического файла, hot-reload её не
/// расширяет, только подключает к реальному Kafka).
#[derive(Debug, Clone, Deserialize)]
pub struct PipelineConfigPayload {
    pub pipeline_id: String,
    pub version: u32,
    pub status: String, // "active" | "archived"
    pub entry_node_id: String,
    pub nodes: Vec<Node>,
}

/// Живое состояние графа поверх статического bootstrap-файла — единый
/// глобальный слот (last-active-wins), тот же принцип, что
/// policy-service::config_reload::ConfigOverlay для POLICY_RULESET. arc-swap
/// был объявлен в services_specifictaion.md §2.3, но до этого файла не был
/// даже зависимостью Cargo.toml, не то что подключён к реальному консьюмеру
/// (см. README "Что НЕ реализовано").
pub struct ConfigOverlay {
    base: PipelineDefinition,
    overlay: Mutex<Option<PipelineDefinition>>,
}

impl ConfigOverlay {
    pub fn new(base: PipelineDefinition) -> Self {
        ConfigOverlay { base, overlay: Mutex::new(None) }
    }

    pub fn apply(&self, payload: PipelineConfigPayload) {
        let mut overlay = self.overlay.lock().expect("overlay mutex poisoned");
        *overlay = if payload.status == "archived" {
            None
        } else {
            Some(PipelineDefinition {
                pipeline_id: payload.pipeline_id,
                version: payload.version,
                entry_node_id: payload.entry_node_id,
                nodes: payload.nodes,
            })
        };
    }

    pub fn current(&self) -> PipelineDefinition {
        self.overlay.lock().expect("overlay mutex poisoned").clone().unwrap_or_else(|| self.base.clone())
    }
}

#[cfg(test)]
mod overlay_tests {
    use super::*;

    fn base() -> PipelineDefinition {
        serde_json::from_str(include_str!("../../../config_schemas/examples/pipeline.valid.json")).unwrap()
    }

    fn payload(status: &str, entry_node_id: &str) -> PipelineConfigPayload {
        PipelineConfigPayload {
            pipeline_id: "hot-reloaded".to_string(),
            version: 2,
            status: status.to_string(),
            entry_node_id: entry_node_id.to_string(),
            nodes: vec![Node { node_id: entry_node_id.to_string(), stage_name: "DESTINATION_RESOLUTION".to_string(), next: HashMap::new() }],
        }
    }

    #[test]
    fn starts_out_matching_the_static_baseline() {
        let overlay = ConfigOverlay::new(base());
        assert_eq!(overlay.current().pipeline_id, base().pipeline_id);
    }

    #[test]
    fn active_event_replaces_the_pipeline() {
        let overlay = ConfigOverlay::new(base());
        overlay.apply(payload("active", "n1"));
        let current = overlay.current();
        assert_eq!(current.pipeline_id, "hot-reloaded");
        assert_eq!(current.entry_node_id, "n1");
    }

    #[test]
    fn archived_event_reverts_to_static_baseline() {
        let overlay = ConfigOverlay::new(base());
        overlay.apply(payload("active", "n1"));
        assert_eq!(overlay.current().pipeline_id, "hot-reloaded");

        overlay.apply(payload("archived", "n1"));
        assert_eq!(overlay.current().pipeline_id, base().pipeline_id);
    }
}
