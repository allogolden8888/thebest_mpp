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
