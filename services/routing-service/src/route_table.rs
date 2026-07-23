//! `resolve_route_table` (service_internal_methods.md §1.7) — форма ровно та,
//! что уже провалидирована в `config_schemas/routing_table.schema.json`.
//! Снапшот здесь — `HashMap<operator_id, RouteTable>`, загружаемый из локальных
//! JSON-файлов; в проде — тот же снапшот, построенный из `config.changes`
//! (`entity_type=routing_table`), как и у остальных сервисов этого среза.

use serde::Deserialize;
use std::collections::HashMap;

#[derive(Debug, Clone, Deserialize)]
pub struct Route {
    pub route_id: String,
    pub protocol: String, // "SMPP" | "HTTP"
    pub failover_priority: u32,
    #[allow(dead_code)]
    pub tps_limit: u32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RouteTable {
    pub operator_id: String,
    pub version: u32,
    pub active_route_id: String,
    pub routes: Vec<Route>,
}

pub struct RouteTableSnapshot {
    by_operator: HashMap<String, RouteTable>,
}

impl RouteTableSnapshot {
    pub fn from_tables(tables: Vec<RouteTable>) -> Self {
        Self { by_operator: tables.into_iter().map(|t| (t.operator_id.clone(), t)).collect() }
    }

    /// `select_routes_for_operator` — здесь тривиален: снапшот уже
    /// организован по operator_id (та же структура, что config_schemas/routing_table.schema.json),
    /// не требует отдельного шага фильтрации широкого списка маршрутов.
    pub fn for_operator(&self, operator_id: &str) -> Option<&RouteTable> {
        self.by_operator.get(operator_id)
    }
}
