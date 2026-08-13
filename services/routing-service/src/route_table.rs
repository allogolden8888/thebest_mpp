//! `resolve_route_table` (service_internal_methods.md §1.7) — форма ровно та,
//! что уже провалидирована в `config_schemas/routing_table.schema.json`.
//! Снапшот здесь — `HashMap<operator_id, RouteTable>`, загружаемый из локальных
//! JSON-файлов; в проде — тот же снапшот, построенный из `config.changes`
//! (`entity_type=routing_table`), как и у остальных сервисов этого среза.

use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Mutex;

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

/// Одна запись `config.changes` (entity_type=ROUTING_TABLE) —
/// `config_schemas/routing_table.schema.json` целиком (не одна строка, в
/// отличие от NUMBER_RANGE — весь route table одного оператора это одна
/// config-сущность, `status` живёт на уровне таблицы, не отдельного route).
#[derive(Debug, Clone, Deserialize)]
pub struct RouteTableConfigPayload {
    pub operator_id: String,
    pub version: u32,
    pub status: String, // "active" | "archived"
    pub active_route_id: String,
    pub routes: Vec<Route>,
}

/// Живое состояние `routing_table` поверх статического bootstrap-снапшота —
/// arc-swap был объявлен в Cargo.toml, но никогда не подключался к реальному
/// Kafka-консьюмеру (тот же класс находки, что в destination-resolution-service/
/// resolver.rs::ConfigOverlay). `entity_id` config.changes-события здесь
/// естественно совпадает с operator_id (один route table на оператора) —
/// проще number_range: обновление просто заменяет значение по ключу, не
/// нужен приоритет "overlay поверх base при пересечении диапазонов".
pub struct ConfigOverlay {
    base: HashMap<String, RouteTable>,
    overlay: Mutex<HashMap<String, RouteTable>>,
}

impl ConfigOverlay {
    pub fn new(base: RouteTableSnapshot) -> Self {
        ConfigOverlay { base: base.by_operator, overlay: Mutex::new(HashMap::new()) }
    }

    pub fn apply(&self, entity_id: &str, payload: &RouteTableConfigPayload) {
        let mut overlay = self.overlay.lock().expect("overlay mutex poisoned");
        if payload.status == "archived" {
            overlay.remove(entity_id);
        } else {
            overlay.insert(
                entity_id.to_string(),
                RouteTable {
                    operator_id: payload.operator_id.clone(),
                    version: payload.version,
                    active_route_id: payload.active_route_id.clone(),
                    routes: payload.routes.clone(),
                },
            );
        }
    }

    /// overlay поверх base по ключу (HashMap::extend — совпадающий ключ
    /// заменяется, не дублируется), в отличие от number_range здесь нет
    /// неоднозначности "что победит при пересечении" — ключ уникален.
    pub fn build_snapshot(&self) -> RouteTableSnapshot {
        let overlay = self.overlay.lock().expect("overlay mutex poisoned");
        let mut by_operator = self.base.clone();
        by_operator.extend(overlay.iter().map(|(k, v)| (k.clone(), v.clone())));
        RouteTableSnapshot { by_operator }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_table(operator_id: &str, active_route_id: &str) -> RouteTable {
        RouteTable {
            operator_id: operator_id.to_string(),
            version: 1,
            active_route_id: active_route_id.to_string(),
            routes: vec![Route {
                route_id: active_route_id.to_string(),
                protocol: "SMPP".to_string(),
                failover_priority: 1,
                tps_limit: 100,
            }],
        }
    }

    fn active_payload(operator_id: &str, active_route_id: &str) -> RouteTableConfigPayload {
        RouteTableConfigPayload {
            operator_id: operator_id.to_string(),
            version: 2,
            status: "active".to_string(),
            active_route_id: active_route_id.to_string(),
            routes: vec![Route {
                route_id: active_route_id.to_string(),
                protocol: "HTTP".to_string(),
                failover_priority: 1,
                tps_limit: 200,
            }],
        }
    }

    #[test]
    fn config_overlay_starts_out_matching_the_static_baseline() {
        let overlay = ConfigOverlay::new(RouteTableSnapshot::from_tables(vec![base_table("beeline_uz", "primary")]));
        let snapshot = overlay.build_snapshot();
        assert_eq!(snapshot.for_operator("beeline_uz").unwrap().active_route_id, "primary");
    }

    #[test]
    fn config_overlay_apply_active_replaces_the_operators_table() {
        let overlay = ConfigOverlay::new(RouteTableSnapshot::from_tables(vec![base_table("beeline_uz", "primary")]));
        overlay.apply("beeline_uz", &active_payload("beeline_uz", "new_primary"));
        let snapshot = overlay.build_snapshot();
        let table = snapshot.for_operator("beeline_uz").unwrap();
        assert_eq!(table.active_route_id, "new_primary");
        assert_eq!(table.version, 2);
        assert_eq!(table.routes[0].protocol, "HTTP");
    }

    #[test]
    fn config_overlay_apply_for_new_operator_adds_it() {
        let overlay = ConfigOverlay::new(RouteTableSnapshot::from_tables(vec![base_table("beeline_uz", "primary")]));
        overlay.apply("ucell_uz", &active_payload("ucell_uz", "ucell_primary"));
        let snapshot = overlay.build_snapshot();
        // Старый оператор остался нетронутым.
        assert_eq!(snapshot.for_operator("beeline_uz").unwrap().active_route_id, "primary");
        assert_eq!(snapshot.for_operator("ucell_uz").unwrap().active_route_id, "ucell_primary");
    }

    #[test]
    fn config_overlay_apply_archived_reverts_to_static_baseline() {
        let overlay = ConfigOverlay::new(RouteTableSnapshot::from_tables(vec![base_table("beeline_uz", "primary")]));
        overlay.apply("beeline_uz", &active_payload("beeline_uz", "new_primary"));
        assert_eq!(overlay.build_snapshot().for_operator("beeline_uz").unwrap().active_route_id, "new_primary");

        let mut archived = active_payload("beeline_uz", "new_primary");
        archived.status = "archived".to_string();
        overlay.apply("beeline_uz", &archived);
        // archived снимает overlay-запись — снапшот падает обратно на статический base,
        // не удаляет оператора целиком (базовый файл остаётся источником истины).
        assert_eq!(overlay.build_snapshot().for_operator("beeline_uz").unwrap().active_route_id, "primary");
    }
}
