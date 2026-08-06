//! Ядро оркестрации Routing Service — `select_routes_for_operator` +
//! `filter_by_control_state` + `select_route_and_protocol` + `apply_failover`
//! из service_internal_methods.md §1.7 объединены в одну функцию
//! {@link resolve_final_route}, потому что "выбрать сконфигурированный
//! primary, если он здоров, иначе — следующий по приоритету среди здоровых"
//! — это одно решение, не четыре независимых шага с промежуточным
//! состоянием, которое стоило бы материализовывать отдельно.

use crate::route_table::RouteTable;
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlState {
    Active,
    Degraded,
    Paused,
}

/// `ControlSnapshot` — projection `execution.control` (scope=OPERATOR_ROUTE) по
/// `route_id`. Отсутствие записи трактуется как ACTIVE (fail-open: контроль
/// ещё не увидел проблем с этим маршрутом, не обязан существовать заранее
/// для каждого маршрута) — тот же fail-open принцип, что уже применён к
/// gap-анализу в остальных сервисах этого среза.
pub type ControlSnapshot = HashMap<String, ControlState>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinalRoute {
    pub route_id: String,
    pub protocol: String,
    pub route_version: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoutingError {
    UnknownOperator,
    NoHealthyRoute,
}

pub fn resolve_final_route(
    snapshot: &crate::route_table::RouteTableSnapshot,
    control: &ControlSnapshot,
    resolved_operator_id: &str,
) -> Result<FinalRoute, RoutingError> {
    let table: &RouteTable = snapshot.for_operator(resolved_operator_id).ok_or(RoutingError::UnknownOperator)?;

    let is_healthy = |route_id: &str| control.get(route_id).copied().unwrap_or(ControlState::Active) != ControlState::Paused;

    let mut healthy: Vec<&crate::route_table::Route> = table.routes.iter().filter(|r| is_healthy(&r.route_id)).collect();
    if healthy.is_empty() {
        return Err(RoutingError::NoHealthyRoute);
    }
    healthy.sort_by_key(|r| r.failover_priority);

    // Предпочитаем сконфигурированный active_route_id, если он в числе здоровых
    // (select_route_and_protocol); иначе — следующий по приоритету среди здоровых
    // (apply_failover) — natural fallthrough на отсортированный healthy[0].
    let chosen = healthy
        .iter()
        .find(|r| r.route_id == table.active_route_id)
        .copied()
        .unwrap_or(healthy[0]);

    Ok(FinalRoute {
        route_id: chosen.route_id.clone(),
        protocol: chosen.protocol.clone(),
        route_version: table.version.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::route_table::RouteTableSnapshot;

    fn real_snapshot() -> RouteTableSnapshot {
        let json = include_str!("../../../config_schemas/examples/routing_table.valid.json");
        let table: RouteTable = serde_json::from_str(json).unwrap();
        RouteTableSnapshot::from_tables(vec![table])
    }

    #[test]
    fn healthy_primary_is_chosen() {
        let snapshot = real_snapshot();
        let control = ControlSnapshot::new(); // пусто — все ACTIVE по fail-open
        let result = resolve_final_route(&snapshot, &control, "beeline_uz").unwrap();
        assert_eq!(result.route_id, "beeline_smpp_primary");
        assert_eq!(result.protocol, "SMPP");
        assert_eq!(result.route_version, "3"); // config_schemas/examples/routing_table.valid.json: version=3
    }

    #[test]
    fn paused_primary_fails_over_to_reserve() {
        let snapshot = real_snapshot();
        let mut control = ControlSnapshot::new();
        control.insert("beeline_smpp_primary".to_string(), ControlState::Paused);
        let result = resolve_final_route(&snapshot, &control, "beeline_uz").unwrap();
        assert_eq!(result.route_id, "beeline_http_reserve", "primary PAUSED — должны уйти на резерв по приоритету");
        assert_eq!(result.protocol, "HTTP", "failover может менять протокол, не только route_id");
    }

    #[test]
    fn degraded_primary_still_preferred_over_reserve() {
        // DEGRADED — не PAUSED: маршрут остаётся в числе здоровых, sконфигурированный
        // primary всё ещё предпочтителен (деградация не значит полную недоступность).
        let snapshot = real_snapshot();
        let mut control = ControlSnapshot::new();
        control.insert("beeline_smpp_primary".to_string(), ControlState::Degraded);
        let result = resolve_final_route(&snapshot, &control, "beeline_uz").unwrap();
        assert_eq!(result.route_id, "beeline_smpp_primary");
    }

    #[test]
    fn all_routes_paused_returns_no_healthy_route_error() {
        let snapshot = real_snapshot();
        let mut control = ControlSnapshot::new();
        control.insert("beeline_smpp_primary".to_string(), ControlState::Paused);
        control.insert("beeline_http_reserve".to_string(), ControlState::Paused);
        let result = resolve_final_route(&snapshot, &control, "beeline_uz");
        assert_eq!(result, Err(RoutingError::NoHealthyRoute));
    }

    #[test]
    fn unknown_operator_returns_error_not_panic() {
        let snapshot = real_snapshot();
        let control = ControlSnapshot::new();
        let result = resolve_final_route(&snapshot, &control, "unknown_operator");
        assert_eq!(result, Err(RoutingError::UnknownOperator));
    }

    /// development_plan.md 5.5 — `Main.rs` теперь мержит `ROUTE_TABLE_PATH` +
    /// `ROUTE_TABLE_EXTRA_PATHS` в один снапшот (раньше — заглушка на ровно
    /// одного оператора, Фаза 2.2). Этот тест воспроизводит тот же мердж
    /// напрямую на трёх реальных routing_table-примерах и доказывает, что все
    /// три резолвятся независимо из одного снапшота — не только "компилируется",
    /// а реально проверено, что operator_id из разных файлов не коллизируют.
    #[test]
    fn snapshot_merged_from_multiple_files_resolves_each_operator_independently() {
        let beeline: RouteTable = serde_json::from_str(include_str!("../../../config_schemas/examples/routing_table.valid.json")).unwrap();
        let ucell: RouteTable = serde_json::from_str(include_str!("../../../config_schemas/examples/routing_table.ucell_uz.valid.json")).unwrap();
        let uzmobile: RouteTable = serde_json::from_str(include_str!("../../../config_schemas/examples/routing_table.uzmobile_uz.valid.json")).unwrap();
        let snapshot = RouteTableSnapshot::from_tables(vec![beeline, ucell, uzmobile]);
        let control = ControlSnapshot::new();

        assert_eq!(resolve_final_route(&snapshot, &control, "beeline_uz").unwrap().route_id, "beeline_smpp_primary");
        assert_eq!(resolve_final_route(&snapshot, &control, "ucell_uz").unwrap().route_id, "ucell_smpp_primary");
        assert_eq!(resolve_final_route(&snapshot, &control, "uzmobile_uz").unwrap().route_id, "uzmobile_smpp_primary");
    }
}
