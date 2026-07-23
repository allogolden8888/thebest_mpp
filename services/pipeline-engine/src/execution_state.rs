//! `ExecutionState` + `resolve_next_stage` + `handle_stage_completed` —
//! service_internal_methods.md §1.4. Аккумулирует результаты стадий (нужны
//! следующим стадиям для построения их `StageExecuteCommand` —
//! `build_stage_execute` не перечитывает msgctx за этими полями, только то,
//! что реально пришло от предыдущих стадий, тем же принципом, что
//! `BillingExtension.segment_count`/`category` не перечитываются Billing).

use crate::pipeline_graph::PipelineDefinition;
use crate::proto::common::{Outcome, StageCompletedEvent};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct ExecutionState {
    pub message_id: String,
    pub pipeline_version: u32,
    pub current_node_id: String,
    pub attempt: i32,
    pub config_versions: HashMap<String, i64>,

    // Аккумулированные результаты предыдущих стадий — нужны build_stage_execute
    // для следующей стадии, без повторного чтения msgctx этой стадией.
    pub resolved_operator_id: Option<String>,
    pub category: Option<String>,
    pub segment_count: i32,
    pub route_id: Option<String>,
    pub protocol: Option<i32>, // proto Protocol as i32 — не нуждается в раскодировке здесь
}

impl ExecutionState {
    /// `handle_incoming` — инициализация, первая стадия всегда DestinationResolution
    /// (жёстко зафиксировано графом: entry_node_id уже провалидирован в
    /// config_schemas/pipeline.schema.json как узел DESTINATION_RESOLUTION).
    pub fn new_from_incoming(message_id: String, pipeline: &PipelineDefinition, segment_count: i32) -> Self {
        Self {
            message_id,
            pipeline_version: pipeline.version,
            current_node_id: pipeline.entry_node_id.clone(),
            attempt: 1,
            config_versions: HashMap::new(),
            resolved_operator_id: None,
            category: None,
            segment_count,
            route_id: None,
            protocol: None,
        }
    }

    fn apply_stage_result(&mut self, event: &StageCompletedEvent) {
        use crate::proto::common::stage_completed_event::StageResult;
        match &event.stage_result {
            Some(StageResult::DestinationResolution(r)) => {
                self.resolved_operator_id = Some(r.resolved_operator_id.clone());
            }
            Some(StageResult::Policy(r)) => {
                self.category = Some(r.category.clone());
            }
            Some(StageResult::Billing(_)) => {
                // category уже записана на шаге Policy — Billing только эхом
                // подтверждает то же значение (platform-contracts: BillingResult.category
                // "эхо категории, с которой был вызван Billing"), не источник истины здесь.
            }
            Some(StageResult::Routing(r)) => {
                self.route_id = Some(r.route_id.clone());
                self.protocol = Some(r.protocol);
            }
            _ => {}
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NextStageDecision {
    pub node_id: String,
    pub stage_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Decision {
    Next(NextStageDecision),
    Terminal,
}

fn outcome_key(outcome: Outcome) -> &'static str {
    match outcome {
        Outcome::Succeeded => "SUCCEEDED",
        Outcome::Rejected => "REJECTED",
        Outcome::Failed => "FAILED",
        Outcome::TimedOut => "TIMED_OUT",
        Outcome::RetryExhausted => "RETRY_EXHAUSTED",
        Outcome::SubmissionOutcomeUnknown => "SUBMISSION_OUTCOME_UNKNOWN",
        Outcome::DeliveryUnresolved => "DELIVERY_UNRESOLVED",
        Outcome::Unspecified => "UNSPECIFIED",
    }
}

/// Особый случай (service_internal_methods.md §1.4): если завершилась
/// BILLING, а сохранённая category == "BLOCKED" — Terminal, **независимо от
/// того, что граф мог бы предписать** для обычного SUCCEEDED от Billing.
/// Проверяется ДО обращения к графу — defense-in-depth поверх графовых
/// данных, не полагается на то, что автор графа правильно developed
/// отдельный BLOCKED-узел (hld.md §5.3.1).
pub fn resolve_next_stage(state: &ExecutionState, pipeline: &PipelineDefinition, completed_event: &StageCompletedEvent) -> Decision {
    let completed_node = pipeline.node(&state.current_node_id).expect("current_node_id всегда валиден — CAS не даёт разъехаться");

    if completed_node.stage_name == "BILLING" && state.category.as_deref() == Some("BLOCKED") {
        return Decision::Terminal;
    }

    let outcome = Outcome::try_from(completed_event.outcome).unwrap_or(Outcome::Unspecified);
    let key = outcome_key(outcome);

    match completed_node.next.get(key) {
        Some(Some(next_node_id)) => {
            let next_node = pipeline.node(next_node_id).expect("next обязан ссылаться на существующий узел");
            Decision::Next(NextStageDecision { node_id: next_node.node_id.clone(), stage_name: next_node.stage_name.clone() })
        }
        _ => Decision::Terminal, // явный null в графе, либо исход не перечислен для этого узла
    }
}

/// `handle_stage_completed` — обновляет состояние, затем резолвит следующий шаг.
pub fn handle_stage_completed(state: &mut ExecutionState, pipeline: &PipelineDefinition, event: &StageCompletedEvent) -> Decision {
    state.apply_stage_result(event);
    let decision = resolve_next_stage(state, pipeline, event);
    if let Decision::Next(ref next) = decision {
        state.current_node_id = next.node_id.clone();
        state.attempt = 1;
    }
    decision
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::common::stage_completed_event::StageResult;
    use crate::proto::common::{BillingResult, DestinationResolutionResult, PolicyResult, RoutingResult};

    fn real_pipeline() -> PipelineDefinition {
        let json = include_str!("../../../config_schemas/examples/pipeline.valid.json");
        serde_json::from_str(json).unwrap()
    }

    fn completed(stage_execution_id: &str, outcome: Outcome, result: Option<StageResult>) -> StageCompletedEvent {
        StageCompletedEvent {
            event_id: format!("evt-{stage_execution_id}"),
            message_id: "m1".into(),
            stage_execution_id: stage_execution_id.into(),
            attempt: 1,
            stage_name: 0,
            outcome: outcome as i32,
            reason_code: String::new(),
            retryable: false,
            retry_after: None,
            traceparent: "tp1".into(),
            completed_at: None,
            stage_result: result,
        }
    }

    #[test]
    fn full_happy_path_walks_entire_real_graph_to_terminal() {
        let pipeline = real_pipeline();
        let mut state = ExecutionState::new_from_incoming("m1".into(), &pipeline, 2);
        assert_eq!(state.current_node_id, "n1_destination_resolution");

        // DestinationResolution SUCCEEDED -> Policy
        let d = handle_stage_completed(&mut state, &pipeline, &completed("se1", Outcome::Succeeded,
            Some(StageResult::DestinationResolution(DestinationResolutionResult { resolved_operator_id: "beeline".into() }))));
        assert_eq!(d, Decision::Next(NextStageDecision { node_id: "n2_policy".into(), stage_name: "POLICY".into() }));
        assert_eq!(state.resolved_operator_id.as_deref(), Some("beeline"));

        // Policy SUCCEEDED (category=TRANSACTION) -> Billing
        let d = handle_stage_completed(&mut state, &pipeline, &completed("se2", Outcome::Succeeded,
            Some(StageResult::Policy(PolicyResult { category: "TRANSACTION".into() }))));
        assert_eq!(d, Decision::Next(NextStageDecision { node_id: "n3_billing".into(), stage_name: "BILLING".into() }));
        assert_eq!(state.category.as_deref(), Some("TRANSACTION"));

        // Billing SUCCEEDED (category still TRANSACTION, not BLOCKED) -> Routing
        let d = handle_stage_completed(&mut state, &pipeline, &completed("se3", Outcome::Succeeded,
            Some(StageResult::Billing(BillingResult { charge_id: "se3".into(), category: "TRANSACTION".into(), amount: None }))));
        assert_eq!(d, Decision::Next(NextStageDecision { node_id: "n4_routing".into(), stage_name: "ROUTING".into() }));

        // Routing SUCCEEDED -> Delivery
        let d = handle_stage_completed(&mut state, &pipeline, &completed("se4", Outcome::Succeeded,
            Some(StageResult::Routing(RoutingResult { route_id: "beeline_smpp_primary".into(), protocol: 1, route_version: "3".into() }))));
        assert_eq!(d, Decision::Next(NextStageDecision { node_id: "n5_delivery".into(), stage_name: "DELIVERY".into() }));
        assert_eq!(state.route_id.as_deref(), Some("beeline_smpp_primary"));

        // Delivery SUCCEEDED -> DeliveryReconciliation
        let d = handle_stage_completed(&mut state, &pipeline, &completed("se5", Outcome::Succeeded, None));
        assert_eq!(d, Decision::Next(NextStageDecision { node_id: "n6_reconciliation".into(), stage_name: "DELIVERY_RECONCILIATION".into() }));

        // DeliveryReconciliation SUCCEEDED -> Terminal (null в графе)
        let d = handle_stage_completed(&mut state, &pipeline, &completed("se6", Outcome::Succeeded, None));
        assert_eq!(d, Decision::Terminal);
    }

    #[test]
    fn policy_rejected_routes_to_billing_blocked_node_then_terminates() {
        // Доказывает то же графовое правило, что config_schemas/validate_all.py
        // уже проверил структурно на самом файле — здесь то же правило
        // проверяется через реальный обход рантайм-кодом, не только валидацией конфига.
        let pipeline = real_pipeline();
        let mut state = ExecutionState::new_from_incoming("m1".into(), &pipeline, 1);
        handle_stage_completed(&mut state, &pipeline, &completed("se1", Outcome::Succeeded,
            Some(StageResult::DestinationResolution(DestinationResolutionResult { resolved_operator_id: "beeline".into() }))));

        let d = handle_stage_completed(&mut state, &pipeline, &completed("se2", Outcome::Rejected, None));
        assert_eq!(d, Decision::Next(NextStageDecision { node_id: "n_billing_blocked".into(), stage_name: "BILLING".into() }),
            "REJECTED от Policy обязан вести в билинговый узел, не сразу в Terminal (hld.md §5.3.1)");

        let d = handle_stage_completed(&mut state, &pipeline, &completed("se3", Outcome::Succeeded,
            Some(StageResult::Billing(BillingResult { charge_id: "se3".into(), category: "BLOCKED".into(), amount: None }))));
        assert_eq!(d, Decision::Terminal, "после тарификации BLOCKED пайплайн обязан завершиться, Routing/Delivery пропускаются");
    }

    #[test]
    fn billing_blocked_override_fires_even_if_graph_would_route_onward() {
        // Ключевой тест: конструируем ПАТОЛОГИЧЕСКИЙ граф, где узел BILLING с
        // SUCCEEDED ведёт в ROUTING (как обычный "хороший" путь), но category в
        // состоянии — BLOCKED (как будто конфигурация графа НЕ развела два случая
        // отдельными узлами). Override обязан сработать НЕЗАВИСИМО от графа —
        // ровно формулировка service_internal_methods.md §1.4: "независимо от
        // того, что граф конфигурации мог бы предписать".
        let mut pipeline = real_pipeline();
        for node in pipeline.nodes_mut() {
            if node.node_id == "n_billing_blocked" {
                node.next.insert("SUCCEEDED".to_string(), Some("n4_routing".to_string()));
            }
        }

        let mut state = ExecutionState::new_from_incoming("m1".into(), &pipeline, 1);
        state.current_node_id = "n_billing_blocked".to_string();
        state.category = Some("BLOCKED".to_string());

        let d = handle_stage_completed(&mut state, &pipeline, &completed("se-x", Outcome::Succeeded,
            Some(StageResult::Billing(BillingResult { charge_id: "se-x".into(), category: "BLOCKED".into(), amount: None }))));

        assert_eq!(d, Decision::Terminal, "override обязан victory над графом, предписывающим переход в ROUTING");
    }

    #[test]
    fn failed_outcome_with_no_configured_next_terminates_not_panics() {
        let pipeline = real_pipeline();
        let mut state = ExecutionState::new_from_incoming("m1".into(), &pipeline, 1);
        let d = handle_stage_completed(&mut state, &pipeline, &completed("se1", Outcome::Failed, None));
        assert_eq!(d, Decision::Terminal);
    }
}
