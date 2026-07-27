//! `ExecutionState` + `resolve_next_stage` + `handle_stage_completed` —
//! service_internal_methods.md §1.4. Аккумулирует результаты стадий (нужны
//! следующим стадиям для построения их `StageExecuteCommand` —
//! `build_stage_execute` не перечитывает msgctx за этими полями, только то,
//! что реально пришло от предыдущих стадий, тем же принципом, что
//! `BillingExtension.segment_count`/`category` не перечитываются Billing).

use crate::pipeline_graph::PipelineDefinition;
use crate::proto::common::{Outcome, StageCompletedEvent, StageName};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct ExecutionState {
    pub message_id: String,
    pub pipeline_version: u32,
    pub current_node_id: String,
    pub attempt: i32,
    pub config_versions: HashMap<String, i64>,

    /// `stage_execution_id`, реально диспетчеризованный для `current_node_id` —
    /// найдено кодревью: без этого поля `handle_stage_completed` не может
    /// отличить событие, относящееся к текущей попытке, от устаревшего/
    /// дублирующегося `StageCompletedEvent` для уже пройденной стадии (Kafka
    /// at-least-once гарантирует такие дубликаты на практике, не только
    /// теоретически). См. проверку в {@link handle_stage_completed}.
    pub awaiting_stage_execution_id: Option<String>,

    // Аккумулированные результаты предыдущих стадий — нужны build_stage_execute
    // для следующей стадии, без повторного чтения msgctx этой стадией.
    pub resolved_operator_id: Option<String>,
    pub category: Option<String>,
    pub segment_count: i32,
    pub route_id: Option<String>,
    pub protocol: Option<i32>, // proto Protocol as i32 — не нуждается в раскодировке здесь
    pub route_version: Option<String>,

    /// Сложено из отдельного `DestinationStore` (development_plan.md 4.2,
    /// Redis CAS интеграция) — раньше хранилось второй отдельной
    /// `Arc<Mutex<HashMap>>` картой (см. README/kafka_io.rs до этой правки),
    /// теперь просто поле того же состояния: один Redis HASH на message_id,
    /// не два независимых стора, которые надо было бы держать в синхроне
    /// вручную.
    pub destination_address: String,

    /// Дедлайн текущей диспетчеризованной попытки (unix ms) —
    /// `cas_transition_and_track_deadline` пишет его в `deadlines:{bucket}`
    /// той же атомарной Lua-транзакцией, что и CAS (hld.md:770/912,
    /// `src/redis_cas.rs`). Не путать с `StageExecuteCommand.deadline`
    /// (wire-поле, отдельный, всё ещё не реализованный пробел — см. README) —
    /// это внутреннее поле Pipeline Engine для Critical Sweep.
    pub deadline_ms: i64,
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
            awaiting_stage_execution_id: None,
            resolved_operator_id: None,
            category: None,
            segment_count,
            route_id: None,
            protocol: None,
            route_version: None,
            destination_address: String::new(),
            deadline_ms: 0,
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
                self.route_version = Some(r.route_version.clone());
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
    /// Событие не относится к стадии, которую мы сейчас реально ждём
    /// (устаревший/дублирующийся `StageCompletedEvent`) — состояние НЕ
    /// меняется, ничего не публикуется, но это НЕ Terminal: пайплайн
    /// по-прежнему в процессе, просто это конкретное событие — шум.
    Ignored { reason: &'static str },
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

fn proto_stage_name_to_key(stage_name_i32: i32) -> Option<&'static str> {
    match StageName::try_from(stage_name_i32).ok()? {
        StageName::DestinationResolution => Some("DESTINATION_RESOLUTION"),
        StageName::Policy => Some("POLICY"),
        StageName::Billing => Some("BILLING"),
        StageName::Routing => Some("ROUTING"),
        StageName::Delivery => Some("DELIVERY"),
        StageName::DeliveryReconciliation => Some("DELIVERY_RECONCILIATION"),
        StageName::Unspecified => None,
    }
}

/// Особый случай (service_internal_methods.md §1.4): если завершилась
/// BILLING, а сохранённая category == "BLOCKED" — Terminal, **независимо от
/// того, что граф мог бы предписать** для обычного SUCCEEDED от Billing.
/// Проверяется ДО обращения к графу — defense-in-depth поверх графовых
/// данных, не полагается на то, что автор графа правильно развёл
/// отдельный BLOCKED-узел (hld.md §5.3.1).
fn resolve_next_stage(state: &ExecutionState, pipeline: &PipelineDefinition, completed_event: &StageCompletedEvent) -> Decision {
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

/// `handle_stage_completed` — валидирует, что событие относится к стадии,
/// которую мы реально ждём (см. `ExecutionState::awaiting_stage_execution_id`),
/// затем обновляет состояние и резолвит следующий шаг.
///
/// **Найдено кодревью, исправлено здесь:** без этой проверки устаревший/
/// дублирующийся `StageCompletedEvent` для уже пройденной стадии (гарантированно
/// возможно под Kafka at-least-once — не гипотетический случай) применялся бы
/// вслепую против того, что сейчас в `current_node_id`, что могло молча
/// продвинуть пайплайн повторно и пропустить реальную следующую стадию
/// (например, Policy со всеми её проверками).
pub fn handle_stage_completed(state: &mut ExecutionState, pipeline: &PipelineDefinition, event: &StageCompletedEvent) -> Decision {
    let completed_node = pipeline.node(&state.current_node_id).expect("current_node_id всегда валиден");
    let expected_key = completed_node.stage_name.as_str();
    let event_key = proto_stage_name_to_key(event.stage_name);

    if event_key != Some(expected_key) {
        return Decision::Ignored { reason: "stage_name не совпадает с текущим узлом" };
    }
    if state.awaiting_stage_execution_id.as_deref() != Some(event.stage_execution_id.as_str()) {
        return Decision::Ignored { reason: "stage_execution_id не совпадает с диспетчеризованным — устаревшее/дублирующееся событие" };
    }

    state.apply_stage_result(event);
    let decision = resolve_next_stage(state, pipeline, event);
    if let Decision::Next(ref next) = decision {
        state.current_node_id = next.node_id.clone();
        state.attempt = 1;
        state.awaiting_stage_execution_id = None; // выставляется заново вызывающей стороной при диспетчеризации (kafka_io.rs)
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

    fn completed(stage_execution_id: &str, stage_name: StageName, outcome: Outcome, result: Option<StageResult>) -> StageCompletedEvent {
        StageCompletedEvent {
            event_id: format!("evt-{stage_execution_id}"),
            message_id: "m1".into(),
            stage_execution_id: stage_execution_id.into(),
            attempt: 1,
            stage_name: stage_name as i32,
            outcome: outcome as i32,
            reason_code: String::new(),
            retryable: false,
            retry_after: None,
            traceparent: "tp1".into(),
            completed_at: None,
            stage_result: result,
        }
    }

    /// В реальном run() awaiting_stage_execution_id выставляется kafka_io::handle_incoming/advance
    /// сразу после генерации id для только что диспетчеризованной команды — тесты здесь
    /// эмулируют это вручную, чтобы проверять handle_stage_completed изолированно.
    fn dispatch(state: &mut ExecutionState, stage_execution_id: &str) {
        state.awaiting_stage_execution_id = Some(stage_execution_id.to_string());
    }

    #[test]
    fn full_happy_path_walks_entire_real_graph_to_terminal() {
        let pipeline = real_pipeline();
        let mut state = ExecutionState::new_from_incoming("m1".into(), &pipeline, 2);
        assert_eq!(state.current_node_id, "n1_destination_resolution");

        // DestinationResolution SUCCEEDED -> Policy
        dispatch(&mut state, "se1");
        let d = handle_stage_completed(&mut state, &pipeline, &completed("se1", StageName::DestinationResolution, Outcome::Succeeded,
            Some(StageResult::DestinationResolution(DestinationResolutionResult { resolved_operator_id: "beeline".into() }))));
        assert_eq!(d, Decision::Next(NextStageDecision { node_id: "n2_policy".into(), stage_name: "POLICY".into() }));
        assert_eq!(state.resolved_operator_id.as_deref(), Some("beeline"));

        // Policy SUCCEEDED (category=TRANSACTION) -> Billing
        dispatch(&mut state, "se2");
        let d = handle_stage_completed(&mut state, &pipeline, &completed("se2", StageName::Policy, Outcome::Succeeded,
            Some(StageResult::Policy(PolicyResult { category: "TRANSACTION".into() }))));
        assert_eq!(d, Decision::Next(NextStageDecision { node_id: "n3_billing".into(), stage_name: "BILLING".into() }));
        assert_eq!(state.category.as_deref(), Some("TRANSACTION"));

        // Billing SUCCEEDED (category still TRANSACTION, not BLOCKED) -> Routing
        dispatch(&mut state, "se3");
        let d = handle_stage_completed(&mut state, &pipeline, &completed("se3", StageName::Billing, Outcome::Succeeded,
            Some(StageResult::Billing(BillingResult { charge_id: "se3".into(), category: "TRANSACTION".into(), amount: None }))));
        assert_eq!(d, Decision::Next(NextStageDecision { node_id: "n4_routing".into(), stage_name: "ROUTING".into() }));

        // Routing SUCCEEDED -> Delivery
        dispatch(&mut state, "se4");
        let d = handle_stage_completed(&mut state, &pipeline, &completed("se4", StageName::Routing, Outcome::Succeeded,
            Some(StageResult::Routing(RoutingResult { route_id: "beeline_smpp_primary".into(), protocol: 1, route_version: "3".into() }))));
        assert_eq!(d, Decision::Next(NextStageDecision { node_id: "n5_delivery".into(), stage_name: "DELIVERY".into() }));
        assert_eq!(state.route_id.as_deref(), Some("beeline_smpp_primary"));

        // Delivery SUCCEEDED -> DeliveryReconciliation
        dispatch(&mut state, "se5");
        let d = handle_stage_completed(&mut state, &pipeline, &completed("se5", StageName::Delivery, Outcome::Succeeded, None));
        assert_eq!(d, Decision::Next(NextStageDecision { node_id: "n6_reconciliation".into(), stage_name: "DELIVERY_RECONCILIATION".into() }));

        // DeliveryReconciliation SUCCEEDED -> Terminal (null в графе)
        dispatch(&mut state, "se6");
        let d = handle_stage_completed(&mut state, &pipeline, &completed("se6", StageName::DeliveryReconciliation, Outcome::Succeeded, None));
        assert_eq!(d, Decision::Terminal);
    }

    #[test]
    fn policy_rejected_routes_to_billing_blocked_node_then_terminates() {
        let pipeline = real_pipeline();
        let mut state = ExecutionState::new_from_incoming("m1".into(), &pipeline, 1);
        dispatch(&mut state, "se1");
        handle_stage_completed(&mut state, &pipeline, &completed("se1", StageName::DestinationResolution, Outcome::Succeeded,
            Some(StageResult::DestinationResolution(DestinationResolutionResult { resolved_operator_id: "beeline".into() }))));

        dispatch(&mut state, "se2");
        let d = handle_stage_completed(&mut state, &pipeline, &completed("se2", StageName::Policy, Outcome::Rejected, None));
        assert_eq!(d, Decision::Next(NextStageDecision { node_id: "n_billing_blocked".into(), stage_name: "BILLING".into() }),
            "REJECTED от Policy обязан вести в билинговый узел, не сразу в Terminal (hld.md §5.3.1)");

        dispatch(&mut state, "se3");
        let d = handle_stage_completed(&mut state, &pipeline, &completed("se3", StageName::Billing, Outcome::Succeeded,
            Some(StageResult::Billing(BillingResult { charge_id: "se3".into(), category: "BLOCKED".into(), amount: None }))));
        assert_eq!(d, Decision::Terminal, "после тарификации BLOCKED пайплайн обязан завершиться, Routing/Delivery пропускаются");
    }

    #[test]
    fn billing_blocked_override_fires_even_if_graph_would_route_onward() {
        let mut pipeline = real_pipeline();
        for node in pipeline.nodes_mut() {
            if node.node_id == "n_billing_blocked" {
                node.next.insert("SUCCEEDED".to_string(), Some("n4_routing".to_string()));
            }
        }

        let mut state = ExecutionState::new_from_incoming("m1".into(), &pipeline, 1);
        state.current_node_id = "n_billing_blocked".to_string();
        state.category = Some("BLOCKED".to_string());
        dispatch(&mut state, "se-x");

        let d = handle_stage_completed(&mut state, &pipeline, &completed("se-x", StageName::Billing, Outcome::Succeeded,
            Some(StageResult::Billing(BillingResult { charge_id: "se-x".into(), category: "BLOCKED".into(), amount: None }))));

        assert_eq!(d, Decision::Terminal, "override обязан victory над графом, предписывающим переход в ROUTING");
    }

    #[test]
    fn failed_outcome_with_no_configured_next_terminates_not_panics() {
        let pipeline = real_pipeline();
        let mut state = ExecutionState::new_from_incoming("m1".into(), &pipeline, 1);
        dispatch(&mut state, "se1");
        let d = handle_stage_completed(&mut state, &pipeline, &completed("se1", StageName::DestinationResolution, Outcome::Failed, None));
        assert_eq!(d, Decision::Terminal);
    }

    #[test]
    fn stale_duplicate_event_for_already_advanced_stage_is_ignored_not_applied() {
        // Найдено кодревью: под Kafka at-least-once дублирующийся StageCompletedEvent
        // для УЖЕ пройденной стадии гарантированно возможен. Пайплайн уже продвинулся
        // с DestinationResolution на Policy; повторное (устаревшее) событие для
        // DestinationResolution с тем же message_id обязано быть проигнорировано,
        // не применено повторно (иначе пайплайн откатился/продвинулся бы мимо Policy).
        let pipeline = real_pipeline();
        let mut state = ExecutionState::new_from_incoming("m1".into(), &pipeline, 1);
        dispatch(&mut state, "se1");
        handle_stage_completed(&mut state, &pipeline, &completed("se1", StageName::DestinationResolution, Outcome::Succeeded,
            Some(StageResult::DestinationResolution(DestinationResolutionResult { resolved_operator_id: "beeline".into() }))));
        assert_eq!(state.current_node_id, "n2_policy");
        dispatch(&mut state, "se2"); // теперь ждём Policy с se2

        // Дубликат старого DestinationResolution-события (тот же stage_execution_id se1) прилетает снова.
        let stale = completed("se1", StageName::DestinationResolution, Outcome::Succeeded,
            Some(StageResult::DestinationResolution(DestinationResolutionResult { resolved_operator_id: "ucell".into() })));
        let d = handle_stage_completed(&mut state, &pipeline, &stale);

        assert_eq!(d, Decision::Ignored { reason: "stage_name не совпадает с текущим узлом" });
        assert_eq!(state.current_node_id, "n2_policy", "состояние не должно было измениться от устаревшего события");
        assert_eq!(state.resolved_operator_id.as_deref(), Some("beeline"), "старое значение не должно было перезаписаться значением из дубликата");
    }

    #[test]
    fn mismatched_stage_execution_id_for_same_stage_is_ignored() {
        // Тот же класс проблемы, но stage_name совпадает (не отличимо по одному
        // только имени стадии) — например, повторная попытка с новым attempt
        // прислала команду, а событие относится к СТАРОЙ, уже неактуальной попытке.
        let pipeline = real_pipeline();
        let mut state = ExecutionState::new_from_incoming("m1".into(), &pipeline, 1);
        dispatch(&mut state, "se1-attempt2"); // реально ждём вторую попытку

        let stale_attempt1 = completed("se1-attempt1", StageName::DestinationResolution, Outcome::Succeeded,
            Some(StageResult::DestinationResolution(DestinationResolutionResult { resolved_operator_id: "beeline".into() })));
        let d = handle_stage_completed(&mut state, &pipeline, &stale_attempt1);

        assert_eq!(d, Decision::Ignored { reason: "stage_execution_id не совпадает с диспетчеризованным — устаревшее/дублирующееся событие" });
        assert_eq!(state.current_node_id, "n1_destination_resolution", "не должны были продвинуться по устаревшей попытке");
    }
}
