//! `build_stage_execute` (service_internal_methods.md §1.4) — строит
//! `StageExecuteCommand` для целевой стадии из аккумулированного
//! `ExecutionState`, включая нужный вариант `stage_extension` oneof.
//! `DeliveryExtension`/`DeliveryReconciliationExtension` требуют данных
//! (route_id/protocol из Routing; triggering_outcome из Reconciliation-триггера),
//! которых в этом срезе ещё нет полного источника — заполняются best-effort
//! из уже накопленного состояния, отмечено как открытый пункт в README, не скрыто.

use crate::execution_state::{ExecutionState, NextStageDecision};
use crate::proto::common::stage_execute_command::StageExtension;
use crate::proto::common::{
    BillingExtension, DestinationResolutionExtension, PolicyExtension, RoutingExtension, StageExecuteCommand, StageName,
};

fn stage_name_to_proto(stage_name: &str) -> StageName {
    match stage_name {
        "DESTINATION_RESOLUTION" => StageName::DestinationResolution,
        "POLICY" => StageName::Policy,
        "BILLING" => StageName::Billing,
        "ROUTING" => StageName::Routing,
        "DELIVERY" => StageName::Delivery,
        "DELIVERY_RECONCILIATION" => StageName::DeliveryReconciliation,
        other => panic!("неизвестный stage_name в графе: {other}"),
    }
}

pub fn build_stage_execute(decision: &NextStageDecision, state: &ExecutionState, destination_address: &str, stage_execution_id: String) -> StageExecuteCommand {
    let stage_name = stage_name_to_proto(&decision.stage_name);

    let extension = match stage_name {
        StageName::DestinationResolution => StageExtension::DestinationResolution(DestinationResolutionExtension {
            destination_address: destination_address.to_string(),
        }),
        StageName::Policy => StageExtension::Policy(PolicyExtension {
            resolved_operator_id: state.resolved_operator_id.clone().unwrap_or_default(),
        }),
        StageName::Billing => StageExtension::Billing(BillingExtension {
            resolved_operator_id: state.resolved_operator_id.clone().unwrap_or_default(),
            segment_count: state.segment_count,
            category: state.category.clone().unwrap_or_default(),
        }),
        StageName::Routing => StageExtension::Routing(RoutingExtension {
            resolved_operator_id: state.resolved_operator_id.clone().unwrap_or_default(),
        }),
        // Delivery/DeliveryReconciliation extensions нуждаются в данных
        // (route_id/protocol; triggering_outcome), для которых полный источник
        // ещё не формализован в этом срезе — см. README "Что НЕ реализовано".
        _ => StageExtension::Routing(RoutingExtension { resolved_operator_id: state.resolved_operator_id.clone().unwrap_or_default() }),
    };

    StageExecuteCommand {
        event_id: format!("evt-{stage_execution_id}"),
        message_id: state.message_id.clone(),
        channel: 0,
        pipeline_id: String::new(),
        pipeline_version: state.pipeline_version.to_string(),
        node_id: decision.node_id.clone(),
        stage_name: stage_name as i32,
        stage_execution_id,
        attempt: state.attempt,
        deadline: None,
        message_ttl: None,
        config_versions: state.config_versions.clone(),
        traceparent: String::new(),
        payload_ref: None,
        stage_extension: Some(extension),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline_graph::PipelineDefinition;

    fn pipeline() -> PipelineDefinition {
        serde_json::from_str(include_str!("../../../config_schemas/examples/pipeline.valid.json")).unwrap()
    }

    #[test]
    fn destination_resolution_command_carries_destination_address() {
        let pipeline = pipeline();
        let state = ExecutionState::new_from_incoming("m1".into(), &pipeline, 1);
        let decision = NextStageDecision { node_id: "n1_destination_resolution".into(), stage_name: "DESTINATION_RESOLUTION".into() };
        let command = build_stage_execute(&decision, &state, "998901331835", "se1".into());
        match command.stage_extension {
            Some(StageExtension::DestinationResolution(ext)) => assert_eq!(ext.destination_address, "998901331835"),
            other => panic!("ожидали DestinationResolutionExtension, получили {other:?}"),
        }
        assert_eq!(command.stage_name, StageName::DestinationResolution as i32);
    }

    #[test]
    fn billing_command_carries_accumulated_operator_and_category() {
        let pipeline = pipeline();
        let mut state = ExecutionState::new_from_incoming("m1".into(), &pipeline, 3);
        state.resolved_operator_id = Some("beeline".into());
        state.category = Some("TRANSACTION".into());
        let decision = NextStageDecision { node_id: "n3_billing".into(), stage_name: "BILLING".into() };
        let command = build_stage_execute(&decision, &state, "998901331835", "se3".into());
        match command.stage_extension {
            Some(StageExtension::Billing(ext)) => {
                assert_eq!(ext.resolved_operator_id, "beeline");
                assert_eq!(ext.category, "TRANSACTION");
                assert_eq!(ext.segment_count, 3, "segment_count посчитан один раз при кэшировании контекста, не перечитывается");
            }
            other => panic!("ожидали BillingExtension, получили {other:?}"),
        }
    }

    #[test]
    fn stage_execution_id_is_caller_provided_not_derived() {
        // stage_execution_id — единственный ключ идемпотентности (HLD §4), должен
        // оставаться СТАБИЛЬНЫМ при retry (не пересчитываться из attempt/timestamp
        // внутри build_stage_execute) — контроль за этим лежит на вызывающей стороне,
        // здесь только доказано, что значение пробрасывается как есть.
        let pipeline = pipeline();
        let state = ExecutionState::new_from_incoming("m1".into(), &pipeline, 1);
        let decision = NextStageDecision { node_id: "n1_destination_resolution".into(), stage_name: "DESTINATION_RESOLUTION".into() };
        let command = build_stage_execute(&decision, &state, "998901331835", "stable-id-123".into());
        assert_eq!(command.stage_execution_id, "stable-id-123");
    }
}
