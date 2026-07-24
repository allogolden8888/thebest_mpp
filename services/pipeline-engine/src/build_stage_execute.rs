//! `build_stage_execute` (service_internal_methods.md §1.4) — строит
//! `StageExecuteCommand` для целевой стадии из аккумулированного
//! `ExecutionState`, включая нужный вариант `stage_extension` oneof.

use crate::execution_state::{ExecutionState, NextStageDecision};
use crate::proto::common::stage_execute_command::StageExtension;
use crate::proto::common::{
    BillingExtension, DeliveryExtension, DeliveryReconciliationExtension, DestinationResolutionExtension, Outcome,
    PolicyExtension, RoutingExtension, StageExecuteCommand, StageName,
};

fn stage_name_to_proto(stage_name: &str) -> Result<StageName, String> {
    match stage_name {
        "DESTINATION_RESOLUTION" => Ok(StageName::DestinationResolution),
        "POLICY" => Ok(StageName::Policy),
        "BILLING" => Ok(StageName::Billing),
        "ROUTING" => Ok(StageName::Routing),
        "DELIVERY" => Ok(StageName::Delivery),
        "DELIVERY_RECONCILIATION" => Ok(StageName::DeliveryReconciliation),
        other => Err(format!("неизвестный stage_name в графе: {other}")),
    }
}

/// Найдено кодревью, исправлено здесь: предыдущая версия использовала
/// `RoutingExtension` как fallback-заглушку для ОБОИХ вариантов Delivery/
/// DeliveryReconciliation — реально достижимо через настоящий граф пайплайна
/// и покрывалось собственным happy-path тестом сервиса, который проверял
/// только факт продвижения по графу, не КАКОЙ именно вариант oneof при этом
/// строился, так что баг проходил тесты незамеченным. Теперь оба варианта
/// строятся из реально накопленных полей.
pub fn build_stage_execute(
    decision: &NextStageDecision,
    state: &ExecutionState,
    destination_address: &str,
    stage_execution_id: String,
) -> Result<StageExecuteCommand, String> {
    let stage_name = stage_name_to_proto(&decision.stage_name)?;

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
        StageName::Delivery => {
            let route_id = state.route_id.clone().ok_or("Delivery без route_id в состоянии — Routing ещё не завершился успешно")?;
            let protocol = state.protocol.ok_or("Delivery без protocol в состоянии — Routing ещё не завершился успешно")?;
            StageExtension::Delivery(DeliveryExtension {
                route_id,
                protocol,
                route_version: state.route_version.clone().unwrap_or_default(),
            })
        }
        StageName::DeliveryReconciliation => {
            // triggering_outcome — "сегодня всегда SUBMISSION_OUTCOME_UNKNOWN"
            // (platform-contracts, DeliveryReconciliationExtension) — единственный
            // реальный триггер в текущей архитектуре (dlr_correlation TTL-истечение,
            // не решение самого Pipeline Engine), поэтому это константа, не поле
            // из ExecutionState — источник этого значения лежит вне сервиса
            // (Scheduler Background Lane решает, когда диспетчеризовать эту стадию).
            StageExtension::DeliveryReconciliation(DeliveryReconciliationExtension {
                triggering_outcome: Outcome::SubmissionOutcomeUnknown as i32,
                queue_msg_id: String::new(),
            })
        }
        StageName::Unspecified => return Err("StageName::Unspecified недопустим для диспетчеризации".to_string()),
    };

    Ok(StageExecuteCommand {
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
    })
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
        let command = build_stage_execute(&decision, &state, "998901331835", "se1".into()).unwrap();
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
        let command = build_stage_execute(&decision, &state, "998901331835", "se3".into()).unwrap();
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
    fn delivery_command_carries_real_route_from_routing_result_not_routing_extension() {
        // Прямая регрессия на находку кодревью: раньше здесь строился RoutingExtension.
        let pipeline = pipeline();
        let mut state = ExecutionState::new_from_incoming("m1".into(), &pipeline, 1);
        state.route_id = Some("beeline_smpp_primary".into());
        state.protocol = Some(1); // PROTOCOL_SMPP
        state.route_version = Some("3".into());
        let decision = NextStageDecision { node_id: "n5_delivery".into(), stage_name: "DELIVERY".into() };
        let command = build_stage_execute(&decision, &state, "998901331835", "se5".into()).unwrap();
        match command.stage_extension {
            Some(StageExtension::Delivery(ext)) => {
                assert_eq!(ext.route_id, "beeline_smpp_primary");
                assert_eq!(ext.protocol, 1);
                assert_eq!(ext.route_version, "3");
            }
            other => panic!("ожидали DeliveryExtension, получили {other:?}"),
        }
    }

    #[test]
    fn delivery_command_without_route_in_state_is_rejected_not_built_with_empty_fields() {
        let pipeline = pipeline();
        let state = ExecutionState::new_from_incoming("m1".into(), &pipeline, 1); // route_id/protocol всё ещё None
        let decision = NextStageDecision { node_id: "n5_delivery".into(), stage_name: "DELIVERY".into() };
        assert!(build_stage_execute(&decision, &state, "998901331835", "se5".into()).is_err());
    }

    #[test]
    fn delivery_reconciliation_command_carries_delivery_reconciliation_extension() {
        let pipeline = pipeline();
        let state = ExecutionState::new_from_incoming("m1".into(), &pipeline, 1);
        let decision = NextStageDecision { node_id: "n6_reconciliation".into(), stage_name: "DELIVERY_RECONCILIATION".into() };
        let command = build_stage_execute(&decision, &state, "998901331835", "se6".into()).unwrap();
        match command.stage_extension {
            Some(StageExtension::DeliveryReconciliation(ext)) => {
                assert_eq!(ext.triggering_outcome, Outcome::SubmissionOutcomeUnknown as i32);
            }
            other => panic!("ожидали DeliveryReconciliationExtension, получили {other:?}"),
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
        let command = build_stage_execute(&decision, &state, "998901331835", "stable-id-123".into()).unwrap();
        assert_eq!(command.stage_execution_id, "stable-id-123");
    }
}
