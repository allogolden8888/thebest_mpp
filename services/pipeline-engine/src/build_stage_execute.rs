//! `build_stage_execute` (service_internal_methods.md §1.4) — строит
//! `StageExecuteCommand` для целевой стадии из аккумулированного
//! `ExecutionState`, включая нужный вариант `stage_extension` oneof.

use crate::execution_state::{ExecutionState, NextStageDecision};
use crate::proto::common::stage_execute_command::StageExtension;
use crate::proto::common::{
    BillingExtension, DeliveryExtension, DeliveryReconciliationExtension, DestinationResolutionExtension, Outcome,
    PolicyExtension, RoutingExtension, StageExecuteCommand, StageName,
};

/// `i64::MAX` (см. `ExecutionState::message_ttl_ms` — "нет TTL, никогда не
/// истекает") намеренно НЕ конвертируется в реальный `Timestamp` — секунды
/// переполнили бы разумный диапазон и не несут полезной информации;
/// `None` на wire для "без TTL" — то же самое, что уже означало отсутствие
/// поля до этой правки.
fn message_ttl_to_timestamp(message_ttl_ms: i64) -> Option<prost_types::Timestamp> {
    if message_ttl_ms == i64::MAX {
        return None;
    }
    Some(prost_types::Timestamp { seconds: message_ttl_ms / 1000, nanos: ((message_ttl_ms % 1000) * 1_000_000) as i32 })
}

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
            // Найдено при закрытии multi-tenancy-пробела в Billing Service
            // (Фаза 5a): добавлено полем 4 в BillingExtension, см. комментарий
            // в platform-contracts/common/stage_contract.proto. ExecutionState
            // уже накапливает partner_id с handle_incoming — только не
            // прокидывал дальше сюда, тот же класс находки, что
            // DeliveryExtension.resolved_operator_id выше.
            partner_id: state.partner_id.clone(),
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
                // Найдено при реализации delivery-service: DeliveryExtension
                // изначально не нёс resolved_operator_id вообще — добавлено
                // полем 4 в platform-contracts/common/stage_contract.proto,
                // см. комментарий там. ExecutionState уже накапливал это
                // значение с этапа DestinationResolution — только не
                // прокидывал дальше сюда.
                resolved_operator_id: state.resolved_operator_id.clone().unwrap_or_default(),
                // Партнёрское поле (SMPP priority_flag, 0-3), прокинутое от
                // приёма без изменений — см. ExecutionState.priority_flag.
                priority_flag: state.priority_flag,
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
        // Реальная находка: раньше это поле было хардкожено в None для
        // ЛЮБОЙ стадии, несмотря на то, что ExecutionState с этой правки
        // реально несёт message_ttl_ms — теперь заполняется для всех стадий
        // одинаково, не только для Delivery (retry-until-expiry читает его
        // именно отсюда через StageCompletedEvent -> ExecutionState, не
        // напрямую из этого поля, но wire-контракт должен быть честным).
        message_ttl: message_ttl_to_timestamp(state.message_ttl_ms),
        config_versions: state.config_versions.clone(),
        traceparent: String::new(),
        payload_ref: None,
        // Фаза 11: верхнеуровневое поле, присваивается один раз независимо
        // от того, какая стадия строится — не размножается по match-arm'ам
        // выше (в отличие от BillingExtension.partner_id, Фаза 5a), потому
        // что sandbox нужен всем стадиям одинаково, не по-разному.
        sandbox: state.sandbox,
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
        let state = ExecutionState::new_from_incoming("m1".into(), &pipeline, 1, 2, i64::MAX, "acme".into(), false);
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
        let mut state = ExecutionState::new_from_incoming("m1".into(), &pipeline, 3, 2, i64::MAX, "acme".into(), false);
        state.resolved_operator_id = Some("beeline".into());
        state.category = Some("TRANSACTION".into());
        let decision = NextStageDecision { node_id: "n3_billing".into(), stage_name: "BILLING".into() };
        let command = build_stage_execute(&decision, &state, "998901331835", "se3".into()).unwrap();
        match command.stage_extension {
            Some(StageExtension::Billing(ext)) => {
                assert_eq!(ext.resolved_operator_id, "beeline");
                assert_eq!(ext.category, "TRANSACTION");
                assert_eq!(ext.segment_count, 3, "segment_count посчитан один раз при кэшировании контекста, не перечитывается");
                assert_eq!(ext.partner_id, "acme", "partner_id обязан дойти до BillingExtension (Фаза 5a — multi-tenancy в Billing Service)");
            }
            other => panic!("ожидали BillingExtension, получили {other:?}"),
        }
    }

    /// Фаза 11: sandbox — верхнеуровневое поле StageExecuteCommand, не
    /// per-extension (в отличие от BillingExtension.partner_id) — должно
    /// доезжать одинаково для ЛЮБОЙ стадии, не только Billing/Delivery,
    /// которые реально на него реагируют. Проверяем на Routing именно
    /// потому, что RoutingExtension сам по себе sandbox не несёт вообще —
    /// это доказывает, что поле присваивается независимо от того, какой
    /// match-arm сработал.
    #[test]
    fn sandbox_reaches_top_level_command_regardless_of_stage() {
        let pipeline = pipeline();
        let mut state = ExecutionState::new_from_incoming("m1".into(), &pipeline, 1, 2, i64::MAX, "acme".into(), true);
        state.resolved_operator_id = Some("beeline".into());
        let decision = NextStageDecision { node_id: "n4_routing".into(), stage_name: "ROUTING".into() };
        let command = build_stage_execute(&decision, &state, "998901331835", "se4".into()).unwrap();
        assert!(command.sandbox, "sandbox=true в ExecutionState обязан попасть в StageExecuteCommand.sandbox даже для Routing");
    }

    #[test]
    fn delivery_command_carries_real_route_from_routing_result_not_routing_extension() {
        // Прямая регрессия на находку кодревью: раньше здесь строился RoutingExtension.
        let pipeline = pipeline();
        let mut state = ExecutionState::new_from_incoming("m1".into(), &pipeline, 1, 2, i64::MAX, "acme".into(), false);
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
    fn delivery_command_carries_resolved_operator_id_accumulated_since_destination_resolution() {
        // Найдено при реализации delivery-service: DeliveryExtension изначально
        // не нёс resolved_operator_id вообще (platform-contracts/common/
        // stage_contract.proto field 4, добавлено вместе с этим тестом) —
        // Delivery не может резолвить Operator Route Registry
        // (operator_route:{operator_id}:{route_id}) без него.
        let pipeline = pipeline();
        let mut state = ExecutionState::new_from_incoming("m1".into(), &pipeline, 1, 2, i64::MAX, "acme".into(), false);
        state.resolved_operator_id = Some("beeline".into());
        state.route_id = Some("beeline_smpp_primary".into());
        state.protocol = Some(1);
        let decision = NextStageDecision { node_id: "n5_delivery".into(), stage_name: "DELIVERY".into() };
        let command = build_stage_execute(&decision, &state, "998901331835", "se5".into()).unwrap();
        match command.stage_extension {
            Some(StageExtension::Delivery(ext)) => assert_eq!(ext.resolved_operator_id, "beeline"),
            other => panic!("ожидали DeliveryExtension, получили {other:?}"),
        }
    }

    #[test]
    fn delivery_command_carries_priority_flag_and_message_ttl_from_state() {
        // Партнёрское поле (SMPP priority_flag) и message_ttl прокинуты через
        // ExecutionState без изменений — не выводятся из category, как
        // resolved_operator_id/route_id выше.
        let pipeline = pipeline();
        let mut state = ExecutionState::new_from_incoming("m1".into(), &pipeline, 1, 3, 1_700_000_000_000, "acme".into(), false);
        state.route_id = Some("beeline_smpp_primary".into());
        state.protocol = Some(1);
        let decision = NextStageDecision { node_id: "n5_delivery".into(), stage_name: "DELIVERY".into() };
        let command = build_stage_execute(&decision, &state, "998901331835", "se5".into()).unwrap();
        match command.stage_extension {
            Some(StageExtension::Delivery(ext)) => assert_eq!(ext.priority_flag, 3),
            other => panic!("ожидали DeliveryExtension, получили {other:?}"),
        }
        let ttl = command.message_ttl.expect("message_ttl должен быть заполнен, не None");
        assert_eq!(ttl.seconds, 1_700_000_000);
    }

    #[test]
    fn delivery_command_without_route_in_state_is_rejected_not_built_with_empty_fields() {
        let pipeline = pipeline();
        let state = ExecutionState::new_from_incoming("m1".into(), &pipeline, 1, 2, i64::MAX, "acme".into(), false); // route_id/protocol всё ещё None
        let decision = NextStageDecision { node_id: "n5_delivery".into(), stage_name: "DELIVERY".into() };
        assert!(build_stage_execute(&decision, &state, "998901331835", "se5".into()).is_err());
    }

    #[test]
    fn delivery_reconciliation_command_carries_delivery_reconciliation_extension() {
        let pipeline = pipeline();
        let state = ExecutionState::new_from_incoming("m1".into(), &pipeline, 1, 2, i64::MAX, "acme".into(), false);
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
        let state = ExecutionState::new_from_incoming("m1".into(), &pipeline, 1, 2, i64::MAX, "acme".into(), false);
        let decision = NextStageDecision { node_id: "n1_destination_resolution".into(), stage_name: "DESTINATION_RESOLUTION".into() };
        let command = build_stage_execute(&decision, &state, "998901331835", "stable-id-123".into()).unwrap();
        assert_eq!(command.stage_execution_id, "stable-id-123");
    }
}
