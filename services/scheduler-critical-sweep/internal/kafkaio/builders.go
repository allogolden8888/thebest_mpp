// Builders — publish_retry / publish_timeout_result / publish_dlq
// (service_internal_methods.md §2.1): чистые функции, собирают protobuf-
// сообщения из sweep.ExecutionState, без сети. Публикация — в publisher.go.
package kafkaio

import (
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "mpp/platformcontracts/common/v1"
	eventsv1 "mpp/platformcontracts/events/v1"

	"mpp/scheduler-critical-sweep/internal/sweep"
)

// buildPayloadRef — StagePayloadRef детерминированно строится из message_id:
// runtime_redis_key = "msgctx:" + message_id (data_infrastructure_spec.md
// §2.1, msgctx:{message_id}) — в отличие от stage_extension (см.
// buildStageExtension), это НЕ требует дополнительных данных из
// exec:{message_id}, поэтому всегда полностью заполняется, без условия "ok".
func buildPayloadRef(state sweep.ExecutionState) *commonv1.StagePayloadRef {
	return &commonv1.StagePayloadRef{
		MessageId:       state.MessageID,
		RuntimeRedisKey: "msgctx:" + state.MessageID,
	}
}

// buildStageExtension — восстанавливает oneof stage_extension из
// накопленных в ExecutionState результатов предыдущих стадий (см. докстринг
// полей в internal/sweep/types.go) и присваивает его cmd.StageExtension
// напрямую (oneof-поле сгенерированного protobuf-типа — неэкспортируемый
// интерфейс, конкретные *StageExecuteCommand_* обёртки им, конечно,
// удовлетворяют, но НАЗВАТЬ этот интерфейсный тип как тип возврата функции
// из пакета kafkaio нельзя — присваивание полю, а не возврат по значению).
// Возвращает ok=false, если данных, необходимых именно для этого
// stage_name, нет (сегодня это ожидаемый путь для всех стадий, пока
// Pipeline Engine не начнёт писать эти поля — см. README "Открытый
// вопрос") — вызывающая сторона (main.go) не должна публиковать retry без
// extension (CODE_REVIEW.md finding #1: republish без stage_extension
// гарантированно REJECTED на стороне стадии-потребителя).
func buildStageExtension(cmd *commonv1.StageExecuteCommand, state sweep.ExecutionState) (ok bool) {
	switch StageNameFromString(state.StageName) {
	case commonv1.StageName_STAGE_NAME_DESTINATION_RESOLUTION:
		if state.DestinationAddress == "" {
			return false
		}
		cmd.StageExtension = &commonv1.StageExecuteCommand_DestinationResolution{
			DestinationResolution: &commonv1.DestinationResolutionExtension{
				DestinationAddress: state.DestinationAddress,
			},
		}
		return true

	case commonv1.StageName_STAGE_NAME_POLICY:
		if state.ResolvedOperatorID == "" {
			return false
		}
		cmd.StageExtension = &commonv1.StageExecuteCommand_Policy{
			Policy: &commonv1.PolicyExtension{ResolvedOperatorId: state.ResolvedOperatorID},
		}
		return true

	case commonv1.StageName_STAGE_NAME_BILLING:
		if state.ResolvedOperatorID == "" || state.Category == "" {
			return false
		}
		cmd.StageExtension = &commonv1.StageExecuteCommand_Billing{
			Billing: &commonv1.BillingExtension{
				ResolvedOperatorId: state.ResolvedOperatorID,
				SegmentCount:       state.SegmentCount,
				Category:           state.Category,
			},
		}
		return true

	case commonv1.StageName_STAGE_NAME_ROUTING:
		if state.ResolvedOperatorID == "" {
			return false
		}
		cmd.StageExtension = &commonv1.StageExecuteCommand_Routing{
			Routing: &commonv1.RoutingExtension{ResolvedOperatorId: state.ResolvedOperatorID},
		}
		return true

	case commonv1.StageName_STAGE_NAME_DELIVERY:
		if state.RouteID == "" {
			return false
		}
		cmd.StageExtension = &commonv1.StageExecuteCommand_Delivery{
			Delivery: &commonv1.DeliveryExtension{
				RouteId:      state.RouteID,
				Protocol:     commonv1.Protocol(state.Protocol),
				RouteVersion: state.RouteVersion,
			},
		}
		return true

	default:
		// DELIVERY_RECONCILIATION: DeliveryReconciliationExtension не входит
		// в накопленные поля ExecutionState вовсе (triggering_outcome/
		// queue_msg_id — не результат предыдущей стадии, а обстоятельства
		// самого reconciliation) — честно не восстанавливаем, ok=false.
		return false
	}
}

// BuildRetryCommand — publish_retry: тот же stage_execution_id, явный
// attempt (не всегда +1 — см. ниже), новый deadline, payload_ref всегда
// заполнен (buildPayloadRef), stage_extension — если данных достаточно
// (buildStageExtension). Второй возврат — ok: false означает "republish
// этой команды гарантированно будет REJECTED на стороне стадии-потребителя
// (нет stage_extension) — не публикуй, уходи в DLQ с честной причиной"
// (CODE_REVIEW.md finding #1, main.go::processTick).
//
// attempt передаётся явно, а не вычисляется как state.Attempt+1 внутри —
// finding #2: раньше эта же функция переиспользовалась для original_command
// в DlqRecord и всегда прибавляла 1, из-за чего в DLQ попадал номер попытки
// на единицу больше реально исчерпанной. Теперь вызывающая сторона
// (main.go) явно решает: attempt+1 для настоящего retry, state.Attempt для
// DLQ-снимка исчерпанной попытки.
func BuildRetryCommand(state sweep.ExecutionState, eventID string, attempt int32, newDeadline time.Time, traceparent string) (*commonv1.StageExecuteCommand, bool) {
	cmd := &commonv1.StageExecuteCommand{
		EventId:          eventID,
		MessageId:        state.MessageID,
		PipelineVersion:  state.PipelineVersion,
		NodeId:           state.NodeID,
		StageName:        StageNameFromString(state.StageName),
		StageExecutionId: state.StageExecutionID,
		Attempt:          attempt,
		Deadline:         timestamppb.New(newDeadline),
		Traceparent:      traceparent,
		PayloadRef:       buildPayloadRef(state),
	}
	ok := buildStageExtension(cmd, state)
	return cmd, ok
}

// BuildTimeoutEvent — publish_timeout_result: stage.completed с
// outcome=TIMED_OUT, без retry (стадия не сконфигурирована на ретрай по
// таймауту, RetryPolicy.MaxAttempts=0).
func BuildTimeoutEvent(state sweep.ExecutionState, eventID string, now time.Time) *commonv1.StageCompletedEvent {
	return &commonv1.StageCompletedEvent{
		EventId:          eventID,
		MessageId:        state.MessageID,
		StageExecutionId: state.StageExecutionID,
		Attempt:          state.Attempt,
		StageName:        StageNameFromString(state.StageName),
		Outcome:          commonv1.Outcome_OUTCOME_TIMED_OUT,
		ReasonCode:       "STAGE_TIMEOUT",
		Retryable:        false,
		CompletedAt:      timestamppb.New(now),
	}
}

// BuildDlqRecord — publish_dlq: попытки исчерпаны (RETRY_EXHAUSTED), либо
// (main.go) состояние недостаточно для безопасного retry. original_command —
// снимок команды на исчерпанной/непригодной для retry попытке, собранный
// через BuildRetryCommand с attempt=state.Attempt (см. её докстринг про
// finding #2).
func BuildDlqRecord(state sweep.ExecutionState, originalCommand *commonv1.StageExecuteCommand, errorDetail string, now time.Time) *eventsv1.DlqRecord {
	return &eventsv1.DlqRecord{
		StageExecutionId: state.StageExecutionID,
		MessageId:        state.MessageID,
		StageName:        StageNameFromString(state.StageName),
		Attempt:          state.Attempt,
		OriginalCommand:  originalCommand,
		ReasonCode:       "RETRY_EXHAUSTED",
		ErrorDetail:      errorDetail,
		CreatedAt:        timestamppb.New(now),
	}
}
