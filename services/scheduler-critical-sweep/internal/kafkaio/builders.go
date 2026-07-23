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

// BuildRetryCommand — publish_retry: тот же stage_execution_id, attempt+1,
// новый deadline. **Известное ограничение этого среза**: exec:{message_id}
// не хранит payload_ref/stage_extension исходной команды (см.
// data_infrastructure_spec.md §2.1 — HASH exec:{message_id} перечисляет
// только pipeline_version/node_id/stage_execution_id/current_state/attempt/
// deadline/last_applied_event_id), поэтому здесь собирается частичная
// StageExecuteCommand — без oneof stage_extension и без payload_ref.
// Стадия-потребитель должна уметь дочитать message context самостоятельно
// (msgctx:{message_id}), либо схема Runtime Redis должна быть расширена —
// см. README.md "Открытый вопрос".
func BuildRetryCommand(state sweep.ExecutionState, eventID string, newDeadline time.Time, traceparent string) *commonv1.StageExecuteCommand {
	return &commonv1.StageExecuteCommand{
		EventId:          eventID,
		MessageId:        state.MessageID,
		PipelineVersion:  state.PipelineVersion,
		NodeId:           state.NodeID,
		StageName:        StageNameFromString(state.StageName),
		StageExecutionId: state.StageExecutionID,
		Attempt:          state.Attempt + 1,
		Deadline:         timestamppb.New(newDeadline),
		Traceparent:      traceparent,
	}
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

// BuildDlqRecord — publish_dlq: попытки исчерпаны (RETRY_EXHAUSTED).
// original_command — тот же частичный BuildRetryCommand (см. её докстринг
// про ограничение payload_ref/stage_extension).
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
