package dlr

import (
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "mpp/platformcontracts/common/v1"
	eventsv1 "mpp/platformcontracts/events/v1"

	"mpp/dlr-manager/internal/correlation"
)

// BuildDeliveryStatusEvent — publish_delivery_status.
func BuildDeliveryStatusEvent(eventID string, rec *correlation.Record, dlrEvent *eventsv1.OperatorDlr, normalizedStatus string, now time.Time) *eventsv1.DeliveryStatusEvent {
	return &eventsv1.DeliveryStatusEvent{
		EventId:           eventID,
		MessageId:         rec.MessageID,
		OperatorId:        dlrEvent.GetOperatorId(),
		NormalizedStatus:  normalizedStatus,
		RawOperatorStatus: dlrEvent.GetRawStatus(),
		OccurredAt:        timestamppb.New(now),
	}
}

// BuildRetryTask — schedule_retry. `attempt` — 1 для самого первого
// планирования (свежий operator.dlr); при повторном планировании (после
// очередного wake-up с operator.dlr.unresolved, всё ещё NotFound) —
// вызывающая сторона передаёт `attempt`, полученный в этом wake-up, без
// дополнительного инкремента здесь: инкремент — забота Scheduler Background
// Lane при СЛЕДУЮЩЕМ dispatch (`DispatchBuilder.buildDlrRetryWakeup`,
// `attempt+1` — уже реализовано и закоммичено Субагентом 1); если бы обе
// стороны инкрементировали, счётчик обгонял бы реальное число попыток.
func BuildRetryTask(eventID string, attempt int32, receivedAt time.Time, correlationWindow time.Duration, backoff time.Duration, now time.Time) *eventsv1.SchedulerBackgroundTask {
	return &eventsv1.SchedulerBackgroundTask{
		TaskType:      commonv1.BackgroundTaskType_BACKGROUND_TASK_TYPE_DLR_CORRELATION_RETRY,
		SourceEventId: eventID,
		Attempt:       attempt,
		DueAt:         timestamppb.New(now.Add(backoff)),
		Deadline:      timestamppb.New(receivedAt.Add(correlationWindow)),
		TargetTopic:   "operator.dlr.unresolved",
	}
}
