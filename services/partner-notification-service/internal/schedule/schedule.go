// Package schedule — handle_delivery_failure/evaluate_ttl
// (service_internal_methods.md §7.1). Чистые функции, тестируются без сети.
package schedule

import (
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "mpp/platformcontracts/common/v1"
	eventsv1 "mpp/platformcontracts/events/v1"
)

// Decision — evaluate_ttl: "Continue | Expire (без побочных эффектов,
// статус уже в lifecycle)" — на Expire ничего не публикуется, push
// прекращается молча, партнёр может получить терминальный статус через
// Partner API pull (HLD §19).
type Decision int

const (
	DecisionContinue Decision = iota
	DecisionExpire
)

func EvaluateTTL(occurredAt time.Time, notificationTTL time.Duration, now time.Time) Decision {
	if now.Before(occurredAt.Add(notificationTTL)) {
		return DecisionContinue
	}
	return DecisionExpire
}

// BuildRetryTask — handle_delivery_failure. `attempt` — 1 для первой
// неудачи; при повторном планировании после очередного wake-up с
// notification.retry — вызывающая сторона передаёт `attempt`, полученный
// в этом wake-up, без инкремента здесь (тот же принцип, что
// dlr-manager/internal/dlr/builders.go::BuildRetryTask — инкремент делает
// Scheduler Background Lane при dispatch,
// `DispatchBuilder.buildNotificationRetry`, уже реализовано Субагентом 1).
func BuildRetryTask(lifecycleEventID string, attempt int32, now, dueAt time.Time) *eventsv1.SchedulerBackgroundTask {
	return &eventsv1.SchedulerBackgroundTask{
		TaskType:      commonv1.BackgroundTaskType_BACKGROUND_TASK_TYPE_NOTIFICATION_RETRY,
		SourceEventId: lifecycleEventID,
		Attempt:       attempt,
		DueAt:         timestamppb.New(dueAt),
		TargetTopic:   "notification.retry",
	}
}
