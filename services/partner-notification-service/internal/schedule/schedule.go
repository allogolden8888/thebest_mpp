// Package schedule — handle_delivery_failure/evaluate_ttl
// (service_internal_methods.md §7.1). Чистые функции, тестируются без сети.
package schedule

import (
	"math/rand/v2"
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
// NextRetryDelay — handle_delivery_failure backoff. MEDIUM находка кодревью:
// раньше был фиксированный 30с интервал без backoff/jitter/circuit-breaker
// (cmd/partner-notification-service/main.go) — каждая неудачная доставка
// ретраилась каждые 30с вплоть до ~2880 попыток за 24ч TTL, амплифицируя
// нагрузку на уже деградировавший партнёрский endpoint вместо отступления.
// Full jitter (AWS Architecture Blog, "Exponential Backoff And Jitter") —
// каждая попытка сама выбирает случайную точку в
// [0, min(maxBackoff, base*2^(attempt-1))], а не синхронно удваивает
// нагрузку от ВСЕХ отложенных сообщений сразу, как сделал бы backoff без
// jitter.
func NextRetryDelay(attempt int32, base, maxBackoff time.Duration) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	shift := attempt - 1
	if shift > 30 { // защита от переполнения int64 при аномально большом attempt
		shift = 30
	}
	upperBound := base * time.Duration(int64(1)<<uint(shift))
	if upperBound <= 0 || upperBound > maxBackoff {
		upperBound = maxBackoff
	}
	if upperBound <= 0 {
		return 0
	}
	return time.Duration(rand.Int64N(int64(upperBound)))
}

func BuildRetryTask(lifecycleEventID string, attempt int32, now, dueAt time.Time) *eventsv1.SchedulerBackgroundTask {
	return &eventsv1.SchedulerBackgroundTask{
		TaskType:      commonv1.BackgroundTaskType_BACKGROUND_TASK_TYPE_NOTIFICATION_RETRY,
		SourceEventId: lifecycleEventID,
		Attempt:       attempt,
		DueAt:         timestamppb.New(dueAt),
		TargetTopic:   "notification.retry",
	}
}
