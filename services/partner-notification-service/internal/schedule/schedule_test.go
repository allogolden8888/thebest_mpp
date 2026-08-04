package schedule

import (
	"testing"
	"time"

	commonv1 "mpp/platformcontracts/common/v1"
)

func TestEvaluateTTLContinuesWithinWindow(t *testing.T) {
	occurredAt := time.Now().Add(-1 * time.Hour)
	decision := EvaluateTTL(occurredAt, 24*time.Hour, time.Now())
	if decision != DecisionContinue {
		t.Fatalf("ожидали DecisionContinue, получили %v", decision)
	}
}

func TestEvaluateTTLExpiresAfterWindow(t *testing.T) {
	occurredAt := time.Now().Add(-25 * time.Hour)
	decision := EvaluateTTL(occurredAt, 24*time.Hour, time.Now())
	if decision != DecisionExpire {
		t.Fatalf("ожидали DecisionExpire, получили %v", decision)
	}
}

func TestEvaluateTTLBoundaryIsExclusive(t *testing.T) {
	now := time.Now()
	occurredAt := now.Add(-24 * time.Hour)
	decision := EvaluateTTL(occurredAt, 24*time.Hour, now)
	if decision != DecisionExpire {
		t.Fatalf("на границе окна (now == deadline) ожидали DecisionExpire, получили %v", decision)
	}
}

// TestNextRetryDelayGrowsWithAttemptAndStaysCapped — прямая регрессия на
// MEDIUM находку кодревью: раньше был фиксированный 30с интервал независимо
// от attempt. Full jitter означает результат всегда в [0, upperBound) —
// проверяем сам upperBound растёт с attempt и капается на maxBackoff, не
// точное значение (недетерминировано по построению).
func TestNextRetryDelayGrowsWithAttemptAndStaysCapped(t *testing.T) {
	base := 30 * time.Second
	max := 5 * time.Minute

	for i := 0; i < 200; i++ {
		if d := NextRetryDelay(1, base, max); d < 0 || d >= base {
			t.Fatalf("attempt=1: ожидали [0, %v), получили %v", base, d)
		}
	}
	for i := 0; i < 200; i++ {
		if d := NextRetryDelay(2, base, max); d < 0 || d >= 2*base {
			t.Fatalf("attempt=2: ожидали [0, %v), получили %v", 2*base, d)
		}
	}
	// attempt=10 -> base*2^9 = 15360с, далеко за maxBackoff — должен
	// закапиться на max, не расти неограниченно.
	for i := 0; i < 200; i++ {
		if d := NextRetryDelay(10, base, max); d < 0 || d >= max {
			t.Fatalf("attempt=10 должен быть закапан на maxBackoff=%v, получили %v", max, d)
		}
	}
}

func TestNextRetryDelayNeverNegativeForAnomalousAttempt(t *testing.T) {
	if d := NextRetryDelay(0, 30*time.Second, 5*time.Minute); d < 0 {
		t.Fatalf("attempt=0 (аномалия) не должен давать отрицательную задержку, получили %v", d)
	}
	if d := NextRetryDelay(-5, 30*time.Second, 5*time.Minute); d < 0 {
		t.Fatalf("отрицательный attempt не должен давать отрицательную задержку, получили %v", d)
	}
	if d := NextRetryDelay(1000000, 30*time.Second, 5*time.Minute); d < 0 || d >= 5*time.Minute {
		t.Fatalf("аномально большой attempt должен закапываться на maxBackoff без переполнения, получили %v", d)
	}
}

func TestBuildRetryTaskCarriesAttemptPassthroughNotIncremented(t *testing.T) {
	now := time.Now()
	task := BuildRetryTask("evt1", 3, now, now.Add(30*time.Second))
	if task.GetAttempt() != 3 {
		t.Errorf("Attempt = %d, want 3 (passthrough, не инкремент — см. комментарий)", task.GetAttempt())
	}
	if task.GetTaskType() != commonv1.BackgroundTaskType_BACKGROUND_TASK_TYPE_NOTIFICATION_RETRY {
		t.Errorf("TaskType = %v, want NOTIFICATION_RETRY", task.GetTaskType())
	}
	if task.GetTargetTopic() != "notification.retry" {
		t.Errorf("TargetTopic = %q, want notification.retry", task.GetTargetTopic())
	}
	if task.GetSourceEventId() != "evt1" {
		t.Errorf("SourceEventId = %q, want evt1", task.GetSourceEventId())
	}
}
