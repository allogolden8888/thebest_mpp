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
