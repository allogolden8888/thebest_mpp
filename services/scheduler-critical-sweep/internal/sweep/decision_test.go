package sweep

import (
	"testing"
	"time"
)

type fakeSnapshot struct {
	paused map[string]bool
}

func (f fakeSnapshot) IsPaused(stageName string) bool {
	return f.paused[stageName]
}

func TestEvaluateRetryPolicyRetriesUnderMax(t *testing.T) {
	policy := RetryPolicy{MaxAttempts: 3}
	if got := EvaluateRetryPolicy(1, policy); got != DecisionRetry {
		t.Fatalf("attempt 1 < max 3 ожидали Retry, получили %v", got)
	}
}

func TestEvaluateRetryPolicyExhaustedAtMax(t *testing.T) {
	policy := RetryPolicy{MaxAttempts: 3}
	if got := EvaluateRetryPolicy(3, policy); got != DecisionExhausted {
		t.Fatalf("attempt 3 >= max 3 ожидали Exhausted, получили %v", got)
	}
}

func TestEvaluateRetryPolicyNotRetryableWhenMaxAttemptsZero(t *testing.T) {
	policy := RetryPolicy{MaxAttempts: 0}
	if got := EvaluateRetryPolicy(0, policy); got != DecisionNotRetryable {
		t.Fatalf("MaxAttempts=0 ожидали NotRetryable, получили %v", got)
	}
}

func TestCheckExecutionControlHoldsWhenStagePaused(t *testing.T) {
	snap := fakeSnapshot{paused: map[string]bool{"BILLING": true}}
	if got := CheckExecutionControl("BILLING", snap); got != DecisionHold {
		t.Fatalf("ожидали Hold для PAUSED стадии, получили %v", got)
	}
	if got := CheckExecutionControl("ROUTING", snap); got != DecisionProceed {
		t.Fatalf("ожидали Proceed для не-PAUSED стадии, получили %v", got)
	}
}

func TestCheckExecutionControlProceedsWithNilSnapshot(t *testing.T) {
	if got := CheckExecutionControl("BILLING", nil); got != DecisionProceed {
		t.Fatalf("nil snapshot (снапшот ещё не загружен) не должен блокировать sweep, получили %v", got)
	}
}

func TestDecideHoldsOnPausedStageRegardlessOfAttempt(t *testing.T) {
	snap := fakeSnapshot{paused: map[string]bool{"ROUTING": true}}
	state := ExecutionState{StageName: "ROUTING", Attempt: 5}
	policy := RetryPolicy{MaxAttempts: 3}
	if got := Decide(state, policy, snap); got != ActionHold {
		t.Fatalf("ожидали Hold — execution control проверяется раньше retry policy, получили %v", got)
	}
}

func TestDecideRetriesWhenAttemptsRemain(t *testing.T) {
	state := ExecutionState{StageName: "BILLING", Attempt: 1}
	policy := RetryPolicy{MaxAttempts: 3}
	if got := Decide(state, policy, nil); got != ActionRetry {
		t.Fatalf("ожидали Retry, получили %v", got)
	}
}

func TestDecideDlqsWhenAttemptsExhausted(t *testing.T) {
	state := ExecutionState{StageName: "BILLING", Attempt: 3}
	policy := RetryPolicy{MaxAttempts: 3}
	if got := Decide(state, policy, nil); got != ActionDlq {
		t.Fatalf("ожидали Dlq на исчерпанных попытках, получили %v", got)
	}
}

func TestDecideTimeoutWhenStageNotConfiguredForRetry(t *testing.T) {
	state := ExecutionState{StageName: "DESTINATION_RESOLUTION", Attempt: 0}
	policy := RetryPolicy{MaxAttempts: 0}
	if got := Decide(state, policy, nil); got != ActionTimeout {
		t.Fatalf("ожидали Timeout (не Dlq) для стадии без ретраев, получили %v", got)
	}
}

func TestNextDeadlineUsesProvidedBackoff(t *testing.T) {
	now := time.Date(2026, 7, 23, 0, 0, 0, 0, time.UTC)
	policy := RetryPolicy{
		MaxAttempts: 3,
		Backoff: func(attempt int32, now time.Time) time.Time {
			return now.Add(time.Duration(attempt) * time.Minute)
		},
	}
	got := NextDeadline(2, policy, now)
	want := now.Add(2 * time.Minute)
	if !got.Equal(want) {
		t.Fatalf("ожидали %v, получили %v", want, got)
	}
}

func TestNextDeadlineFallsBackWithoutBackoffFunc(t *testing.T) {
	now := time.Date(2026, 7, 23, 0, 0, 0, 0, time.UTC)
	policy := RetryPolicy{MaxAttempts: 3}
	got := NextDeadline(2, policy, now)
	if !got.After(now) {
		t.Fatalf("фолбэк должен продвигать deadline вперёд от now, получили %v", got)
	}
}
