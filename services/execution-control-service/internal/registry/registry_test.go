package registry

import (
	"testing"
	"time"

	"mpp/execution-control-service/internal/hysteresis"
)

var testThresholds = hysteresis.Thresholds{
	EnterDegraded:                0.5,
	ExitDegraded:                 0.3,
	EnterPaused:                  0.8,
	ExitPaused:                   0.5,
	EnterConfirmationWindowTicks: 3,
	ExitConfirmationWindowTicks:  5,
	MinStateDurationTicks:        5,
}

func TestEvaluateStartsActiveAtFullRate(t *testing.T) {
	r := New(testThresholds)
	key := ScopeKey{Scope: hysteresis.ScopeGlobal}
	eval := r.Evaluate(key, 0.1, time.Now())
	if eval.State != hysteresis.StateActive || eval.AdmissionRate != 1.0 {
		t.Fatalf("ожидали (ACTIVE, 1.0), получили (%v, %v)", eval.State, eval.AdmissionRate)
	}
}

func TestEvaluateDegradesUnderSustainedPressure(t *testing.T) {
	r := New(testThresholds)
	key := ScopeKey{Scope: hysteresis.ScopePartner, ScopeID: "acme"}
	now := time.Now()

	var last Evaluation
	for i := 0; i < 5; i++ {
		last = r.Evaluate(key, 0.6, now)
	}
	if last.State != hysteresis.StateDegraded {
		t.Fatalf("ожидали DEGRADED после устойчивой деградации, получили %v", last.State)
	}
	if last.AdmissionRate <= 0 || last.AdmissionRate >= 1.0 {
		t.Fatalf("ожидали сниженный, но не нулевой admission rate в DEGRADED, получили %v", last.AdmissionRate)
	}
}

func TestApplyOverrideTakesPrecedenceOverMetric(t *testing.T) {
	r := New(testThresholds)
	key := ScopeKey{Scope: hysteresis.ScopePartnerStage, ScopeID: "acme:billing"}
	now := time.Now()

	version, appliedAt := r.ApplyOverride(key, Override{
		State:         hysteresis.StatePaused,
		AdmissionRate: 0.0,
		Reason:        "billing_freeze",
		RequestedBy:   "billing-reconciliation",
	})
	if version != 1 {
		t.Fatalf("ожидали версию 1 после первого override, получили %d", version)
	}
	if appliedAt.IsZero() {
		t.Fatalf("appliedAt не должен быть нулевым")
	}

	// Метрика говорит "всё хорошо" (0.0), но override должен перевесить.
	eval := r.Evaluate(key, 0.0, now)
	if eval.State != hysteresis.StatePaused {
		t.Fatalf("override должен доминировать над метрикой, получили state=%v", eval.State)
	}
	if eval.AdmissionRate != 0.0 {
		t.Fatalf("ожидали admission_rate=0.0 под override, получили %v", eval.AdmissionRate)
	}
}

func TestOverrideExpiresAndFallsBackToMetric(t *testing.T) {
	r := New(testThresholds)
	key := ScopeKey{Scope: hysteresis.ScopeStage, ScopeID: "billing"}
	past := time.Now().Add(-time.Minute)

	r.ApplyOverride(key, Override{
		State:         hysteresis.StatePaused,
		AdmissionRate: 0.0,
		Reason:        "expired_override",
		RequestedBy:   "backoffice",
		ExpiresAt:     &past,
	})

	eval := r.Evaluate(key, 0.1, time.Now())
	if eval.State != hysteresis.StateActive {
		t.Fatalf("истёкший override не должен применяться, ожидали ACTIVE (metric-driven), получили %v", eval.State)
	}
}

func TestClearOverrideRestoresMetricDrivenEvaluation(t *testing.T) {
	r := New(testThresholds)
	key := ScopeKey{Scope: hysteresis.ScopeGlobal}
	now := time.Now()

	r.ApplyOverride(key, Override{State: hysteresis.StatePaused, AdmissionRate: 0.0, Reason: "manual", RequestedBy: "ops"})
	if eval := r.Evaluate(key, 0.1, now); eval.State != hysteresis.StatePaused {
		t.Fatalf("ожидали override PAUSED до ClearOverride, получили %v", eval.State)
	}

	version := r.ClearOverride(key)
	if version != 2 {
		t.Fatalf("ожидали версию 2 после clear (1=apply, 2=clear), получили %d", version)
	}

	eval := r.Evaluate(key, 0.1, now)
	if eval.State != hysteresis.StateActive {
		t.Fatalf("после ClearOverride ожидали metric-driven ACTIVE, получили %v", eval.State)
	}
}

func TestScopesAreIndependent(t *testing.T) {
	r := New(testThresholds)
	partnerKey := ScopeKey{Scope: hysteresis.ScopePartner, ScopeID: "acme"}
	globalKey := ScopeKey{Scope: hysteresis.ScopeGlobal}
	now := time.Now()

	r.ApplyOverride(partnerKey, Override{State: hysteresis.StatePaused, AdmissionRate: 0.0, Reason: "x", RequestedBy: "y"})

	if eval := r.Evaluate(globalKey, 0.1, now); eval.State != hysteresis.StateActive {
		t.Fatalf("override одного scope не должен влиять на другой, получили %v для global", eval.State)
	}
}
