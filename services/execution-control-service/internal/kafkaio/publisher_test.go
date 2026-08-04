package kafkaio

import (
	"testing"
	"time"

	commonv1 "mpp/platformcontracts/common/v1"

	"mpp/execution-control-service/internal/hysteresis"
	"mpp/execution-control-service/internal/registry"
)

func TestBuildControlRecordMapsScopeAndStateCorrectly(t *testing.T) {
	key := registry.ScopeKey{Scope: hysteresis.ScopePartnerStage, ScopeID: "acme:billing"}
	eval := registry.Evaluation{
		State:         hysteresis.StatePaused,
		AdmissionRate: 0.0,
		DispatchRate:  0.0,
		Reason:        "billing_freeze",
		Version:       3,
	}
	now := time.Date(2026, 7, 23, 10, 0, 0, 0, time.UTC)

	rec := BuildControlRecord(key, eval, now)

	if rec.GetScope() != commonv1.ExecutionControlScope_EXECUTION_CONTROL_SCOPE_PARTNER_STAGE {
		t.Fatalf("неверный scope: %v", rec.GetScope())
	}
	if rec.GetScopeId() != "acme:billing" {
		t.Fatalf("неверный scope_id: %v", rec.GetScopeId())
	}
	if rec.GetState() != commonv1.ExecutionControlState_EXECUTION_CONTROL_STATE_PAUSED {
		t.Fatalf("неверный state: %v", rec.GetState())
	}
	if rec.GetVersion() != 3 {
		t.Fatalf("неверная версия: %v", rec.GetVersion())
	}
	if !rec.GetCreatedAt().AsTime().Equal(now) {
		t.Fatalf("created_at не совпадает: %v", rec.GetCreatedAt().AsTime())
	}
	if rec.GetExpiresAt() != nil {
		t.Fatalf("ожидали пустой expires_at для не-override записи")
	}
}

func TestBuildControlRecordIncludesExpiresAtWhenPresent(t *testing.T) {
	expires := time.Date(2026, 7, 24, 0, 0, 0, 0, time.UTC)
	key := registry.ScopeKey{Scope: hysteresis.ScopeGlobal}
	eval := registry.Evaluation{State: hysteresis.StateActive, AdmissionRate: 1.0, ExpiresAt: &expires}

	rec := BuildControlRecord(key, eval, time.Now())
	if rec.GetExpiresAt() == nil {
		t.Fatalf("ожидали непустой expires_at")
	}
	if !rec.GetExpiresAt().AsTime().Equal(expires) {
		t.Fatalf("expires_at не совпадает: %v", rec.GetExpiresAt().AsTime())
	}
}

func TestRecordKeyDistinguishesScopesWithSameEmptyScopeId(t *testing.T) {
	global := RecordKey(registry.ScopeKey{Scope: hysteresis.ScopeGlobal, ScopeID: ""})
	stageEmpty := RecordKey(registry.ScopeKey{Scope: hysteresis.ScopeStage, ScopeID: ""})
	if string(global) == string(stageEmpty) {
		t.Fatalf("ключи GLOBAL и STAGE(scope_id=\"\") не должны совпадать на compaction: %q == %q", global, stageEmpty)
	}
}

func TestRecordKeyStableForSameScope(t *testing.T) {
	key := registry.ScopeKey{Scope: hysteresis.ScopePartner, ScopeID: "acme"}
	if string(RecordKey(key)) != string(RecordKey(key)) {
		t.Fatalf("ключ должен быть детерминированным для одного и того же scope")
	}
}
