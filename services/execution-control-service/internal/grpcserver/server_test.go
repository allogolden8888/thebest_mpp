package grpcserver

import (
	"context"
	"testing"
	"time"

	"mpp/execution-control-service/internal/hysteresis"
	"mpp/execution-control-service/internal/registry"
	"mpp/execution-control-service/internal/store"

	commonv1 "mpp/platformcontracts/common/v1"
	grpcv1 "mpp/platformcontracts/grpc/v1"
)

// fakeAudit — in-memory замена store.AuditStore, чтобы тестировать
// gRPC-хендлер без реального Postgres (Postgres-путь уже отдельно
// протестирован в internal/store против реальной БД).
type fakeAudit struct {
	entries []store.AuditEntry
}

func (f *fakeAudit) PersistOverrideAudit(ctx context.Context, e store.AuditEntry) (int64, time.Time, error) {
	f.entries = append(f.entries, e)
	return int64(len(f.entries)), time.Now().UTC(), nil
}

func newTestServer() (*Server, *fakeAudit) {
	reg := registry.New(hysteresis.Thresholds{
		EnterDegraded: 0.5, ExitDegraded: 0.3, EnterPaused: 0.8, ExitPaused: 0.5,
		EnterConfirmationWindowTicks: 3, ExitConfirmationWindowTicks: 5, MinStateDurationTicks: 5,
	})
	audit := &fakeAudit{}
	return New(reg, audit), audit
}

func TestApplyOverrideRejectsMissingRequestedBy(t *testing.T) {
	srv, _ := newTestServer()
	_, err := srv.ApplyOverride(context.Background(), &grpcv1.ApplyOverrideRequest{
		Scope: commonv1.ExecutionControlScope_EXECUTION_CONTROL_SCOPE_PARTNER_STAGE,
	})
	if err == nil {
		t.Fatalf("ожидали ошибку при отсутствующем requested_by")
	}
}

func TestApplyOverridePersistsAuditAndReturnsVersion(t *testing.T) {
	srv, audit := newTestServer()
	resp, err := srv.ApplyOverride(context.Background(), &grpcv1.ApplyOverrideRequest{
		Scope:         commonv1.ExecutionControlScope_EXECUTION_CONTROL_SCOPE_PARTNER_STAGE,
		ScopeId:       "acme:billing",
		State:         commonv1.ExecutionControlState_EXECUTION_CONTROL_STATE_PAUSED,
		AdmissionRate: 0.0,
		Reason:        "billing_freeze",
		RequestedBy:   "billing-reconciliation",
	})
	if err != nil {
		t.Fatalf("ApplyOverride failed: %v", err)
	}
	if resp.GetVersion() != 1 {
		t.Fatalf("ожидали версию 1, получили %d", resp.GetVersion())
	}
	if resp.GetAppliedAt() == nil {
		t.Fatalf("ожидали непустой applied_at")
	}
	if len(audit.entries) != 1 {
		t.Fatalf("ожидали 1 запись аудита, получили %d", len(audit.entries))
	}
	if audit.entries[0].Scope != "PARTNER_STAGE" || audit.entries[0].State != "PAUSED" {
		t.Fatalf("неверная запись аудита: %+v", audit.entries[0])
	}
}

func TestClearOverrideBumpsVersionAndAuditsAsActive(t *testing.T) {
	srv, audit := newTestServer()
	ctx := context.Background()

	_, err := srv.ApplyOverride(ctx, &grpcv1.ApplyOverrideRequest{
		Scope: commonv1.ExecutionControlScope_EXECUTION_CONTROL_SCOPE_GLOBAL, State: commonv1.ExecutionControlState_EXECUTION_CONTROL_STATE_PAUSED,
		Reason: "x", RequestedBy: "ops",
	})
	if err != nil {
		t.Fatalf("ApplyOverride failed: %v", err)
	}

	resp, err := srv.ClearOverride(ctx, &grpcv1.ClearOverrideRequest{
		Scope: commonv1.ExecutionControlScope_EXECUTION_CONTROL_SCOPE_GLOBAL, RequestedBy: "ops",
	})
	if err != nil {
		t.Fatalf("ClearOverride failed: %v", err)
	}
	if resp.GetVersion() != 2 {
		t.Fatalf("ожидали версию 2 после clear, получили %d", resp.GetVersion())
	}
	if len(audit.entries) != 2 || audit.entries[1].State != "ACTIVE" || audit.entries[1].Reason != "override_cleared" {
		t.Fatalf("ожидали вторую запись аудита ACTIVE/override_cleared, получили %+v", audit.entries)
	}
}
