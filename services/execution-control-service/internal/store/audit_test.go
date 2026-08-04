package store

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"mpp/execution-control-service/internal/hysteresis"
)

// TestPersistOverrideAuditAgainstRealPostgres — реальная вставка в
// control.execution_control_audit на локальном PostgreSQL 17 (brew,
// migrations/V016__execution_control_audit.sql уже применена в этой
// песочнице, см. README.md сервиса). Пропускается, если локальный Postgres
// недоступен — тот же обход, что и в остальной сессии (Docker daemon
// недоступен, development_plan.md "Координация" п.5).
func TestPersistOverrideAuditAgainstRealPostgres(t *testing.T) {
	dsn := os.Getenv("EXECUTION_CONTROL_TEST_DSN")
	if dsn == "" {
		dsn = "postgres://localhost:5432/mpp"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Skipf("не удалось создать пул подключений к Postgres (%v) — пропуск, БД недоступна в этой песочнице", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		t.Skipf("Postgres недоступен на %q (%v) — пропуск", dsn, err)
	}

	audit := NewAuditStore(pool)
	id, createdAt, err := audit.PersistOverrideAudit(ctx, AuditEntry{
		Scope:         "PARTNER_STAGE",
		ScopeID:       "acme:billing",
		State:         "PAUSED",
		AdmissionRate: 0.0,
		Reason:        "billing_freeze_test",
		RequestedBy:   "execution-control-service-test",
	})
	if err != nil {
		t.Fatalf("PersistOverrideAudit failed: %v", err)
	}
	if id == 0 {
		t.Fatalf("ожидали ненулевой autoincrement id")
	}
	if createdAt.IsZero() {
		t.Fatalf("ожидали непустой created_at (DEFAULT now())")
	}

	var gotScope, gotState, gotReason string
	err = pool.QueryRow(ctx, `SELECT scope, state, reason FROM control.execution_control_audit WHERE id = $1`, id).
		Scan(&gotScope, &gotState, &gotReason)
	if err != nil {
		t.Fatalf("readback failed: %v", err)
	}
	if gotScope != "PARTNER_STAGE" || gotState != "PAUSED" || gotReason != "billing_freeze_test" {
		t.Fatalf("readback mismatch: scope=%s state=%s reason=%s", gotScope, gotState, gotReason)
	}
}

// TestScopeNameAndStateNameMatchCheckConstraints проверяет, что каждое
// значение hysteresis.Scope/State переводится в TEXT, разрешённый
// CHECK-ограничением V016 — рассинхронизация здесь означала бы, что
// PersistOverrideAudit будет молча падать в проде на каждый вызов.
func TestScopeNameAndStateNameMatchCheckConstraints(t *testing.T) {
	allowedScopes := map[string]bool{"GLOBAL": true, "STAGE": true, "PARTNER": true, "PARTNER_STAGE": true, "OPERATOR_ROUTE": true}
	allowedStates := map[string]bool{"ACTIVE": true, "DEGRADED": true, "PAUSED": true}

	scopes := []hysteresis.Scope{
		hysteresis.ScopeGlobal, hysteresis.ScopeStage, hysteresis.ScopePartner,
		hysteresis.ScopePartnerStage, hysteresis.ScopeOperatorRoute,
	}
	for _, s := range scopes {
		if name := ScopeName(s); !allowedScopes[name] {
			t.Fatalf("ScopeName(%v) = %q не входит в CHECK-ограничение V016", s, name)
		}
	}

	states := []hysteresis.State{hysteresis.StateActive, hysteresis.StateDegraded, hysteresis.StatePaused}
	for _, s := range states {
		if name := StateName(s); !allowedStates[name] {
			t.Fatalf("StateName(%v) = %q не входит в CHECK-ограничение V016", s, name)
		}
	}
}
