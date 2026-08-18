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

// TestPersistOverrideAuditWithIncidentIDRoundTrips — luminous-hugging-charm.md
// Ф7: ApplyOverrideRequest.incident_id (internal_control.proto, поле 8)
// должен доходить до control.execution_control_audit.incident_id, чтобы
// incident-service.TimelineForIncident мог его найти (V028__incident.sql).
// Реальный round-trip против локального Postgres, не только компиляция
// AuditEntry.IncidentID.
func TestPersistOverrideAuditWithIncidentIDRoundTrips(t *testing.T) {
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
	// t.Cleanup, НЕ defer — t.Cleanup callbacks запускаются в LIFO-порядке
	// ПОСЛЕ того, как тело функции теста завершилось (в отличие от defer,
	// который выполнился бы раньше любого t.Cleanup, зарегистрированного
	// позже). Регистрируем закрытие пула ПЕРВЫМ, чтобы оно выполнилось
	// ПОСЛЕДНИМ — иначе cleanup удаления строк ниже пытался бы выполнить
	// pool.Exec на уже закрытом пуле и молча проглатывал ошибку (ровно тот
	// баг, из-за которого первая версия этого теста оставляла мусорные
	// строки в incident.incidents/control.execution_control_audit).
	t.Cleanup(func() { pool.Close() })

	if err := pool.Ping(ctx); err != nil {
		t.Skipf("Postgres недоступен на %q (%v) — пропуск", dsn, err)
	}

	// Реальный инцидент нужен только для получения правдоподобного id — нет
	// FK через границу схем (V028), поэтому execution_control_audit.incident_id
	// принял бы и несуществующий id, но вставляем настоящую строку в
	// incident.incidents, чтобы тест отражал реальный сценарий, а не
	// произвольное число.
	var incidentID int64
	err = pool.QueryRow(ctx, `
		INSERT INTO incident.incidents (title, severity, opened_by)
		VALUES ('audit incident_id round-trip test', 'HIGH', 'execution-control-service-test')
		RETURNING id`).Scan(&incidentID)
	if err != nil {
		t.Fatalf("setup: вставка incident.incidents: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM control.execution_control_audit WHERE incident_id = $1`, incidentID)
		_, _ = pool.Exec(context.Background(), `DELETE FROM incident.incidents WHERE id = $1`, incidentID)
	})

	audit := NewAuditStore(pool)
	id, _, err := audit.PersistOverrideAudit(ctx, AuditEntry{
		Scope:         "PARTNER_STAGE",
		ScopeID:       "acme:billing",
		State:         "PAUSED",
		AdmissionRate: 0.0,
		Reason:        "billing_freeze_with_incident_test",
		RequestedBy:   "execution-control-service-test",
		IncidentID:    &incidentID,
	})
	if err != nil {
		t.Fatalf("PersistOverrideAudit failed: %v", err)
	}

	var gotIncidentID *int64
	err = pool.QueryRow(ctx, `SELECT incident_id FROM control.execution_control_audit WHERE id = $1`, id).Scan(&gotIncidentID)
	if err != nil {
		t.Fatalf("readback failed: %v", err)
	}
	if gotIncidentID == nil || *gotIncidentID != incidentID {
		t.Fatalf("ожидали incident_id=%d, получили %v", incidentID, gotIncidentID)
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
