package store

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// testPool — реальный локальный PostgreSQL 17 (brew), migrations/V028__incident.sql
// уже применена в этой песочнице. Пропускается, если Postgres недоступен —
// тот же обход, что в остальной сессии (iam-service/store_test.go).
func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("INCIDENT_SERVICE_TEST_DSN")
	if dsn == "" {
		dsn = "postgres://localhost:5432/mpp"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Skipf("не удалось создать пул подключений к Postgres (%v) — пропуск, БД недоступна в этой песочнице", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Skipf("Postgres недоступен на %q (%v) — пропуск", dsn, err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// openTestIncident — создаёт инцидент с уникальным title (uuid) и
// регистрирует cleanup, удаляющий его (и его заметки) по завершении теста.
func openTestIncident(t *testing.T, pool *pgxpool.Pool, pg *Postgres, severity string) Incident {
	t.Helper()
	title := "test-incident-" + uuid.NewString()
	inc, err := pg.OpenIncident(context.Background(), title, severity, "tester")
	if err != nil {
		t.Fatalf("OpenIncident: %v", err)
	}
	t.Cleanup(func() {
		ctx := context.Background()
		_, _ = pool.Exec(ctx, `DELETE FROM control.execution_control_audit WHERE incident_id = $1`, inc.ID)
		_, _ = pool.Exec(ctx, `DELETE FROM incident.incident_notes WHERE incident_id = $1`, inc.ID)
		_, _ = pool.Exec(ctx, `DELETE FROM incident.incidents WHERE id = $1`, inc.ID)
	})
	return inc
}

func TestOpenIncidentAndGetIncidentRoundTrip(t *testing.T) {
	pool := testPool(t)
	pg := NewPostgres(pool)
	inc := openTestIncident(t, pool, pg, "HIGH")

	if inc.Status != "OPEN" {
		t.Fatalf("ожидали status=OPEN сразу после открытия, получили %q", inc.Status)
	}
	if inc.OpenedAt.IsZero() {
		t.Fatalf("ожидали непустой opened_at (DEFAULT now())")
	}

	got, err := pg.GetIncident(context.Background(), inc.ID)
	if err != nil {
		t.Fatalf("GetIncident: %v", err)
	}
	if got.ID != inc.ID || got.Title != inc.Title || got.Severity != "HIGH" || got.Status != "OPEN" {
		t.Fatalf("неожиданный readback: %+v", got)
	}
	if got.ResolvedAt != nil || got.ResolvedBy != "" || got.PostmortemNotes != "" {
		t.Fatalf("свежеоткрытый инцидент не должен нести resolved_*/postmortem_notes: %+v", got)
	}
}

func TestGetIncidentUnknownReturnsErrIncidentNotFound(t *testing.T) {
	pool := testPool(t)
	pg := NewPostgres(pool)

	_, err := pg.GetIncident(context.Background(), -1)
	if err != ErrIncidentNotFound {
		t.Errorf("ожидали ErrIncidentNotFound, получили %v", err)
	}
}

func TestListIncidentsFiltersByStatus(t *testing.T) {
	pool := testPool(t)
	pg := NewPostgres(pool)
	ctx := context.Background()

	open := openTestIncident(t, pool, pg, "LOW")
	toResolve := openTestIncident(t, pool, pg, "MEDIUM")
	if _, err := pg.ResolveIncident(ctx, toResolve.ID, "resolver", "root cause found and fixed"); err != nil {
		t.Fatalf("ResolveIncident (setup): %v", err)
	}

	openList, err := pg.ListIncidents(ctx, "OPEN")
	if err != nil {
		t.Fatalf("ListIncidents(OPEN): %v", err)
	}
	foundOpen := false
	for _, inc := range openList {
		if inc.ID == toResolve.ID {
			t.Fatalf("RESOLVED инцидент не должен появляться в фильтре status=OPEN")
		}
		if inc.ID == open.ID {
			foundOpen = true
		}
	}
	if !foundOpen {
		t.Fatalf("ожидали найти открытый инцидент %d в фильтре status=OPEN", open.ID)
	}

	resolvedList, err := pg.ListIncidents(ctx, "RESOLVED")
	if err != nil {
		t.Fatalf("ListIncidents(RESOLVED): %v", err)
	}
	foundResolved := false
	for _, inc := range resolvedList {
		if inc.ID == toResolve.ID {
			foundResolved = true
		}
	}
	if !foundResolved {
		t.Fatalf("ожидали найти закрытый инцидент %d в фильтре status=RESOLVED", toResolve.ID)
	}

	all, err := pg.ListIncidents(ctx, "")
	if err != nil {
		t.Fatalf("ListIncidents(\"\"): %v", err)
	}
	found := 0
	for _, inc := range all {
		if inc.ID == open.ID || inc.ID == toResolve.ID {
			found++
		}
	}
	if found != 2 {
		t.Errorf("ожидали найти оба тестовых инцидента без фильтра, нашли %d из 2", found)
	}
}

func TestAddNoteRoundTripAndListNotes(t *testing.T) {
	pool := testPool(t)
	pg := NewPostgres(pool)
	ctx := context.Background()
	inc := openTestIncident(t, pool, pg, "CRITICAL")

	n1, err := pg.AddNote(ctx, inc.ID, "responder-1", "обнаружили деградацию, расследуем")
	if err != nil {
		t.Fatalf("AddNote (1): %v", err)
	}
	if n1.ID == 0 || n1.IncidentID != inc.ID {
		t.Fatalf("неожиданная заметка: %+v", n1)
	}
	time.Sleep(10 * time.Millisecond) // гарантированный порядок created_at для проверки ASC
	n2, err := pg.AddNote(ctx, inc.ID, "responder-2", "нашли причину, применяем override")
	if err != nil {
		t.Fatalf("AddNote (2): %v", err)
	}

	notes, err := pg.ListNotes(ctx, inc.ID)
	if err != nil {
		t.Fatalf("ListNotes: %v", err)
	}
	if len(notes) != 2 {
		t.Fatalf("ожидали 2 заметки, получили %d", len(notes))
	}
	if notes[0].ID != n1.ID || notes[1].ID != n2.ID {
		t.Fatalf("ожидали хронологический порядок (ASC по created_at): %+v", notes)
	}
}

func TestAddNoteUnknownIncidentReturnsErrIncidentNotFound(t *testing.T) {
	pool := testPool(t)
	pg := NewPostgres(pool)

	_, err := pg.AddNote(context.Background(), -1, "responder", "note")
	if err != ErrIncidentNotFound {
		t.Errorf("ожидали ErrIncidentNotFound, получили %v", err)
	}
}

func TestResolveIncidentSetsFieldsAndStatus(t *testing.T) {
	pool := testPool(t)
	pg := NewPostgres(pool)
	ctx := context.Background()
	inc := openTestIncident(t, pool, pg, "HIGH")

	resolved, err := pg.ResolveIncident(ctx, inc.ID, "resolver-1", "root cause: bad routing table entry, fixed via V023-style correction")
	if err != nil {
		t.Fatalf("ResolveIncident: %v", err)
	}
	if resolved.Status != "RESOLVED" {
		t.Fatalf("ожидали status=RESOLVED, получили %q", resolved.Status)
	}
	if resolved.ResolvedBy != "resolver-1" {
		t.Fatalf("ожидали resolved_by=resolver-1, получили %q", resolved.ResolvedBy)
	}
	if resolved.ResolvedAt == nil {
		t.Fatalf("ожидали непустой resolved_at")
	}
	if resolved.PostmortemNotes == "" {
		t.Fatalf("ожидали непустой postmortem_notes")
	}
}

func TestResolveIncidentAlreadyResolvedReturnsErrAlreadyResolved(t *testing.T) {
	pool := testPool(t)
	pg := NewPostgres(pool)
	ctx := context.Background()
	inc := openTestIncident(t, pool, pg, "LOW")

	if _, err := pg.ResolveIncident(ctx, inc.ID, "resolver-1", "fixed"); err != nil {
		t.Fatalf("ResolveIncident (1st): %v", err)
	}

	_, err := pg.ResolveIncident(ctx, inc.ID, "resolver-2", "fixed again")
	if err != ErrAlreadyResolved {
		t.Errorf("ожидали ErrAlreadyResolved на повторное закрытие, получили %v", err)
	}
}

func TestResolveIncidentUnknownReturnsErrIncidentNotFound(t *testing.T) {
	pool := testPool(t)
	pg := NewPostgres(pool)

	_, err := pg.ResolveIncident(context.Background(), -1, "resolver", "fixed")
	if err != ErrIncidentNotFound {
		t.Errorf("ожидали ErrIncidentNotFound, получили %v", err)
	}
}

// TestTimelineForIncidentFindsCrossSchemaAuditRows — доказывает, что
// TimelineForIncident реально читает control.execution_control_audit
// (владелец — execution-control-service, чужая схема), не просто
// компилируется: вставляем строку напрямую (как это делала бы
// execution-control-service.PersistOverrideAudit после ApplyOverride с
// incident_id в запросе, см. platform-contracts/grpc/internal_control.proto
// ApplyOverrideRequest.incident_id) и проверяем, что она находится по
// incident_id и в правильном хронологическом порядке.
func TestTimelineForIncidentFindsCrossSchemaAuditRows(t *testing.T) {
	pool := testPool(t)
	pg := NewPostgres(pool)
	ctx := context.Background()
	inc := openTestIncident(t, pool, pg, "CRITICAL")

	var firstID, secondID int64
	err := pool.QueryRow(ctx, `
		INSERT INTO control.execution_control_audit
			(scope, scope_id, state, admission_rate, reason, requested_by, incident_id)
		VALUES ('PARTNER_STAGE', 'acme:billing', 'PAUSED', 0.0, 'incident_test_first', 'incident-service-test', $1)
		RETURNING id`, inc.ID).Scan(&firstID)
	if err != nil {
		t.Fatalf("вставка первой override-строки: %v", err)
	}
	time.Sleep(10 * time.Millisecond)
	err = pool.QueryRow(ctx, `
		INSERT INTO control.execution_control_audit
			(scope, scope_id, state, admission_rate, reason, requested_by, incident_id)
		VALUES ('PARTNER_STAGE', 'acme:billing', 'ACTIVE', 1.0, 'override_cleared', 'incident-service-test', $1)
		RETURNING id`, inc.ID).Scan(&secondID)
	if err != nil {
		t.Fatalf("вставка второй override-строки: %v", err)
	}

	// Контрольная строка БЕЗ incident_id — не должна попасть в таймлайн
	// (доказывает, что фильтр реально по incident_id, не "все override
	// вообще").
	_, err = pool.Exec(ctx, `
		INSERT INTO control.execution_control_audit
			(scope, scope_id, state, admission_rate, reason, requested_by)
		VALUES ('GLOBAL', '', 'ACTIVE', 1.0, 'unrelated_override', 'someone-else')`)
	if err != nil {
		t.Fatalf("вставка контрольной override-строки: %v", err)
	}

	timeline, err := pg.TimelineForIncident(ctx, inc.ID)
	if err != nil {
		t.Fatalf("TimelineForIncident: %v", err)
	}
	if len(timeline) != 2 {
		t.Fatalf("ожидали ровно 2 записи таймлайна для инцидента %d, получили %d: %+v", inc.ID, len(timeline), timeline)
	}
	if timeline[0].ID != firstID || timeline[1].ID != secondID {
		t.Fatalf("ожидали хронологический порядок (created_at ASC): %+v", timeline)
	}
	if timeline[0].Reason != "incident_test_first" || timeline[0].Scope != "PARTNER_STAGE" || timeline[0].ScopeID != "acme:billing" {
		t.Fatalf("неожиданное содержимое первой записи: %+v", timeline[0])
	}
	if timeline[1].State != "ACTIVE" || timeline[1].Reason != "override_cleared" {
		t.Fatalf("неожиданное содержимое второй записи: %+v", timeline[1])
	}
}
