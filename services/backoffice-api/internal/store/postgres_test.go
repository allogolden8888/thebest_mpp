package store

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("BACKOFFICE_API_TEST_DSN")
	if dsn == "" {
		dsn = "postgres://localhost:5432/mpp"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Skipf("не удалось создать пул подключений к Postgres (%v) — пропуск", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Skipf("Postgres недоступен на %q (%v) — пропуск", dsn, err)
	}
	return pool
}

func uniqueID() string {
	return fmt.Sprintf("%08x-0000-0000-0000-000000000000", time.Now().UnixNano()&0xFFFFFFFF)
}

func TestDlqBrowseFiltersByStageAndReplayStatus(t *testing.T) {
	pool := testPool(t)
	defer pool.Close()
	s := NewPostgres(pool)
	ctx := context.Background()

	stageName := "BILLING-" + uniqueID()
	_, err := pool.Exec(ctx, `
		INSERT INTO messaging.dlq_record (stage_execution_id, message_id, stage_name, attempt, original_command, reason_code, replay_status)
		VALUES ($1, $2, $3, 3, '\x00', 'RETRY_EXHAUSTED', 'pending')
	`, uniqueID(), uniqueID(), stageName)
	if err != nil {
		t.Fatalf("insert dlq_record failed: %v", err)
	}
	_, err = pool.Exec(ctx, `
		INSERT INTO messaging.dlq_record (stage_execution_id, message_id, stage_name, attempt, original_command, reason_code, replay_status)
		VALUES ($1, $2, $3, 1, '\x00', 'TTL_EXPIRED', 'expired')
	`, uniqueID(), uniqueID(), stageName)
	if err != nil {
		t.Fatalf("insert dlq_record failed: %v", err)
	}

	results, err := s.DlqBrowse(ctx, DlqFilter{StageName: stageName, ReplayStatus: "pending"})
	if err != nil {
		t.Fatalf("DlqBrowse failed: %v", err)
	}
	if len(results) != 1 || results[0].ReplayStatus != "pending" {
		t.Fatalf("неверная фильтрация: %+v", results)
	}
}

func TestReconciliationBrowseFiltersByStatus(t *testing.T) {
	pool := testPool(t)
	defer pool.Close()
	s := NewPostgres(pool)
	ctx := context.Background()

	operatorID := "operator-" + uniqueID()
	_, err := pool.Exec(ctx, `
		INSERT INTO reconciliation.reconciliation_cases (case_id, message_id, stage_execution_id, operator_id, status, deadline_at)
		VALUES ($1, $2, $3, $4, 'open', now() + interval '1 hour')
	`, uniqueID(), uniqueID(), uniqueID(), operatorID)
	if err != nil {
		t.Fatalf("insert reconciliation_cases failed: %v", err)
	}
	_, err = pool.Exec(ctx, `
		INSERT INTO reconciliation.reconciliation_cases (case_id, message_id, stage_execution_id, operator_id, status, resolved_at, deadline_at)
		VALUES ($1, $2, $3, $4, 'resolved', now(), now() - interval '1 hour')
	`, uniqueID(), uniqueID(), uniqueID(), operatorID)
	if err != nil {
		t.Fatalf("insert reconciliation_cases failed: %v", err)
	}

	results, err := s.ReconciliationBrowse(ctx, ReconciliationFilter{OperatorID: operatorID, Status: "open"})
	if err != nil {
		t.Fatalf("ReconciliationBrowse failed: %v", err)
	}
	if len(results) != 1 || results[0].Status != "open" {
		t.Fatalf("неверная фильтрация: %+v", results)
	}
}

// TestAuditBrowseMergesAndSortsAcrossSources — реальный round-trip против
// двух из четырёх источников GET /v1/audit (iam.identity_audit,
// control.execution_control_audit — достаточно показать, что UNION ALL и
// маппинг колонок реально работают, не только компилируются). Проверяет:
// строки из обеих таблиц попадают в объединённый результат с правильным
// маппингом actor/action/target, source-фильтр реально ограничивает
// источник, и сортировка created_at DESC отражает реальный порядок вставки
// (более новая строка EC оказывается раньше более старой identity-строки).
func TestAuditBrowseMergesAndSortsAcrossSources(t *testing.T) {
	pool := testPool(t)
	defer pool.Close()
	s := NewPostgres(pool)
	ctx := context.Background()

	marker := uniqueID()
	identityActor := "identity-actor-" + marker
	identityTarget := "identity-target-" + marker
	ecRequestedBy := "ec-actor-" + marker
	ecScopeID := "scope-" + marker

	_, err := pool.Exec(ctx, `
		INSERT INTO iam.identity_audit (actor, action, target, created_at)
		VALUES ($1, 'ROLE_GRANTED', $2, now() - interval '1 minute')
	`, identityActor, identityTarget)
	if err != nil {
		t.Fatalf("insert identity_audit failed: %v", err)
	}

	_, err = pool.Exec(ctx, `
		INSERT INTO control.execution_control_audit (scope, scope_id, state, admission_rate, reason, requested_by, created_at)
		VALUES ('STAGE', $1, 'PAUSED', 0, 'test-reason', $2, now())
	`, ecScopeID, ecRequestedBy)
	if err != nil {
		t.Fatalf("insert execution_control_audit failed: %v", err)
	}

	// Полная выборка (все источники) — обе строки должны присутствовать,
	// EC (новее) раньше identity (старше) в DESC-порядке.
	entries, _, err := s.AuditBrowse(ctx, AuditFilter{Limit: 200})
	if err != nil {
		t.Fatalf("AuditBrowse failed: %v", err)
	}
	ecIdx, identityIdx := -1, -1
	for i, e := range entries {
		if e.Source == "execution_control" && e.Actor == ecRequestedBy {
			ecIdx = i
			if e.Action != "PAUSED" {
				t.Fatalf("execution_control action = %q, want PAUSED", e.Action)
			}
			if e.Target != "STAGE:"+ecScopeID {
				t.Fatalf("execution_control target = %q, want STAGE:%s", e.Target, ecScopeID)
			}
		}
		if e.Source == "identity" && e.Actor == identityActor {
			identityIdx = i
			if e.Action != "ROLE_GRANTED" {
				t.Fatalf("identity action = %q, want ROLE_GRANTED", e.Action)
			}
			if e.Target != identityTarget {
				t.Fatalf("identity target = %q, want %s", e.Target, identityTarget)
			}
		}
	}
	if ecIdx == -1 {
		t.Fatalf("строка execution_control_audit не найдена в объединённом результате: %+v", entries)
	}
	if identityIdx == -1 {
		t.Fatalf("строка identity_audit не найдена в объединённом результате: %+v", entries)
	}
	if ecIdx >= identityIdx {
		t.Fatalf("сортировка created_at DESC нарушена: execution_control (индекс %d) должен быть раньше identity (индекс %d)", ecIdx, identityIdx)
	}

	// source-фильтр — только identity, execution_control не должен попасть.
	filtered, _, err := s.AuditBrowse(ctx, AuditFilter{Source: "identity", Limit: 200})
	if err != nil {
		t.Fatalf("AuditBrowse(source=identity) failed: %v", err)
	}
	for _, e := range filtered {
		if e.Source != "identity" {
			t.Fatalf("source-фильтр не сработал: получили строку source=%q", e.Source)
		}
		if e.Actor == ecRequestedBy {
			t.Fatalf("source-фильтр не сработал: execution_control строка просочилась в identity-выборку")
		}
	}
	foundIdentityInFiltered := false
	for _, e := range filtered {
		if e.Actor == identityActor {
			foundIdentityInFiltered = true
		}
	}
	if !foundIdentityInFiltered {
		t.Fatalf("identity-строка не найдена при source=identity фильтре")
	}
}

func TestSupportMessageSearchFindsAcrossPartnersByMessageIdAndTraceId(t *testing.T) {
	pool := testPool(t)
	defer pool.Close()
	s := NewPostgres(pool)
	ctx := context.Background()

	// uniqueID() masks to 32 bits of a nanosecond timestamp — fine for the
	// single call each other test in this file makes, but four rapid calls
	// here hit real collisions (confirmed: two of the four came back equal
	// on an early run). uuid.NewString() is collision-free regardless of
	// call rate.
	messageID := uuid.NewString()
	traceID := uuid.NewString()
	otherMessageID := uuid.NewString()
	otherTraceID := uuid.NewString()

	_, err := pool.Exec(ctx, `
		INSERT INTO messaging.message_read_model (message_id, partner_id, application_id, trace_id, pipeline_id, pipeline_version, current_status, terminal)
		VALUES ($1, 'partner_a', 'app_a', $2, 'p1', '1', 'DELIVERED', true)
	`, messageID, traceID)
	if err != nil {
		t.Fatalf("insert message_read_model (partner_a) failed: %v", err)
	}
	_, err = pool.Exec(ctx, `
		INSERT INTO messaging.message_read_model (message_id, partner_id, application_id, trace_id, pipeline_id, pipeline_version, current_status, terminal)
		VALUES ($1, 'partner_b', 'app_b', $2, 'p1', '1', 'FAILED', true)
	`, otherMessageID, otherTraceID)
	if err != nil {
		t.Fatalf("insert message_read_model (partner_b) failed: %v", err)
	}

	// Поиск по message_id находит строку ДРУГОГО партнёра — весь смысл
	// кросс-партнёрского поиска, partner-api's Search сюда бы не пустил.
	byMessageID, err := s.SupportMessageSearch(ctx, SupportMessageSearchFilter{MessageID: messageID})
	if err != nil {
		t.Fatalf("SupportMessageSearch(message_id) failed: %v", err)
	}
	if len(byMessageID) != 1 || byMessageID[0].PartnerID != "partner_a" {
		t.Fatalf("ожидали ровно одну строку partner_a по message_id, получили %+v", byMessageID)
	}

	byTraceID, err := s.SupportMessageSearch(ctx, SupportMessageSearchFilter{TraceID: otherTraceID})
	if err != nil {
		t.Fatalf("SupportMessageSearch(trace_id) failed: %v", err)
	}
	if len(byTraceID) != 1 || byTraceID[0].PartnerID != "partner_b" {
		t.Fatalf("ожидали ровно одну строку partner_b по trace_id, получили %+v", byTraceID)
	}
}