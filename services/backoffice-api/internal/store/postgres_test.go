package store

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

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