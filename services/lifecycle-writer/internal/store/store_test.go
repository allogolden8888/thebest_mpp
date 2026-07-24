package store

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"mpp/lifecycle-writer/internal/core"
)

func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("LIFECYCLE_WRITER_TEST_DSN")
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

func uniqueMessageID(t *testing.T) string {
	return fmt.Sprintf("%08x-0000-0000-0000-000000000000", time.Now().UnixNano()&0xFFFFFFFF)
}

func TestInsertReadModelThenUpdate(t *testing.T) {
	pool := testPool(t)
	defer pool.Close()
	s := New(pool)
	ctx := context.Background()
	messageID := uniqueMessageID(t)

	err := s.InsertReadModel(ctx, core.ReadModelRow{
		MessageID: messageID, PartnerID: "acme", ApplicationID: "acme_main", TraceID: uniqueMessageID(t),
		CurrentStatus: "RECEIVED", Timestamp: time.Now(),
	})
	if err != nil {
		t.Fatalf("InsertReadModel failed: %v", err)
	}

	err = s.UpdateReadModel(ctx, core.ReadModelUpdate{
		MessageID: messageID, CurrentStatus: "DELIVERED", Terminal: true, UpdatedAt: time.Now(),
	})
	if err != nil {
		t.Fatalf("UpdateReadModel failed: %v", err)
	}

	var status string
	var terminal bool
	err = pool.QueryRow(ctx, `SELECT current_status, terminal FROM messaging.message_read_model WHERE message_id = $1`, messageID).
		Scan(&status, &terminal)
	if err != nil {
		t.Fatalf("readback failed: %v", err)
	}
	if status != "DELIVERED" || !terminal {
		t.Fatalf("update не применился: status=%s terminal=%v", status, terminal)
	}
}

func TestInsertReadModelIsIdempotent(t *testing.T) {
	pool := testPool(t)
	defer pool.Close()
	s := New(pool)
	ctx := context.Background()
	messageID := uniqueMessageID(t)

	row := core.ReadModelRow{MessageID: messageID, PartnerID: "acme", ApplicationID: "app", TraceID: uniqueMessageID(t), CurrentStatus: "RECEIVED", Timestamp: time.Now()}
	if err := s.InsertReadModel(ctx, row); err != nil {
		t.Fatalf("first insert failed: %v", err)
	}
	if err := s.InsertReadModel(ctx, row); err != nil {
		t.Fatalf("second insert (idempotent) failed: %v", err)
	}

	var count int
	pool.QueryRow(ctx, `SELECT count(*) FROM messaging.message_read_model WHERE message_id = $1`, messageID).Scan(&count)
	if count != 1 {
		t.Fatalf("ожидали ровно 1 строку, получили %d", count)
	}
}

func TestBatchInsertLifecycleHistory(t *testing.T) {
	pool := testPool(t)
	defer pool.Close()
	s := New(pool)
	ctx := context.Background()
	messageID := uniqueMessageID(t)
	now := time.Now()

	rows := []core.LifecycleHistoryRow{
		{MessageID: messageID, LifecycleVersion: 1, Status: "SUBMITTED", EventID: uniqueMessageID(t), OccurredAt: now, Source: "message.lifecycle"},
		{MessageID: messageID, LifecycleVersion: 2, Status: "DELIVERED", EventID: uniqueMessageID(t), OccurredAt: now.Add(time.Second), Source: "message.lifecycle"},
	}
	if err := s.BatchInsertLifecycleHistory(ctx, rows); err != nil {
		t.Fatalf("BatchInsertLifecycleHistory failed: %v", err)
	}

	var count int
	pool.QueryRow(ctx, `SELECT count(*) FROM messaging.message_lifecycle_history WHERE message_id = $1`, messageID).Scan(&count)
	if count != 2 {
		t.Fatalf("ожидали 2 строки истории, получили %d", count)
	}
}

func TestBatchInsertDlq(t *testing.T) {
	pool := testPool(t)
	defer pool.Close()
	s := New(pool)
	ctx := context.Background()
	stageExecutionID := uniqueMessageID(t)

	rows := []core.DlqRow{
		{StageExecutionID: stageExecutionID, MessageID: uniqueMessageID(t), StageName: "BILLING", Attempt: 3,
			OriginalCommand: []byte{0x01, 0x02}, ReasonCode: "RETRY_EXHAUSTED", CreatedAt: time.Now()},
	}
	if err := s.BatchInsertDlq(ctx, rows); err != nil {
		t.Fatalf("BatchInsertDlq failed: %v", err)
	}

	var reasonCode string
	err := pool.QueryRow(ctx, `SELECT reason_code FROM messaging.dlq_record WHERE stage_execution_id = $1`, stageExecutionID).Scan(&reasonCode)
	if err != nil {
		t.Fatalf("readback failed: %v", err)
	}
	if reasonCode != "RETRY_EXHAUSTED" {
		t.Fatalf("неверный reason_code: %s", reasonCode)
	}
}