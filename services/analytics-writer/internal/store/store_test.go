package store

import (
	"context"
	"os"
	"testing"
	"time"

	"mpp/analytics-writer/internal/core"
)

func testStore(t *testing.T) *Store {
	t.Helper()
	addr := os.Getenv("ANALYTICS_WRITER_TEST_CLICKHOUSE_ADDR")
	if addr == "" {
		addr = "127.0.0.1:9000"
	}
	s, err := New(addr, "default", "default", "")
	if err != nil {
		t.Skipf("не удалось создать ClickHouse клиент (%v) — пропуск", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.EnsureSchema(ctx); err != nil {
		t.Skipf("ClickHouse недоступен на %q (%v) — пропуск", addr, err)
	}
	return s
}

func TestEnsureSchemaIsIdempotent(t *testing.T) {
	s := testStore(t)
	defer s.Close()
	ctx := context.Background()
	if err := s.EnsureSchema(ctx); err != nil {
		t.Fatalf("повторный EnsureSchema не должен падать: %v", err)
	}
}

func TestFlushBatchInsertsRealRows(t *testing.T) {
	s := testStore(t)
	defer s.Close()
	ctx := context.Background()

	messageID := "test-msg-" + time.Now().Format("20060102150405.000000000")
	records := []core.NormalizedRecord{
		{EventType: "stage_completed", MessageID: messageID, StageName: "BILLING", Outcome: "SUCCEEDED", OccurredAt: time.Now()},
		{EventType: "lifecycle", MessageID: messageID, LifecycleStatus: "DELIVERED", OccurredAt: time.Now()},
	}

	if err := s.FlushBatch(ctx, records); err != nil {
		t.Fatalf("FlushBatch failed: %v", err)
	}

	var count uint64
	row := s.conn.QueryRow(ctx, "SELECT count() FROM analytics.stage_events WHERE message_id = ?", messageID)
	if err := row.Scan(&count); err != nil {
		t.Fatalf("readback failed: %v", err)
	}
	if count != 2 {
		t.Fatalf("ожидали 2 строки, получили %d", count)
	}
}

func TestFlushBatchWithEmptySliceIsNoOp(t *testing.T) {
	s := testStore(t)
	defer s.Close()
	if err := s.FlushBatch(context.Background(), nil); err != nil {
		t.Fatalf("пустой batch не должен возвращать ошибку: %v", err)
	}
}