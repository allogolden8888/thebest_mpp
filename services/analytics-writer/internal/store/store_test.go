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
		{EventType: "stage_completed", EventID: "evt-" + messageID + "-billing", MessageID: messageID, StageName: "BILLING", Outcome: "SUCCEEDED", OccurredAt: time.Now()},
		{EventType: "lifecycle", EventID: "evt-" + messageID + "-lifecycle", MessageID: messageID, LifecycleStatus: "DELIVERED", OccurredAt: time.Now()},
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

// TestRedeliveredEventDoesNotDoubleCountAfterMerge — CODE_REVIEW.md
// finding: раньше at-least-once redelivery одного и того же события
// (нормальное поведение Kafka на rebalance/restart) вставляла вторую
// строку и молча раздувала count()-агрегаты. С ReplacingMergeTree по
// (occurred_at, event_id, message_id) повторная вставка ТОЙ ЖЕ строки
// (тот же event_id) в итоге схлопывается в 1 при мердже — здесь
// форсируем merge через OPTIMIZE ... FINAL, чтобы не зависеть от
// таймингов фонового мерджа, и проверяем результат.
func TestRedeliveredEventDoesNotDoubleCountAfterMerge(t *testing.T) {
	s := testStore(t)
	defer s.Close()
	ctx := context.Background()

	messageID := "test-redeliver-" + time.Now().Format("20060102150405.000000000")
	occurredAt := time.Now()
	record := core.NormalizedRecord{
		EventType: "stage_completed", EventID: "evt-" + messageID, MessageID: messageID,
		StageName: "BILLING", Outcome: "SUCCEEDED", OccurredAt: occurredAt,
	}

	// Первая доставка.
	if err := s.FlushBatch(ctx, []core.NormalizedRecord{record}); err != nil {
		t.Fatalf("первый FlushBatch failed: %v", err)
	}
	// Redelivery того же события (тот же event_id/occurred_at/message_id) —
	// например, consumer не успел закоммитить offset до рестарта.
	if err := s.FlushBatch(ctx, []core.NormalizedRecord{record}); err != nil {
		t.Fatalf("повторный FlushBatch (redelivery) failed: %v", err)
	}

	if err := s.conn.Exec(ctx, "OPTIMIZE TABLE analytics.stage_events FINAL"); err != nil {
		t.Fatalf("OPTIMIZE TABLE FINAL failed: %v", err)
	}

	var count uint64
	row := s.conn.QueryRow(ctx, "SELECT count() FROM analytics.stage_events FINAL WHERE message_id = ?", messageID)
	if err := row.Scan(&count); err != nil {
		t.Fatalf("readback failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("redelivery того же event_id не должна раздувать count — ожидали 1, получили %d", count)
	}
}

func TestFlushBatchWithEmptySliceIsNoOp(t *testing.T) {
	s := testStore(t)
	defer s.Close()
	if err := s.FlushBatch(context.Background(), nil); err != nil {
		t.Fatalf("пустой batch не должен возвращать ошибку: %v", err)
	}
}