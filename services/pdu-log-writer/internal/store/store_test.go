package store

import (
	"context"
	"os"
	"testing"
	"time"

	"mpp/pdu-log-writer/internal/core"
)

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func testStore(t *testing.T) *Store {
	t.Helper()
	addr := envOr("PDU_LOG_WRITER_TEST_CLICKHOUSE_ADDR", "127.0.0.1:9000")
	user := envOr("PDU_LOG_WRITER_TEST_CLICKHOUSE_USER", "default")
	password := envOr("PDU_LOG_WRITER_TEST_CLICKHOUSE_PASSWORD", "")
	s, err := New(addr, "default", user, password)
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
	records := []core.PduLogRecord{
		{OperatorID: "beeline_uz", Protocol: "SMPP", Direction: "A2P", PduType: "SUBMIT_SM",
			SequenceNumber: 1, MessageID: messageID, SegmentID: 1, OccurredAt: time.Now()},
		{OperatorID: "beeline_uz", Protocol: "SMPP", Direction: "A2P", PduType: "SUBMIT_SM_RESP",
			SequenceNumber: 1, MessageID: messageID, SegmentID: 1, Status: "OK",
			SmscMessageID: "dkr87lit9o7b", OccurredAt: time.Now()},
	}

	if err := s.FlushBatch(ctx, records); err != nil {
		t.Fatalf("FlushBatch failed: %v", err)
	}

	var count uint64
	row := s.conn.QueryRow(ctx, "SELECT count() FROM analytics.operator_pdu_log WHERE message_id = ?", messageID)
	if err := row.Scan(&count); err != nil {
		t.Fatalf("readback failed: %v", err)
	}
	if count != 2 {
		t.Fatalf("ожидали 2 строки, получили %d", count)
	}
}

func TestFlushBatchEmptyIsNoop(t *testing.T) {
	s := testStore(t)
	defer s.Close()
	if err := s.FlushBatch(context.Background(), nil); err != nil {
		t.Fatalf("пустой батч не должен падать: %v", err)
	}
}
