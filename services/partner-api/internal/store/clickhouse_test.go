package store

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	chdriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// testClickHouseConn — партнёр не владеет схемой ClickHouse (её создаёт
// analytics-writer, см. services/analytics-writer/internal/store/store.go
// EnsureSchema); здесь schema создаётся тем же DDL только для того, чтобы
// тест был самодостаточным и не зависел от порядка запуска сервисов.
func testClickHouseConn(t *testing.T) chdriver.Conn {
	t.Helper()
	addr := os.Getenv("PARTNER_API_TEST_CLICKHOUSE_ADDR")
	if addr == "" {
		addr = "127.0.0.1:9000"
	}
	conn, err := clickhouse.Open(&clickhouse.Options{Addr: []string{addr}, Auth: clickhouse.Auth{Database: "default"}})
	if err != nil {
		t.Skipf("не удалось создать ClickHouse клиент (%v) — пропуск", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := conn.Ping(ctx); err != nil {
		t.Skipf("ClickHouse недоступен на %q (%v) — пропуск", addr, err)
	}

	if err := conn.Exec(ctx, "CREATE DATABASE IF NOT EXISTS analytics"); err != nil {
		t.Fatalf("create database failed: %v", err)
	}
	// Схема должна совпадать с analytics-writer/internal/store/store.go —
	// ReplacingMergeTree/event_id (CODE_REVIEW.md dedup finding), иначе
	// FINAL в Report() (см. clickhouse.go) проверял бы не то, что реально
	// работает в проде.
	if err := conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS analytics.stage_events (
			event_type       String,
			event_id         String,
			message_id       String,
			partner_id       String,
			stage_name       String,
			outcome          String,
			reason_code      String,
			lifecycle_status String,
			occurred_at      DateTime64(3),
			ingested_at      DateTime64(3) DEFAULT now64(3)
		) ENGINE = ReplacingMergeTree()
		ORDER BY (occurred_at, event_id, message_id)
	`); err != nil {
		t.Fatalf("create table failed: %v", err)
	}
	return conn
}

func TestReportAggregatesByPartnerStageOutcome(t *testing.T) {
	conn := testClickHouseConn(t)
	s := NewClickHouseFromConn(conn)
	ctx := context.Background()

	partnerID := "acme-" + time.Now().Format("20060102150405.000000000")
	hour := time.Now().Truncate(time.Hour)

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO analytics.stage_events (event_type, message_id, partner_id, stage_name, outcome, occurred_at)")
	if err != nil {
		t.Fatalf("prepare batch failed: %v", err)
	}
	rows := []struct {
		messageID, stage, outcome string
		at                        time.Time
	}{
		{"msg-1", "BILLING", "SUCCEEDED", hour.Add(5 * time.Minute)},
		{"msg-2", "BILLING", "SUCCEEDED", hour.Add(10 * time.Minute)},
		{"msg-3", "BILLING", "FAILED", hour.Add(15 * time.Minute)},
		{"msg-4", "DELIVERY", "SUCCEEDED", hour.Add(20 * time.Minute)},
	}
	for _, r := range rows {
		if err := batch.Append("stage_completed", r.messageID, partnerID, r.stage, r.outcome, r.at); err != nil {
			t.Fatalf("batch append failed: %v", err)
		}
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("batch send failed: %v", err)
	}

	results, err := s.Report(ctx, ReportFilter{PartnerID: partnerID, From: hour, To: hour.Add(time.Hour)})
	if err != nil {
		t.Fatalf("Report failed: %v", err)
	}

	counts := map[string]uint64{}
	for _, r := range results {
		counts[r.StageName+"/"+r.Outcome] = r.EventCount
	}
	if counts["BILLING/SUCCEEDED"] != 2 || counts["BILLING/FAILED"] != 1 || counts["DELIVERY/SUCCEEDED"] != 1 {
		t.Fatalf("неверные агрегаты: %+v", counts)
	}
}

func TestReportFiltersByStageName(t *testing.T) {
	conn := testClickHouseConn(t)
	s := NewClickHouseFromConn(conn)
	ctx := context.Background()

	partnerID := "acme-filter-" + time.Now().Format("20060102150405.000000000")
	hour := time.Now().Truncate(time.Hour)

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO analytics.stage_events (event_type, message_id, partner_id, stage_name, outcome, occurred_at)")
	if err != nil {
		t.Fatalf("prepare batch failed: %v", err)
	}
	if err := batch.Append("stage_completed", "msg-a", partnerID, "BILLING", "SUCCEEDED", hour.Add(time.Minute)); err != nil {
		t.Fatalf("batch append failed: %v", err)
	}
	if err := batch.Append("stage_completed", "msg-b", partnerID, "ROUTING", "SUCCEEDED", hour.Add(time.Minute)); err != nil {
		t.Fatalf("batch append failed: %v", err)
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("batch send failed: %v", err)
	}

	results, err := s.Report(ctx, ReportFilter{PartnerID: partnerID, StageName: "ROUTING"})
	if err != nil {
		t.Fatalf("Report failed: %v", err)
	}
	if len(results) != 1 || results[0].StageName != "ROUTING" {
		t.Fatalf("неверная фильтрация по stage_name: %+v", results)
	}
}
