package store

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	chdriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

func testClickHouseConn(t *testing.T) chdriver.Conn {
	t.Helper()
	addr := os.Getenv("BACKOFFICE_API_TEST_CLICKHOUSE_ADDR")
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
	// Схема должна совпадать с analytics-writer/internal/store/store.go
	// (владелец таблицы) — ReplacingMergeTree/event_id, иначе тест `FINAL`
	// в Report() (см. clickhouse.go) проверял бы не то, что реально
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

func TestReportAggregatesAcrossPartnersWhenUnfiltered(t *testing.T) {
	conn := testClickHouseConn(t)
	s := NewClickHouseFromConn(conn)
	ctx := context.Background()

	hour := time.Now().Truncate(time.Hour)
	partnerA := "partner-a-" + time.Now().Format("20060102150405.000000000")
	partnerB := "partner-b-" + time.Now().Format("20060102150405.000000000")

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO analytics.stage_events (event_type, message_id, partner_id, stage_name, outcome, occurred_at)")
	if err != nil {
		t.Fatalf("prepare batch failed: %v", err)
	}
	for _, p := range []string{partnerA, partnerB} {
		if err := batch.Append("stage_completed", "msg-"+p, p, "BILLING", "SUCCEEDED", hour.Add(time.Minute)); err != nil {
			t.Fatalf("batch append failed: %v", err)
		}
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("batch send failed: %v", err)
	}

	resultsA, err := s.Report(ctx, ReportFilter{PartnerID: partnerA, From: hour, To: hour.Add(time.Hour)})
	if err != nil {
		t.Fatalf("Report failed: %v", err)
	}
	if len(resultsA) != 1 || resultsA[0].PartnerID != partnerA {
		t.Fatalf("фильтр по partner_id не сработал: %+v", resultsA)
	}
}
