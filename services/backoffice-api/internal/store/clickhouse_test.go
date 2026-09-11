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
	// Схема должна совпадать с pdu-log-writer/internal/store/store.go
	// (владелец таблицы) — иначе MessagePduLog() тестировал бы не ту форму
	// строки, что реально пишет продюсер.
	if err := conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS analytics.operator_pdu_log (
			operator_id        String,
			protocol           String,
			direction          String,
			pdu_type           String,
			sequence_number    Int32,
			message_id         String,
			stage_execution_id String,
			smsc_message_id    String,
			segment_id         Int32,
			status             String,
			occurred_at        DateTime64(3),
			ingested_at        DateTime64(3) DEFAULT now64(3)
		) ENGINE = ReplacingMergeTree()
		ORDER BY (occurred_at, operator_id, sequence_number, pdu_type)
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

// TestMessagePduLogCombinesA2PAndDlrByCorrelation — A2P-строка несёт
// message_id напрямую (submit_sm/_resp), DLR-строка — нет (deliver_sm/
// _resp, см. OperatorPduLog.message_id doc-комментарий), только
// smsc_message_id. handleMessagePduLog (pdulog.go) резолвит
// smsc_message_id через dlr.dlr_correlation и передаёт сюда — здесь
// проверяем, что MessagePduLog реально объединяет оба направления одной
// ленты через (message_id = ? OR smsc_message_id IN (...)), в
// хронологическом порядке.
func TestMessagePduLogCombinesA2PAndDlrByCorrelation(t *testing.T) {
	conn := testClickHouseConn(t)
	s := NewClickHouseFromConn(conn)
	ctx := context.Background()

	suffix := time.Now().Format("20060102150405.000000000")
	messageID := "msg-" + suffix
	smscMessageID := "smsc-" + suffix
	otherMessageID := "other-msg-" + suffix
	base := time.Now().Truncate(time.Millisecond)

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO analytics.operator_pdu_log "+
		"(operator_id, protocol, direction, pdu_type, sequence_number, message_id, stage_execution_id, smsc_message_id, segment_id, status, occurred_at)")
	if err != nil {
		t.Fatalf("prepare batch failed: %v", err)
	}
	rows := []struct {
		pduType       string
		seq           int32
		messageID     string
		smscMessageID string
		occurredAt    time.Time
	}{
		{"SUBMIT_SM", 1, messageID, "", base},
		{"SUBMIT_SM_RESP", 1, messageID, smscMessageID, base.Add(10 * time.Millisecond)},
		// DLR-направление: message_id пуст, единственная связь — smsc_message_id.
		{"DELIVER_SM", 7, "", smscMessageID, base.Add(time.Second)},
		{"DELIVER_SM_RESP", 7, "", smscMessageID, base.Add(time.Second + 10*time.Millisecond)},
		// Другое сообщение — не должно попасть в ленту.
		{"SUBMIT_SM", 2, otherMessageID, "", base},
	}
	for _, r := range rows {
		if err := batch.Append("op-1", "SMPP", "OUTBOUND", r.pduType, r.seq, r.messageID, "", r.smscMessageID, int32(0), "OK", r.occurredAt); err != nil {
			t.Fatalf("batch append failed: %v", err)
		}
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("batch send failed: %v", err)
	}

	entries, err := s.MessagePduLog(ctx, messageID, []string{smscMessageID})
	if err != nil {
		t.Fatalf("MessagePduLog failed: %v", err)
	}
	if len(entries) != 4 {
		t.Fatalf("ожидалось 4 PDU (2 A2P + 2 DLR), получено %d: %+v", len(entries), entries)
	}
	wantOrder := []string{"SUBMIT_SM", "SUBMIT_SM_RESP", "DELIVER_SM", "DELIVER_SM_RESP"}
	for i, want := range wantOrder {
		if entries[i].PduType != want {
			t.Fatalf("позиция %d: ожидался %s, получен %s (полная лента: %+v)", i, want, entries[i].PduType, entries)
		}
	}
}
