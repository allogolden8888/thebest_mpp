package store

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	commonv1 "mpp/platformcontracts/common/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("REPLAY_SERVICE_TEST_DSN")
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

func uniqueUUID() string {
	return fmt.Sprintf("%08x-0000-0000-0000-000000000000", time.Now().UnixNano()&0xFFFFFFFF)
}

func insertDlqRecord(t *testing.T, pool *pgxpool.Pool, stageExecutionID, messageID, stageName string, ttl time.Time) {
	t.Helper()
	cmd := &commonv1.StageExecuteCommand{MessageId: messageID, MessageTtl: timestamppb.New(ttl)}
	original, err := proto.Marshal(cmd)
	if err != nil {
		t.Fatalf("marshal original command failed: %v", err)
	}
	_, err = pool.Exec(context.Background(), `
		INSERT INTO messaging.dlq_record (stage_execution_id, message_id, stage_name, attempt, original_command, reason_code)
		VALUES ($1, $2, $3, 3, $4, 'RETRY_EXHAUSTED')
	`, stageExecutionID, messageID, stageName, original)
	if err != nil {
		t.Fatalf("insert test dlq_record failed: %v", err)
	}
}

func TestLoadDlqRecordParsesMessageTTLFromOriginalCommand(t *testing.T) {
	pool := testPool(t)
	defer pool.Close()
	s := New(pool)
	ctx := context.Background()

	stageExecutionID := uniqueUUID()
	ttl := time.Now().Add(2 * time.Hour).Truncate(time.Millisecond)
	insertDlqRecord(t, pool, stageExecutionID, uniqueUUID(), "BILLING", ttl)

	record, err := s.LoadDlqRecord(ctx, stageExecutionID)
	if err != nil {
		t.Fatalf("LoadDlqRecord failed: %v", err)
	}
	if record.StageName != "BILLING" || record.Attempt != 3 || record.ReasonCode != "RETRY_EXHAUSTED" {
		t.Fatalf("неверные поля: %+v", record)
	}
	if !record.MessageTTL.Equal(ttl) {
		t.Fatalf("message_ttl не совпадает: получили %v, ожидали %v", record.MessageTTL, ttl)
	}
	if record.ReplayStatus != "pending" {
		t.Fatalf("ожидали default replay_status=pending, получили %s", record.ReplayStatus)
	}
}

func TestChargeExistsInLedgerRealCheck(t *testing.T) {
	pool := testPool(t)
	defer pool.Close()
	s := New(pool)
	ctx := context.Background()

	chargeID := uniqueUUID()
	exists, err := s.ChargeExistsInLedger(ctx, chargeID)
	if err != nil {
		t.Fatalf("ChargeExistsInLedger failed: %v", err)
	}
	if exists {
		t.Fatalf("новый charge_id не должен существовать")
	}

	_, err = pool.Exec(ctx, `
		INSERT INTO billing.billing_ledger (charge_id, account_id, partner_id, amount, currency, entry_type)
		VALUES ($1, 'acc-1', 'acme', 100.0000, 'UZS', 'charge')
	`, chargeID)
	if err != nil {
		t.Fatalf("insert test ledger row failed: %v", err)
	}

	exists, err = s.ChargeExistsInLedger(ctx, chargeID)
	if err != nil {
		t.Fatalf("ChargeExistsInLedger (after insert) failed: %v", err)
	}
	if !exists {
		t.Fatalf("charge_id должен существовать после вставки")
	}
}

func TestMarkReplayedThenIdempotencyCheckIsUnsafe(t *testing.T) {
	pool := testPool(t)
	defer pool.Close()
	s := New(pool)
	ctx := context.Background()

	stageExecutionID := uniqueUUID()
	insertDlqRecord(t, pool, stageExecutionID, uniqueUUID(), "DELIVERY", time.Now().Add(time.Hour))

	if err := s.MarkReplayed(ctx, stageExecutionID); err != nil {
		t.Fatalf("MarkReplayed failed: %v", err)
	}

	record, err := s.LoadDlqRecord(ctx, stageExecutionID)
	if err != nil {
		t.Fatalf("LoadDlqRecord failed: %v", err)
	}
	if record.ReplayStatus != "replayed" {
		t.Fatalf("ожидали replay_status=replayed, получили %s", record.ReplayStatus)
	}
}

func TestWriteAuditInsertsRealRow(t *testing.T) {
	pool := testPool(t)
	defer pool.Close()
	s := New(pool)
	ctx := context.Background()

	stageExecutionID := uniqueUUID()
	err := s.WriteAudit(ctx, stageExecutionID, "ops@mpp", ChecksPassed{TTL: true, Idempotency: true, BillingSideEffect: true, DeliveryAmbiguity: true}, "REPUBLISHED", "stage.billing")
	if err != nil {
		t.Fatalf("WriteAudit failed: %v", err)
	}

	var outcome, targetTopic string
	err = pool.QueryRow(ctx, `SELECT outcome, target_topic FROM messaging.replay_audit WHERE stage_execution_id = $1`, stageExecutionID).
		Scan(&outcome, &targetTopic)
	if err != nil {
		t.Fatalf("readback failed: %v", err)
	}
	if outcome != "REPUBLISHED" || targetTopic != "stage.billing" {
		t.Fatalf("неверные поля: outcome=%s target_topic=%s", outcome, targetTopic)
	}
}