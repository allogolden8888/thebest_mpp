package store

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
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

// TestClaimForReplaySucceedsOnce — CODE_REVIEW.md CRITICAL finding #2:
// baseline single-claim behavior.
func TestClaimForReplaySucceedsOnce(t *testing.T) {
	pool := testPool(t)
	defer pool.Close()
	s := New(pool)
	ctx := context.Background()

	stageExecutionID := uniqueUUID()
	insertDlqRecord(t, pool, stageExecutionID, uniqueUUID(), "BILLING", time.Now().Add(time.Hour))

	record, claimed, err := s.ClaimForReplay(ctx, stageExecutionID)
	if err != nil {
		t.Fatalf("ClaimForReplay failed: %v", err)
	}
	if !claimed {
		t.Fatalf("ожидали claimed=true для pending записи")
	}
	if record.ReplayStatus != "in_progress" {
		t.Fatalf("ожидали replay_status=in_progress, получили %s", record.ReplayStatus)
	}

	// Повторный claim той же записи (ещё in_progress) должен провалиться.
	_, claimedAgain, err := s.ClaimForReplay(ctx, stageExecutionID)
	if err != nil {
		t.Fatalf("ClaimForReplay (второй раз) failed: %v", err)
	}
	if claimedAgain {
		t.Fatalf("повторный claim уже in_progress записи не должен проходить")
	}
}

// TestClaimForReplayConcurrentClaimsOnlyOneWins — CODE_REVIEW.md CRITICAL
// finding #2: реальный TOCTOU-регрессионный тест против настоящего
// PostgreSQL. Раньше (LoadDlqRecord + отдельный MarkReplayed после
// republish) 20 конкурентных вызовов для одной stage_execution_id все
// прошли бы check_idempotency и все республиковали бы — DELIVERY-стадийная
// запись ушла бы абоненту физически много раз. Атомарный
// UPDATE ... WHERE replay_status='pending' ... RETURNING гарантирует
// ровно одного победителя.
func TestClaimForReplayConcurrentClaimsOnlyOneWins(t *testing.T) {
	pool := testPool(t)
	defer pool.Close()
	s := New(pool)

	stageExecutionID := uniqueUUID()
	insertDlqRecord(t, pool, stageExecutionID, uniqueUUID(), "DELIVERY", time.Now().Add(time.Hour))

	const concurrency = 20
	var wg sync.WaitGroup
	var claimedCount int64
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, claimed, err := s.ClaimForReplay(context.Background(), stageExecutionID)
			if err != nil {
				t.Errorf("ClaimForReplay failed: %v", err)
				return
			}
			if claimed {
				atomic.AddInt64(&claimedCount, 1)
			}
		}()
	}
	wg.Wait()

	if claimedCount != 1 {
		t.Fatalf("ожидали ровно 1 успешный claim из %d конкурентных вызовов, получили %d", concurrency, claimedCount)
	}
}

// TestReleaseClaimReturnsToPending — billing/delivery-unsafe путь должен
// оставлять запись доступной для будущей попытки, не "успешно
// обработанной" и не зависшей в in_progress навсегда.
func TestReleaseClaimReturnsToPending(t *testing.T) {
	pool := testPool(t)
	defer pool.Close()
	s := New(pool)
	ctx := context.Background()

	stageExecutionID := uniqueUUID()
	insertDlqRecord(t, pool, stageExecutionID, uniqueUUID(), "BILLING", time.Now().Add(time.Hour))

	if _, claimed, err := s.ClaimForReplay(ctx, stageExecutionID); err != nil || !claimed {
		t.Fatalf("setup claim failed: claimed=%v err=%v", claimed, err)
	}
	if err := s.ReleaseClaim(ctx, stageExecutionID); err != nil {
		t.Fatalf("ReleaseClaim failed: %v", err)
	}

	record, claimed, err := s.ClaimForReplay(ctx, stageExecutionID)
	if err != nil {
		t.Fatalf("re-claim after release failed: %v", err)
	}
	if !claimed {
		t.Fatalf("после ReleaseClaim запись должна снова быть claim-able (pending)")
	}
	if record.StageName != "BILLING" {
		t.Fatalf("неверные данные после re-claim: %+v", record)
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