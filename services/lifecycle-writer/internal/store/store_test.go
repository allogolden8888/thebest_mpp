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
		MessageID: messageID, CurrentStatus: "DELIVERED", Terminal: true, UpdatedAt: time.Now(), LifecycleVersion: 1,
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

// TestUpdateReadModelIgnoresOutOfOrderRedelivery — CODE_REVIEW.md MEDIUM
// finding #3: раньше UpdateReadModel безусловно перезаписывал
// current_status/terminal, поэтому редоставленное после rebalance старое
// событие (например SUBMITTED), пришедшее ПОСЛЕ уже применённого более
// нового (DELIVERED), откатывало партнёр-facing read model назад.
func TestUpdateReadModelIgnoresOutOfOrderRedelivery(t *testing.T) {
	pool := testPool(t)
	defer pool.Close()
	s := New(pool)
	ctx := context.Background()
	messageID := uniqueMessageID(t)

	if err := s.InsertReadModel(ctx, core.ReadModelRow{
		MessageID: messageID, PartnerID: "acme", ApplicationID: "acme_main", TraceID: uniqueMessageID(t),
		CurrentStatus: "RECEIVED", Timestamp: time.Now(),
	}); err != nil {
		t.Fatalf("InsertReadModel failed: %v", err)
	}

	// Новое событие (lifecycle_version=2) применяется первым.
	if err := s.UpdateReadModel(ctx, core.ReadModelUpdate{
		MessageID: messageID, CurrentStatus: "DELIVERED", Terminal: true, UpdatedAt: time.Now(), LifecycleVersion: 2,
	}); err != nil {
		t.Fatalf("UpdateReadModel (v2) failed: %v", err)
	}

	// Старое, редоставленное событие (lifecycle_version=1) приходит
	// ПОСЛЕ — не должно откатить read model назад.
	if err := s.UpdateReadModel(ctx, core.ReadModelUpdate{
		MessageID: messageID, CurrentStatus: "SUBMITTED", Terminal: false, UpdatedAt: time.Now(), LifecycleVersion: 1,
	}); err != nil {
		t.Fatalf("UpdateReadModel (stale v1) failed: %v", err)
	}

	var status string
	var terminal bool
	err := pool.QueryRow(ctx, `SELECT current_status, terminal FROM messaging.message_read_model WHERE message_id = $1`, messageID).
		Scan(&status, &terminal)
	if err != nil {
		t.Fatalf("readback failed: %v", err)
	}
	if status != "DELIVERED" || !terminal {
		t.Fatalf("устаревшее событие откатило read model назад: status=%s terminal=%v, ожидали DELIVERED/true", status, terminal)
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

// TestBatchInsertReadModelAndBatchUpdateReadModel — CODE_REVIEW.md MEDIUM
// finding: read model раньше писался синхронно по одной строке, не
// батчем вместе с history/dlq. Проверяем, что batch-версии дают тот же
// результат, включая lifecycle_version-guard.
func TestBatchInsertReadModelAndBatchUpdateReadModel(t *testing.T) {
	pool := testPool(t)
	defer pool.Close()
	s := New(pool)
	ctx := context.Background()
	id1, id2 := uniqueMessageID(t), uniqueMessageID(t)

	err := s.BatchInsertReadModel(ctx, []core.ReadModelRow{
		{MessageID: id1, PartnerID: "acme", ApplicationID: "app", TraceID: uniqueMessageID(t), CurrentStatus: "RECEIVED", Timestamp: time.Now()},
		{MessageID: id2, PartnerID: "acme", ApplicationID: "app", TraceID: uniqueMessageID(t), CurrentStatus: "RECEIVED", Timestamp: time.Now()},
	})
	if err != nil {
		t.Fatalf("BatchInsertReadModel failed: %v", err)
	}

	missing, err := s.BatchUpdateReadModel(ctx, []core.ReadModelUpdate{
		{MessageID: id1, CurrentStatus: "DELIVERED", Terminal: true, UpdatedAt: time.Now(), LifecycleVersion: 1},
		{MessageID: id2, CurrentStatus: "FAILED", Terminal: true, UpdatedAt: time.Now(), LifecycleVersion: 1},
	})
	if err != nil {
		t.Fatalf("BatchUpdateReadModel failed: %v", err)
	}
	if len(missing) != 0 {
		t.Fatalf("ожидали 0 missing (обе строки read model уже существуют), получили %d", len(missing))
	}

	for id, want := range map[string]string{id1: "DELIVERED", id2: "FAILED"} {
		var status string
		if err := pool.QueryRow(ctx, `SELECT current_status FROM messaging.message_read_model WHERE message_id = $1`, id).Scan(&status); err != nil {
			t.Fatalf("readback failed: %v", err)
		}
		if status != want {
			t.Fatalf("message_id=%s: status=%s, want %s", id, status, want)
		}
	}
}

// TestBatchUpdateReadModelReturnsMissingForRaceWithInsert — прямое
// доказательство реального бага "current_status застревает на RECEIVED"
// (не гипотеза, см. javadoc BatchUpdateReadModel): incoming.messages и
// message.lifecycle — разные топики одного consumer'а, ничто не
// гарантирует порядок между ними. Если update приходит РАНЬШЕ, чем
// строка read model создана — раньше это был молчаливый no-op (0 affected
// rows), обновление терялось навсегда. Теперь такое обновление
// возвращается вызывающей стороне как missing, не отбрасывается.
func TestBatchUpdateReadModelReturnsMissingForRaceWithInsert(t *testing.T) {
	pool := testPool(t)
	defer pool.Close()
	s := New(pool)
	ctx := context.Background()
	neverInserted := uniqueMessageID(t)

	missing, err := s.BatchUpdateReadModel(ctx, []core.ReadModelUpdate{
		{MessageID: neverInserted, CurrentStatus: "SUBMITTED", Terminal: false, UpdatedAt: time.Now(), LifecycleVersion: 1},
	})
	if err != nil {
		t.Fatalf("BatchUpdateReadModel failed: %v", err)
	}
	if len(missing) != 1 || missing[0].MessageID != neverInserted {
		t.Fatalf("ожидали ровно 1 missing для message_id=%s (строка ещё не создана), получили %+v", neverInserted, missing)
	}

	var count int
	pool.QueryRow(ctx, `SELECT count(*) FROM messaging.message_read_model WHERE message_id = $1`, neverInserted).Scan(&count)
	if count != 0 {
		t.Fatalf("update для несуществующего message_id не должен был создать строку сам по себе, найдено %d", count)
	}
}

// TestBatchUpdateReadModelStaleVersionIsNotMissing — вторая половина той
// же гарантии: строка СУЩЕСТВУЕТ, но lifecycle_version уже не новее (
// легитимный дубликат/устаревшая редоставка) — это НЕ гонка, retry не
// нужен, иначе буфер рос бы вечно на заведомо неприменимых обновлениях.
func TestBatchUpdateReadModelStaleVersionIsNotMissing(t *testing.T) {
	pool := testPool(t)
	defer pool.Close()
	s := New(pool)
	ctx := context.Background()
	id := uniqueMessageID(t)

	if err := s.BatchInsertReadModel(ctx, []core.ReadModelRow{
		{MessageID: id, PartnerID: "acme", ApplicationID: "app", TraceID: uniqueMessageID(t), CurrentStatus: "RECEIVED", Timestamp: time.Now()},
	}); err != nil {
		t.Fatalf("BatchInsertReadModel failed: %v", err)
	}
	if _, err := s.BatchUpdateReadModel(ctx, []core.ReadModelUpdate{
		{MessageID: id, CurrentStatus: "DELIVERED", Terminal: true, UpdatedAt: time.Now(), LifecycleVersion: 2},
	}); err != nil {
		t.Fatalf("BatchUpdateReadModel (v2) failed: %v", err)
	}

	// Устаревшее редоставленное событие (lifecycle_version=1, уже позади
	// применённого v2) — строка существует, просто guard его отклоняет.
	missing, err := s.BatchUpdateReadModel(ctx, []core.ReadModelUpdate{
		{MessageID: id, CurrentStatus: "SUBMITTED", Terminal: false, UpdatedAt: time.Now(), LifecycleVersion: 1},
	})
	if err != nil {
		t.Fatalf("BatchUpdateReadModel (stale v1) failed: %v", err)
	}
	if len(missing) != 0 {
		t.Fatalf("устаревшая версия для существующей строки не должна считаться missing, получили %+v", missing)
	}

	var status string
	pool.QueryRow(ctx, `SELECT current_status FROM messaging.message_read_model WHERE message_id = $1`, id).Scan(&status)
	if status != "DELIVERED" {
		t.Fatalf("устаревшее обновление не должно было откатить статус назад: %s", status)
	}
}

func TestBatchInsertLifecycleHistory(t *testing.T) {
	pool := testPool(t)
	defer pool.Close()
	s := New(pool)
	ctx := context.Background()
	messageID := uniqueMessageID(t)
	now := time.Now()

	// EnsurePartition — без него этот INSERT падает с "no partition of
	// relation found for row" на любой машине, где партиция текущего часа
	// ещё не создана бутстрап-окном V015 (см. javadoc EnsurePartition) —
	// тот же принцип, что уже применяется в dlr-correlation-writer's
	// pg_writer_test.go.
	if err := s.EnsurePartition(ctx, now); err != nil {
		t.Fatalf("EnsurePartition: %v", err)
	}

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

// TestInsertPastBootstrapWindowFailsWithoutEnsurePartition — прямое
// доказательство самого бага "current_status застревает" (не гипотеза):
// достаточно далёкий в будущем час заведомо не создан бутстрап-окном
// V015 — без EnsurePartition вставка в него падает ровно так, как падала
// в реальной эксплуатации (см. package doc store.go EnsurePartition).
func TestInsertPastBootstrapWindowFailsWithoutEnsurePartition(t *testing.T) {
	pool := testPool(t)
	defer pool.Close()
	s := New(pool)
	ctx := context.Background()
	messageID := uniqueMessageID(t)
	future := time.Now().Add(365 * 24 * time.Hour)

	rows := []core.LifecycleHistoryRow{
		{MessageID: messageID, LifecycleVersion: 1, Status: "SUBMITTED", EventID: uniqueMessageID(t), OccurredAt: future, Source: "message.lifecycle"},
	}
	if err := s.BatchInsertLifecycleHistory(ctx, rows); err == nil {
		t.Fatal("ожидали ошибку insert в несуществующую партицию БЕЗ EnsurePartition — если тест прошёл, партиция уже существовала по другой причине")
	}

	if err := s.EnsurePartition(ctx, future); err != nil {
		t.Fatalf("EnsurePartition: %v", err)
	}
	if err := s.BatchInsertLifecycleHistory(ctx, rows); err != nil {
		t.Fatalf("BatchInsertLifecycleHistory после EnsurePartition должен пройти: %v", err)
	}
}

func TestDropOldPartitions(t *testing.T) {
	pool := testPool(t)
	defer pool.Close()
	s := New(pool)
	ctx := context.Background()

	// Не проверяем конкретное число удалённых партиций (зависит от
	// состояния БД на момент прогона других тестов) — только что вызов
	// не падает и возвращает неотрицательное число, тот же уровень
	// проверки, что и у dlr-correlation-writer's эквивалентного теста.
	dropped, err := s.DropOldPartitions(ctx, 72)
	if err != nil {
		t.Fatalf("DropOldPartitions: %v", err)
	}
	if dropped < 0 {
		t.Fatalf("dropped не должен быть отрицательным: %d", dropped)
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

// Фаза 11 плана закрытия API-пробелов (migrations/V028): sandbox
// зафиксирован на INSERT из IncomingMessage.Sandbox и обязан пережить
// последующий UpdateReadModel (которое его вообще не трогает — SET
// current_status/terminal/updated_at/lifecycle_version, не sandbox).
func TestInsertReadModelPersistsSandboxThroughUpdate(t *testing.T) {
	pool := testPool(t)
	defer pool.Close()
	s := New(pool)
	ctx := context.Background()
	messageID := uniqueMessageID(t)

	if err := s.InsertReadModel(ctx, core.ReadModelRow{
		MessageID: messageID, PartnerID: "acme", ApplicationID: "acme_main", TraceID: uniqueMessageID(t),
		CurrentStatus: "RECEIVED", Timestamp: time.Now(), Sandbox: true,
	}); err != nil {
		t.Fatalf("InsertReadModel failed: %v", err)
	}

	if err := s.UpdateReadModel(ctx, core.ReadModelUpdate{
		MessageID: messageID, CurrentStatus: "DELIVERED", Terminal: true, UpdatedAt: time.Now(), LifecycleVersion: 1,
	}); err != nil {
		t.Fatalf("UpdateReadModel failed: %v", err)
	}

	var sandbox bool
	if err := pool.QueryRow(ctx, `SELECT sandbox FROM messaging.message_read_model WHERE message_id = $1`, messageID).Scan(&sandbox); err != nil {
		t.Fatalf("readback failed: %v", err)
	}
	if !sandbox {
		t.Fatalf("sandbox=true на INSERT обязан пережить последующий UpdateReadModel")
	}
}

func TestInsertReadModelDefaultsSandboxToFalse(t *testing.T) {
	pool := testPool(t)
	defer pool.Close()
	s := New(pool)
	ctx := context.Background()
	messageID := uniqueMessageID(t)

	if err := s.InsertReadModel(ctx, core.ReadModelRow{
		MessageID: messageID, PartnerID: "acme", ApplicationID: "acme_main", TraceID: uniqueMessageID(t),
		CurrentStatus: "RECEIVED", Timestamp: time.Now(),
	}); err != nil {
		t.Fatalf("InsertReadModel failed: %v", err)
	}

	var sandbox bool
	if err := pool.QueryRow(ctx, `SELECT sandbox FROM messaging.message_read_model WHERE message_id = $1`, messageID).Scan(&sandbox); err != nil {
		t.Fatalf("readback failed: %v", err)
	}
	if sandbox {
		t.Fatalf("обычное сообщение не должно получить sandbox=true")
	}
}