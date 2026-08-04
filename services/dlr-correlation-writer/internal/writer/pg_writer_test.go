package writer

import (
	"context"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
)

// TestFlushAgainstRealPostgres — реальный batch insert в dlr.dlr_correlation
// на локальном PostgreSQL 17 (brew, migrations/V009__dlr_correlation.sql уже
// применена в этой песочнице — см. migrations/README.md). Пропускается, если
// локальный Postgres недоступен (тот же обход, что и в остальной сессии,
// development_plan.md "Координация" п.5) — тот же паттерн, что
// execution-control-service/internal/store/audit_test.go.
func TestFlushAgainstRealPostgres(t *testing.T) {
	dsn := os.Getenv("DLR_CORRELATION_WRITER_TEST_DSN")
	if dsn == "" {
		dsn = "postgres://localhost:5432/mpp?sslmode=disable"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	w, err := NewPgWriter(ctx, dsn)
	if err != nil {
		t.Skipf("не удалось создать пул подключений к Postgres (%v) — пропуск, БД недоступна в этой песочнице", err)
	}
	defer w.Close()

	if err := w.pool.Ping(ctx); err != nil {
		t.Skipf("Postgres недоступен на %q (%v) — пропуск", dsn, err)
	}
	if err := w.EnsurePartition(ctx, time.Now()); err != nil {
		t.Fatalf("EnsurePartition: %v", err)
	}

	operatorID := "test-op-" + uuid.NewString()[:8]
	submittedAt := time.Now().UTC().Truncate(time.Microsecond)
	rec := CorrelationRecord{
		OperatorID:       operatorID,
		SmscMessageID:    "smsc-" + uuid.NewString(),
		SegmentID:        1,
		MessageID:        uuid.NewString(),
		StageExecutionID: uuid.NewString(),
		SubmittedAt:      submittedAt,
		ExpiresAt:        submittedAt.Add(48 * time.Hour),
	}

	if err := w.Flush(ctx, []CorrelationRecord{rec}); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	var count int
	row := w.pool.QueryRow(ctx,
		"SELECT count(*) FROM dlr.dlr_correlation WHERE operator_id = $1 AND smsc_message_id = $2",
		rec.OperatorID, rec.SmscMessageID)
	if err := row.Scan(&count); err != nil {
		t.Fatalf("select count: %v", err)
	}
	if count != 1 {
		t.Fatalf("ожидали ровно 1 строку после Flush, получили %d", count)
	}
}

// TestFlushIsIdempotentUnderRedelivery — прямая проверка того, ЗАЧЕМ здесь
// ON CONFLICT DO NOTHING, а не голый COPY (см. комментарий в pg_writer.go):
// повторный Flush ровно того же CorrelationRecord (тот же PRIMARY KEY)
// не должен создавать вторую строку — именно так at-least-once redelivery
// operator.submit.accepted не задваивает correlation-записи.
func TestFlushIsIdempotentUnderRedelivery(t *testing.T) {
	dsn := os.Getenv("DLR_CORRELATION_WRITER_TEST_DSN")
	if dsn == "" {
		dsn = "postgres://localhost:5432/mpp?sslmode=disable"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	w, err := NewPgWriter(ctx, dsn)
	if err != nil {
		t.Skipf("не удалось создать пул подключений к Postgres (%v) — пропуск, БД недоступна в этой песочнице", err)
	}
	defer w.Close()

	if err := w.pool.Ping(ctx); err != nil {
		t.Skipf("Postgres недоступен на %q (%v) — пропуск", dsn, err)
	}
	if err := w.EnsurePartition(ctx, time.Now()); err != nil {
		t.Fatalf("EnsurePartition: %v", err)
	}

	operatorID := "test-op-" + uuid.NewString()[:8]
	submittedAt := time.Now().UTC().Truncate(time.Microsecond)
	rec := CorrelationRecord{
		OperatorID:       operatorID,
		SmscMessageID:    "smsc-" + uuid.NewString(),
		SegmentID:        1,
		MessageID:        uuid.NewString(),
		StageExecutionID: uuid.NewString(),
		SubmittedAt:      submittedAt,
		ExpiresAt:        submittedAt.Add(48 * time.Hour),
	}

	if err := w.Flush(ctx, []CorrelationRecord{rec}); err != nil {
		t.Fatalf("первый Flush: %v", err)
	}
	// Имитация редоставленного operator.submit.accepted — тот же PRIMARY KEY
	// (operator_id, smsc_message_id, segment_id, submitted_at).
	if err := w.Flush(ctx, []CorrelationRecord{rec}); err != nil {
		t.Fatalf("повторный Flush не должен возвращать ошибку (ON CONFLICT DO NOTHING): %v", err)
	}

	var count int
	row := w.pool.QueryRow(ctx,
		"SELECT count(*) FROM dlr.dlr_correlation WHERE operator_id = $1 AND smsc_message_id = $2",
		rec.OperatorID, rec.SmscMessageID)
	if err := row.Scan(&count); err != nil {
		t.Fatalf("select count: %v", err)
	}
	if count != 1 {
		t.Fatalf("повторный Flush не должен был создать вторую строку, получили count=%d", count)
	}
}

// TestEnsurePartitionRequiredBeforeInsertPastBootstrapWindow — прямая
// регрессия на реальную находку (см. комментарий в pg_writer.go): без
// EnsurePartition, Flush в текущий час падает с "no partition of relation
// ... found for row", если этот час не входит в бутстрап-окно
// V015 (текущий час на момент применения миграций + 4 часа вперёд).
// Показываем, что EnsurePartition решает эту проблему, не просто
// предполагаем.
func TestEnsurePartitionRequiredBeforeInsertPastBootstrapWindow(t *testing.T) {
	dsn := os.Getenv("DLR_CORRELATION_WRITER_TEST_DSN")
	if dsn == "" {
		dsn = "postgres://localhost:5432/mpp?sslmode=disable"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	w, err := NewPgWriter(ctx, dsn)
	if err != nil {
		t.Skipf("не удалось создать пул подключений к Postgres (%v) — пропуск, БД недоступна в этой песочнице", err)
	}
	defer w.Close()
	if err := w.pool.Ping(ctx); err != nil {
		t.Skipf("Postgres недоступен на %q (%v) — пропуск", dsn, err)
	}

	// Далёкое будущее, случайный сдвиг в 1-500 лет — заведомо вне
	// бутстрап-окна V015, партиция для него точно не существует, пока
	// EnsurePartition её не создаст. Случайность обязательна: против живой,
	// не сбрасываемой между прогонами PostgreSQL фиксированный сдвиг
	// (например ровно +365 дней) сделал бы тест неповторяемым — второй
	// прогон нашёл бы уже созданную первым прогоном партицию и не увидел
	// бы ожидаемую ошибку (реально найдено при повторном запуске).
	future := time.Now().
		Add(time.Duration(1+rand.Int63n(500)) * 365 * 24 * time.Hour).
		UTC().Truncate(time.Microsecond)
	rec := CorrelationRecord{
		OperatorID:       "test-op-future",
		SmscMessageID:    "smsc-" + uuid.NewString(),
		SegmentID:        1,
		MessageID:        uuid.NewString(),
		StageExecutionID: uuid.NewString(),
		SubmittedAt:      future,
		ExpiresAt:        future.Add(48 * time.Hour),
	}

	if err := w.Flush(ctx, []CorrelationRecord{rec}); err == nil {
		t.Fatal("ожидали ошибку insert в несуществующую партицию БЕЗ EnsurePartition — если тест прошёл, партиция уже существовала по другой причине")
	}

	if err := w.EnsurePartition(ctx, future); err != nil {
		t.Fatalf("EnsurePartition: %v", err)
	}
	if err := w.Flush(ctx, []CorrelationRecord{rec}); err != nil {
		t.Fatalf("Flush после EnsurePartition должен пройти: %v", err)
	}
}

// TestDropOldPartitionsRemovesRowsPastRetentionWindow — LOW находка
// кодревью (PART 2, dlr-correlation-writer #2): dlr.drop_old_correlation_partitions
// была определена в V015, но нигде не вызывалась — эта регрессия
// подтверждает, что DropOldPartitions реально удаляет партицию и её
// строки, а не просто компилируется.
func TestDropOldPartitionsRemovesRowsPastRetentionWindow(t *testing.T) {
	dsn := os.Getenv("DLR_CORRELATION_WRITER_TEST_DSN")
	if dsn == "" {
		dsn = "postgres://localhost:5432/mpp?sslmode=disable"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	w, err := NewPgWriter(ctx, dsn)
	if err != nil {
		t.Skipf("не удалось создать пул подключений к Postgres (%v) — пропуск, БД недоступна в этой песочнице", err)
	}
	defer w.Close()
	if err := w.pool.Ping(ctx); err != nil {
		t.Skipf("Postgres недоступен на %q (%v) — пропуск", dsn, err)
	}

	// Случайный сдвиг в прошлое (100-500 лет) — заведомо старше любого
	// разумного p_retain_hours, и не пересекается с партициями, которые
	// могли остаться от других тестов/прогонов (та же причина случайности,
	// что и в TestEnsurePartitionRequiredBeforeInsertPastBootstrapWindow).
	past := time.Now().
		Add(-time.Duration(100+rand.Int63n(400)) * 365 * 24 * time.Hour).
		UTC().Truncate(time.Microsecond)
	if err := w.EnsurePartition(ctx, past); err != nil {
		t.Fatalf("EnsurePartition (прошлое): %v", err)
	}
	operatorID := "test-op-retention-" + uuid.NewString()[:8]
	rec := CorrelationRecord{
		OperatorID:       operatorID,
		SmscMessageID:    "smsc-" + uuid.NewString(),
		SegmentID:        1,
		MessageID:        uuid.NewString(),
		StageExecutionID: uuid.NewString(),
		SubmittedAt:      past,
		ExpiresAt:        past.Add(48 * time.Hour),
	}
	if err := w.Flush(ctx, []CorrelationRecord{rec}); err != nil {
		t.Fatalf("Flush в прошлую партицию: %v", err)
	}

	var beforeCount int
	if err := w.pool.QueryRow(ctx,
		"SELECT count(*) FROM dlr.dlr_correlation WHERE operator_id = $1", rec.OperatorID,
	).Scan(&beforeCount); err != nil {
		t.Fatalf("select count (до retention): %v", err)
	}
	if beforeCount != 1 {
		t.Fatalf("ожидали 1 строку до retention, получили %d", beforeCount)
	}

	dropped, err := w.DropOldPartitions(ctx, 48)
	if err != nil {
		t.Fatalf("DropOldPartitions: %v", err)
	}
	if dropped < 1 {
		t.Fatalf("ожидали дропнуть минимум 1 партицию (заведомо древнюю), получили %d", dropped)
	}

	var afterCount int
	if err := w.pool.QueryRow(ctx,
		"SELECT count(*) FROM dlr.dlr_correlation WHERE operator_id = $1", rec.OperatorID,
	).Scan(&afterCount); err != nil {
		t.Fatalf("select count (после retention): %v", err)
	}
	if afterCount != 0 {
		t.Fatalf("строка должна была исчезнуть вместе с дропнутой партицией, получили count=%d", afterCount)
	}
}
