package correlation

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
)

// TestLookupAgainstRealPostgres — реальный round-trip против локального
// PostgreSQL 17: пишем строку напрямую (имитируя dlr-correlation-writer,
// тот же PRIMARY KEY, та же таблица `dlr.dlr_correlation`), читаем через
// Store.Lookup, сверяем поля. Тот же паттерн, что
// dlr-correlation-writer/internal/writer/pg_writer_test.go.
func TestLookupAgainstRealPostgres(t *testing.T) {
	dsn := os.Getenv("DLR_MANAGER_TEST_DSN")
	if dsn == "" {
		dsn = "postgres://localhost:5432/mpp?sslmode=disable"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	store, err := NewStore(ctx, dsn)
	if err != nil {
		t.Skipf("не удалось создать пул подключений к Postgres (%v) — пропуск, БД недоступна в этой песочнице", err)
	}
	defer store.Close()

	operatorID := "test-op-" + uuid.NewString()[:8]
	smscMessageID := "smsc-" + uuid.NewString()
	messageID := uuid.NewString()
	stageExecutionID := uuid.NewString()
	submittedAt := time.Now().UTC().Truncate(time.Microsecond)
	expiresAt := submittedAt.Add(48 * time.Hour)

	// Партиция должна существовать (dlr-correlation-writer гарантирует это
	// в проде через EnsurePartition — здесь создаём напрямую тем же
	// SQL-вызовом, если её ещё нет).
	if _, err := store.pool.Exec(ctx, "SELECT dlr.create_correlation_partition($1)", submittedAt.Truncate(time.Hour)); err != nil {
		t.Skipf("не удалось создать партицию (%v) — пропуск", err)
	}

	_, err = store.pool.Exec(ctx, `
		INSERT INTO dlr.dlr_correlation (operator_id, smsc_message_id, segment_id, message_id, stage_execution_id, submitted_at, expires_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`, operatorID, smscMessageID, int32(1), messageID, stageExecutionID, submittedAt, expiresAt)
	if err != nil {
		t.Fatalf("insert тестовой строки: %v", err)
	}

	rec, err := store.Lookup(ctx, operatorID, smscMessageID, 1, submittedAt.Add(time.Minute))
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if rec == nil {
		t.Fatal("ожидали найденную запись, получили nil")
	}
	if rec.MessageID != messageID {
		t.Errorf("MessageID = %q, want %q", rec.MessageID, messageID)
	}
	if rec.StageExecutionID != stageExecutionID {
		t.Errorf("StageExecutionID = %q, want %q", rec.StageExecutionID, stageExecutionID)
	}
}

// TestLookupIgnoresReusedSmscMessageIdSubmittedAfterTheDlr — прямое
// доказательство исправления HIGH находки кодревью: оператор переиспользует
// smsc_message_id для ДВУХ разных submission'ов (A раньше, B позже); DLR,
// пришедшая вскоре после A (до того как B вообще существовал), должна
// корректно коррелировать к A, даже если к моменту запроса Lookup в таблице
// уже есть более поздняя B. Без границы submitted_at <= received_at Lookup
// вернул бы B (просто "самая свежая строка"), что означало бы неверный
// message_id/stage_execution_id в опубликованном DeliveryStatusEvent.
func TestLookupIgnoresReusedSmscMessageIdSubmittedAfterTheDlr(t *testing.T) {
	dsn := os.Getenv("DLR_MANAGER_TEST_DSN")
	if dsn == "" {
		dsn = "postgres://localhost:5432/mpp?sslmode=disable"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	store, err := NewStore(ctx, dsn)
	if err != nil {
		t.Skipf("не удалось создать пул подключений к Postgres (%v) — пропуск, БД недоступна в этой песочнице", err)
	}
	defer store.Close()

	operatorID := "test-op-" + uuid.NewString()[:8]
	smscMessageID := "smsc-reused-" + uuid.NewString()
	messageIDA := uuid.NewString()
	messageIDB := uuid.NewString()
	submittedAtA := time.Now().UTC().Truncate(time.Microsecond)
	submittedAtB := submittedAtA.Add(10 * time.Minute) // оператор переиспользовал smsc_message_id позже
	dlrReceivedAtForA := submittedAtA.Add(30 * time.Second)
	expiresAt := submittedAtB.Add(48 * time.Hour)

	if _, err := store.pool.Exec(ctx, "SELECT dlr.create_correlation_partition($1)", submittedAtA.Truncate(time.Hour)); err != nil {
		t.Skipf("не удалось создать партицию (%v) — пропуск", err)
	}
	if _, err := store.pool.Exec(ctx, "SELECT dlr.create_correlation_partition($1)", submittedAtB.Truncate(time.Hour)); err != nil {
		t.Skipf("не удалось создать партицию (%v) — пропуск", err)
	}

	for _, row := range []struct {
		messageID   string
		submittedAt time.Time
	}{
		{messageIDA, submittedAtA},
		{messageIDB, submittedAtB},
	} {
		_, err = store.pool.Exec(ctx, `
			INSERT INTO dlr.dlr_correlation (operator_id, smsc_message_id, segment_id, message_id, stage_execution_id, submitted_at, expires_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
		`, operatorID, smscMessageID, int32(1), row.messageID, uuid.NewString(), row.submittedAt, expiresAt)
		if err != nil {
			t.Fatalf("insert тестовой строки (message_id=%s): %v", row.messageID, err)
		}
	}

	rec, err := store.Lookup(ctx, operatorID, smscMessageID, 1, dlrReceivedAtForA)
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if rec == nil {
		t.Fatal("ожидали найденную запись, получили nil")
	}
	if rec.MessageID != messageIDA {
		t.Fatalf("Lookup вернул message_id=%q (submitted_at=%v), ожидали message_id=%q (A, до DLR) — не более позднюю переиспользованную B",
			rec.MessageID, rec.SubmittedAt, messageIDA)
	}
}

func TestLookupReturnsNilNilWhenNotFound(t *testing.T) {
	dsn := os.Getenv("DLR_MANAGER_TEST_DSN")
	if dsn == "" {
		dsn = "postgres://localhost:5432/mpp?sslmode=disable"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	store, err := NewStore(ctx, dsn)
	if err != nil {
		t.Skipf("не удалось создать пул подключений к Postgres (%v) — пропуск, БД недоступна в этой песочнице", err)
	}
	defer store.Close()

	rec, err := store.Lookup(ctx, "nonexistent-operator", "nonexistent-smsc-id-"+uuid.NewString(), 1, time.Now())
	if err != nil {
		t.Fatalf("Lookup для несуществующей записи не должен возвращать ошибку: %v", err)
	}
	if rec != nil {
		t.Fatalf("ожидали nil для ненайденной записи, получили %+v", rec)
	}
}
