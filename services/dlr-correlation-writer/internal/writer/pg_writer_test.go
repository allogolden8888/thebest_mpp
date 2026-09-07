package writer

import (
	"context"
	"math/rand"
	"os"
	"strings"
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
	// AddDate, а не Add(N*365*24h): time.Duration это int64 наносекунд,
	// её потолок ~292 года — сдвиг на 293+ лет ПЕРЕПОЛНЯЛСЯ и заворачивался
	// в произвольный (нередко противоположный по знаку) момент. Здесь это
	// давало ложные "проходы" мимо проверяемого условия, а в
	// TestDropOldPartitionsRemovesRowsPastRetentionWindow — реальные
	// флаки: "прошлое" уезжало в БУДУЩЕЕ, retention такую партицию не
	// трогала, тест падал с dropped=0 (воспроизведено на чистом HEAD,
	// ~1 падение из 5; в БД остались следы — партиции 2149..2312 годов).
	future := time.Now().UTC().
		AddDate(1+rand.Intn(500), 0, 0).
		Truncate(time.Microsecond)
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
	// AddDate, а не Add(N*365*24h) — см. переполнение time.Duration в
	// TestEnsurePartitionRequiredBeforeInsertPastBootstrapWindow выше.
	past := time.Now().UTC().
		AddDate(-(100 + rand.Intn(400)), 0, 0).
		Truncate(time.Microsecond)
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

// TestEnsurePartitionsForBatchCreatesPartitionForPastSubmittedAt — прямая
// регрессия на фатальный дефект, из-за которого сервис 18 суток подряд
// (67153 строки в логах docker-dlr-correlation-writer-1, непрерывно с
// 2026-08-20 05:00:07) не записал НИ ОДНОЙ корреляции: партиции
// обеспечивались только под текущий/следующий час, а не под фактический
// submitted_at записей. Стоило сервису отстать от operator.submit.accepted
// или переиграть топик — submitted_at попадал в ПРОШЛЫЙ час, партиции под
// который нет, batch insert падал с SQLSTATE 23514, offset не двигался,
// тот же батч переигрывался вечно.
//
// В отличие от TestEnsurePartitionRequiredBeforeInsertPastBootstrapWindow
// (там случайное далёкое будущее гарантирует отсутствие партиции), здесь
// час обязан лежать ВНУТРИ окна — случайностью его отсутствие не
// обеспечить, поэтому предусловие создаётся явно: партиция выбранного часа
// дропается перед проверкой. Это локальная dev-БД, в дропаемом часе могут
// быть только строки прошлых прогонов тестов (см. README, "тесты пишут
// настоящие строки и не удаляют их за собой").
func TestEnsurePartitionsForBatchCreatesPartitionForPastSubmittedAt(t *testing.T) {
	dsn := os.Getenv("DLR_CORRELATION_WRITER_TEST_DSN")
	if dsn == "" {
		dsn = "postgres://localhost:5432/mpp?sslmode=disable"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	w, err := NewPgWriter(ctx, dsn)
	if err != nil {
		t.Skipf("не удалось создать пул подключений к Postgres (%v) — пропуск, БД недоступна в этой песочнице", err)
	}
	defer w.Close()
	if err := w.pool.Ping(ctx); err != nil {
		t.Skipf("Postgres недоступен на %q (%v) — пропуск", dsn, err)
	}

	// Прошлый час внутри окна корреляции (48ч): 3..40 часов назад —
	// ровно тот случай, который ломал сервис (отставание/replay).
	past := time.Now().UTC().
		Add(-time.Duration(3+rand.Int63n(37)) * time.Hour).
		Truncate(time.Microsecond)
	pastHour := past.Truncate(time.Hour)
	partitionName := "dlr_correlation_" + pastHour.Format("20060102_15")

	// Предусловие: партиции этого часа заведомо нет.
	if _, err := w.pool.Exec(ctx, "DROP TABLE IF EXISTS dlr."+partitionName); err != nil {
		t.Fatalf("подготовка (DROP %s): %v", partitionName, err)
	}

	rec := CorrelationRecord{
		OperatorID:       "test-op-past-" + uuid.NewString()[:8],
		SmscMessageID:    "smsc-" + uuid.NewString(),
		SegmentID:        1,
		MessageID:        uuid.NewString(),
		StageExecutionID: uuid.NewString(),
		SubmittedAt:      past,
		ExpiresAt:        past.Add(48 * time.Hour),
	}

	// Старое поведение: партиции только под текущий/следующий час — именно
	// так сервис и вставал в вечный retry.
	if err := w.EnsurePartition(ctx, time.Now()); err != nil {
		t.Fatalf("EnsurePartition(now): %v", err)
	}
	if err := w.EnsurePartition(ctx, time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("EnsurePartition(now+1h): %v", err)
	}
	if err := w.Flush(ctx, []CorrelationRecord{rec}); err == nil {
		t.Fatalf("ожидали SQLSTATE 23514 для submitted_at=%s без партиции — если ошибки нет, партиция %s существовала вопреки предусловию", past.Format(time.RFC3339), partitionName)
	} else if !strings.Contains(err.Error(), "no partition of relation") {
		t.Fatalf("ожидали именно 'no partition of relation', получили: %v", err)
	}

	// Новое поведение: партиция создаётся под ФАКТИЧЕСКИЙ submitted_at.
	plan, err := w.EnsurePartitionsForBatch(ctx, []CorrelationRecord{rec}, time.Now())
	if err != nil {
		t.Fatalf("EnsurePartitionsForBatch: %v", err)
	}
	if len(plan.Rejected) != 0 {
		t.Fatalf("запись внутри окна не должна отбрасываться, Rejected=%d", len(plan.Rejected))
	}
	if len(plan.Accepted) != 1 {
		t.Fatalf("ожидали 1 принятую запись, получили %d", len(plan.Accepted))
	}
	if err := w.Flush(ctx, plan.Accepted); err != nil {
		t.Fatalf("Flush после EnsurePartitionsForBatch должен пройти (это и есть починка): %v", err)
	}

	var count int
	if err := w.pool.QueryRow(ctx,
		"SELECT count(*) FROM dlr.dlr_correlation WHERE operator_id = $1 AND smsc_message_id = $2",
		rec.OperatorID, rec.SmscMessageID,
	).Scan(&count); err != nil {
		t.Fatalf("select count: %v", err)
	}
	if count != 1 {
		t.Fatalf("ожидали 1 строку в прошлой партиции после починки, получили %d", count)
	}

	// Кэш ensuredHours: повторный вызов не должен ходить в БД снова.
	// Проверяем через pg_stat: дешевле и надёжнее — просто убеждаемся, что
	// час уже помечен как обеспеченный.
	if !w.partitionEnsured(pastHour) {
		t.Fatal("час должен был попасть в кэш ensuredHours после успешного создания")
	}
}

// TestPlanPartitionsRejectsOutOfWindowSubmittedAt — вторая половина той же
// починки, чистая (без PostgreSQL): создавать партиции под произвольный
// submitted_at нельзя безоговорочно. Одна запись с битым/нулевым
// таймстампом (epoch-1970 в этом проекте уже встречались) иначе заставила
// бы сервис создать десятки тысяч пустых почасовых партиций. Такие записи
// отбрасываются, а не вставляются: вставка гарантированно упала бы с 23514
// и снова заблокировала бы весь батч.
func TestPlanPartitionsRejectsOutOfWindowSubmittedAt(t *testing.T) {
	now := time.Date(2026, 9, 7, 12, 30, 0, 0, time.UTC)

	mk := func(id string, submittedAt time.Time) CorrelationRecord {
		return CorrelationRecord{OperatorID: id, SmscMessageID: id, SegmentID: 1, SubmittedAt: submittedAt}
	}
	records := []CorrelationRecord{
		mk("epoch-zero", time.Time{}),             // нулевой time.Time
		mk("epoch-1970", time.Unix(0, 0).UTC()),   // epoch — реальный класс мусора
		mk("too-old", now.Add(-49*time.Hour)),     // за окном корреляции
		mk("backfill-ok", now.Add(-40*time.Hour)), // отставание внутри окна — ДОЛЖНО пройти
		mk("replay-ok", now.Add(-90*time.Minute)), // прошлый час — тот самый сломанный кейс
		mk("now-ok", now),                         // текущий час
		mk("clock-skew-ok", now.Add(2*time.Hour)), // небольшой рассинхрон часов
		mk("far-future", now.Add(72*time.Hour)),   // за окном вперёд
	}

	plan := PlanPartitions(records, now, DefaultPartitionMaxPast, DefaultPartitionMaxFuture)

	accepted := make(map[string]bool, len(plan.Accepted))
	for _, r := range plan.Accepted {
		accepted[r.OperatorID] = true
	}
	for _, id := range []string{"backfill-ok", "replay-ok", "now-ok", "clock-skew-ok"} {
		if !accepted[id] {
			t.Errorf("запись %q внутри окна должна быть принята", id)
		}
	}
	rejected := make(map[string]bool, len(plan.Rejected))
	for _, r := range plan.Rejected {
		rejected[r.OperatorID] = true
	}
	for _, id := range []string{"epoch-zero", "epoch-1970", "too-old", "far-future"} {
		if !rejected[id] {
			t.Errorf("запись %q вне окна должна быть отброшена, а не отправлена в insert", id)
		}
	}

	// Часы: текущий, следующий и по одному на каждый принятый час — и
	// никогда больше ширины окна (иначе один мусорный таймстамп = тысячи
	// CREATE TABLE).
	maxHours := int((DefaultPartitionMaxPast + DefaultPartitionMaxFuture).Hours()) + 2
	if len(plan.Hours) > maxHours {
		t.Errorf("число создаваемых партиций (%d) не должно превышать ширину окна (%d)", len(plan.Hours), maxHours)
	}
	wantHours := map[time.Time]bool{
		now.Truncate(time.Hour):                        true, // now-ok + текущий час
		now.Truncate(time.Hour).Add(time.Hour):         true, // следующий час
		now.Add(-40 * time.Hour).Truncate(time.Hour):   true,
		now.Add(-90 * time.Minute).Truncate(time.Hour): true,
		now.Add(2 * time.Hour).Truncate(time.Hour):     true,
	}
	if len(plan.Hours) != len(wantHours) {
		t.Fatalf("ожидали %d различных часов, получили %d: %v", len(wantHours), len(plan.Hours), plan.Hours)
	}
	for _, h := range plan.Hours {
		if !wantHours[h] {
			t.Errorf("неожиданный час в плане: %s", h.Format(time.RFC3339))
		}
	}
	for i := 1; i < len(plan.Hours); i++ {
		if !plan.Hours[i-1].Before(plan.Hours[i]) {
			t.Errorf("Hours должны быть отсортированы по возрастанию и без дублей: %v", plan.Hours)
		}
	}
}
