package writer

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PgWriter — flush_batch (service_internal_methods.md §4.1): batch insert
// в dlr.dlr_correlation. Найдено при реализации, не в спеке дословно:
// спецификация говорит "COPY/batch insert", но чистый `COPY` не умеет
// `ON CONFLICT` — а at-least-once redelivery `operator.submit.accepted`
// (тот же Kafka-consumer-group рестарт после сбоя между flush и offset
// commit, тот же класс, что уже задокументирован во всех Kafka-consumer'ах
// этой сессии) means один и тот же CorrelationRecord может прийти на
// flush дважды. PRIMARY KEY (operator_id, smsc_message_id, segment_id,
// submitted_at) уже существует в V009 именно для этого — используется
// batched `INSERT ... ON CONFLICT DO NOTHING` (тот же идемпотентный
// паттерн, что уже реально проверен для billing_ledger, см.
// migrations/README.md), не голый COPY.
type PgWriter struct {
	pool *pgxpool.Pool

	// mu защищает ensuredHours/maxPast/maxFuture. ensuredHours — кэш уже
	// созданных в этом процессе часов: `CREATE TABLE IF NOT EXISTS` дёшев,
	// но не бесплатен — это round trip на каждый час на каждый flush
	// (flush идёт раз в BATCH_FLUSH_INTERVAL_MS, по умолчанию 2с). Кэш
	// сбрасывается для устаревших часов в DropOldPartitions и целиком —
	// если insert всё-таки упал с 23514 (партицию дропнул кто-то извне).
	mu           sync.Mutex
	ensuredHours map[time.Time]struct{}
	maxPast      time.Duration
	maxFuture    time.Duration
}

// Границы окна, в пределах которого сервис готов создавать почасовые
// партиции под ФАКТИЧЕСКИЙ submitted_at записей батча (см.
// EnsurePartitionsForBatch). Прошлое ограничено окном корреляции DLR
// (dlr-manager's DLR_CORRELATION_WINDOW, 48ч) — оно же окно retention
// этого сервиса (DLR_CORRELATION_RETAIN_HOURS): писать в час, который
// DropOldPartitions дропнет следующим же проходом, бессмысленно. Будущее
// ограничено сутками — запас на рассинхрон часов между сервисами.
// Ограничения обязательны, а не "на всякий случай": без них одна запись с
// битым/нулевым submitted_at (epoch-1970 таймстампы в этом проекте уже
// встречались) заставила бы сервис создать десятки тысяч пустых партиций.
const (
	DefaultPartitionMaxPast   = 48 * time.Hour
	DefaultPartitionMaxFuture = 24 * time.Hour
)

func NewPgWriter(ctx context.Context, databaseURL string) (*PgWriter, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("pgxpool.New: %w", err)
	}
	return &PgWriter{
		pool:         pool,
		ensuredHours: make(map[time.Time]struct{}),
		maxPast:      DefaultPartitionMaxPast,
		maxFuture:    DefaultPartitionMaxFuture,
	}, nil
}

// SetPartitionWindow — переопределяет границы окна создания партиций
// (main.go связывает maxPast с DLR_CORRELATION_RETAIN_HOURS, чтобы окно
// записи и окно retention не разъехались).
func (w *PgWriter) SetPartitionWindow(maxPast, maxFuture time.Duration) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.maxPast = maxPast
	w.maxFuture = maxFuture
}

func (w *PgWriter) Close() {
	w.pool.Close()
}

// EnsurePartition — реальная находка при живом тестировании против
// PostgreSQL: `dlr.dlr_correlation` (V009) партиционирована по часам, а
// `V015__partition_maintenance.sql` создаёт партиции только на момент
// применения миграций ("текущий час + следующие 4 часа") и явно
// документирует, что `dlr.create_correlation_partition` дальше "вызывается
// любым внешним планировщиком (k8s CronJob, pg_cron, приложение) раз в
// час" — но ни один такой планировщик нигде в репозитории не заведён
// (проверено grep'ом по `create_correlation_partition`/CronJob). Без этого
// INSERT начинает падать с `no partition of relation "dlr_correlation"
// found for row` (SQLSTATE 23514) сразу после того, как истекает
// предсозданное окно — не гипотетически, реально воспроизведено в
// `pg_writer_test.go` при первом запуске против настоящей БД. Раз этот
// сервис — единственный писатель в эту таблицу, он и берёт на себя
// самообслуживание (`CREATE TABLE IF NOT EXISTS`, дешёвый idempotent
// вызов) — вызывается перед каждым flush в `cmd/dlr-correlation-writer/main.go`,
// не полагается на внешний планировщик, которого не существует.
func (w *PgWriter) EnsurePartition(ctx context.Context, hourStart time.Time) error {
	hour := hourStart.UTC().Truncate(time.Hour)
	if w.partitionEnsured(hour) {
		return nil
	}
	_, err := w.pool.Exec(ctx, "SELECT dlr.create_correlation_partition($1)", hour)
	if err != nil {
		return fmt.Errorf("dlr.create_correlation_partition(%s): %w", hour.Format(time.RFC3339), err)
	}
	w.rememberPartition(hour)
	return nil
}

func (w *PgWriter) partitionEnsured(hour time.Time) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	_, ok := w.ensuredHours[hour]
	return ok
}

func (w *PgWriter) rememberPartition(hour time.Time) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.ensuredHours[hour] = struct{}{}
}

// forgetPartitionsBefore/forgetAllPartitions — кэш ensuredHours это
// утверждение "партиция существует", а не "мы её когда-то создавали":
// как только партиции реально исчезают (retention — ниже, или внешний
// DROP — см. Flush), утверждение перестаёт быть верным и кэш обязан это
// признать, иначе сервис молча перестанет создавать нужную партицию.
func (w *PgWriter) forgetPartitionsBefore(cutoff time.Time) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for hour := range w.ensuredHours {
		if hour.Before(cutoff) {
			delete(w.ensuredHours, hour)
		}
	}
}

func (w *PgWriter) forgetAllPartitions() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.ensuredHours = make(map[time.Time]struct{})
}

// PartitionPlan — раскладка батча по почасовым партициям (результат
// PlanPartitions).
type PartitionPlan struct {
	// Hours — различные часы, партиции под которые нужно обеспечить перед
	// вставкой: часы фактических submitted_at записей батча + текущий и
	// следующий час. Отсортированы по возрастанию.
	Hours []time.Time
	// Accepted — записи, чей submitted_at попал в окно; только они идут в
	// Flush.
	Accepted []CorrelationRecord
	// Rejected — записи вне окна (битый/нулевой submitted_at, либо
	// отставание больше окна корреляции — коррелировать такое всё равно
	// уже некому). Осознанно НЕ пишутся: попытка вставки гарантированно
	// упала бы с 23514 и заблокировала бы весь батч, как это и произошло
	// в реальной эксплуатации.
	Rejected []CorrelationRecord
}

// PlanPartitions — чистая функция (тестируется без PostgreSQL): считает,
// какие почасовые партиции нужны батчу.
//
// РЕАЛЬНЫЙ ДЕФЕКТ, который она чинит (не гипотеза — измерено на живой
// системе): раньше main.go обеспечивал партиции только под ТЕКУЩИЙ и
// СЛЕДУЮЩИЙ час, а не под фактический submitted_at вставляемых записей.
// Стоило сервису отстать от топика operator.submit.accepted (или
// переиграть его), submitted_at записей оказывался в ПРОШЛЫХ часах,
// партиций под которые нет — batch insert падал с `no partition of
// relation "dlr_correlation" found for row` (SQLSTATE 23514), offset не
// коммитился, буфер не чистился, тот же батч переигрывался вечно и
// сервис не писал НИЧЕГО. В логах docker-dlr-correlation-writer-1 эта
// ошибка шла непрерывно 18 суток (67153 строки, с 2026-08-20 05:00:07),
// и вниз по цепочке: dlr-manager не мог сопоставить DLR → delivery.status
// пустой → нерешённые сообщения висят 30 минут в Runtime Redis.
func PlanPartitions(records []CorrelationRecord, now time.Time, maxPast, maxFuture time.Duration) PartitionPlan {
	nowHour := now.UTC().Truncate(time.Hour)
	earliest := now.UTC().Add(-maxPast).Truncate(time.Hour)
	latest := now.UTC().Add(maxFuture).Truncate(time.Hour)

	plan := PartitionPlan{Accepted: make([]CorrelationRecord, 0, len(records))}
	seen := make(map[time.Time]struct{}, len(records))
	addHour := func(hour time.Time) {
		if _, ok := seen[hour]; ok {
			return
		}
		seen[hour] = struct{}{}
		plan.Hours = append(plan.Hours, hour)
	}
	// Текущий и следующий час — как и раньше: буфер может пересечь границу
	// часа между первой записью в него и flush'ем, и партиция под "сейчас"
	// нужна даже когда батч пуст.
	addHour(nowHour)
	addHour(nowHour.Add(time.Hour))

	for _, r := range records {
		hour := r.SubmittedAt.UTC().Truncate(time.Hour)
		if hour.Before(earliest) || hour.After(latest) {
			plan.Rejected = append(plan.Rejected, r)
			continue
		}
		addHour(hour)
		plan.Accepted = append(plan.Accepted, r)
	}
	sort.Slice(plan.Hours, func(i, j int) bool { return plan.Hours[i].Before(plan.Hours[j]) })
	return plan
}

// EnsurePartitionsForBatch — создаёт недостающие партиции под фактические
// submitted_at батча (см. PlanPartitions) перед вставкой. Число round
// trip'ов ограничено сверху шириной окна (48+24 = максимум 73 часа) и на
// практике равно нулю: кэш ensuredHours отсекает повторные вызовы для уже
// созданных часов, а установившийся поток пишет в один-два часа.
func (w *PgWriter) EnsurePartitionsForBatch(ctx context.Context, records []CorrelationRecord, now time.Time) (PartitionPlan, error) {
	w.mu.Lock()
	maxPast, maxFuture := w.maxPast, w.maxFuture
	w.mu.Unlock()

	plan := PlanPartitions(records, now, maxPast, maxFuture)
	for _, hour := range plan.Hours {
		if err := w.EnsurePartition(ctx, hour); err != nil {
			return plan, err
		}
	}
	return plan, nil
}

// DropOldPartitions — LOW находка кодревью (PART 2, dlr-correlation-writer
// #2): `dlr.drop_old_correlation_partitions` (migrations/V015) была
// определена, но нигде в репозитории не вызывалась — партиции
// dlr.dlr_correlation росли неограниченно. Тот же принцип, что
// EnsurePartition: этот сервис — единственный писатель в таблицу, он же
// берёт на себя самообслуживание, вместо несуществующего внешнего
// CronJob/pg_cron. Вызывается реже, чем EnsurePartition (раз в час, не на
// каждый flush — DROP TABLE, не дешёвый idempotent CREATE IF NOT EXISTS),
// см. cmd/dlr-correlation-writer/main.go.
func (w *PgWriter) DropOldPartitions(ctx context.Context, retainHours int) (int, error) {
	var dropped int
	err := w.pool.QueryRow(ctx, "SELECT dlr.drop_old_correlation_partitions($1)", retainHours).Scan(&dropped)
	if err != nil {
		return 0, fmt.Errorf("dlr.drop_old_correlation_partitions: %w", err)
	}
	// Дропнутые часы больше не существуют — убрать их из кэша, иначе
	// EnsurePartition для такого часа вернул бы "уже есть" и insert снова
	// упал бы с 23514.
	w.forgetPartitionsBefore(time.Now().UTC().Add(-time.Duration(retainHours) * time.Hour).Truncate(time.Hour))
	return dropped, nil
}

const insertSQL = `
INSERT INTO dlr.dlr_correlation
	(operator_id, smsc_message_id, segment_id, message_id, stage_execution_id, submitted_at, expires_at)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (operator_id, smsc_message_id, segment_id, submitted_at) DO NOTHING
`

// Flush — один batch round-trip через pgx.Batch (pipelined, не N
// последовательных round-trip'ов), реальный API, не заглушка.
func (w *PgWriter) Flush(ctx context.Context, records []CorrelationRecord) error {
	if len(records) == 0 {
		return nil
	}

	batch := &pgx.Batch{}
	for _, r := range records {
		batch.Queue(insertSQL, r.OperatorID, r.SmscMessageID, r.SegmentID, r.MessageID, r.StageExecutionID, r.SubmittedAt, r.ExpiresAt)
	}

	results := w.pool.SendBatch(ctx, batch)
	defer results.Close()

	for range records {
		if _, err := results.Exec(); err != nil {
			// 23514 (check_violation) на партиционированной таблице — это
			// ровно "no partition of relation ... found for row". Раз мы
			// сюда дошли, кэш ensuredHours соврал (партицию дропнули
			// извне) — сбросить его, чтобы следующая попытка реально
			// вызвала create_correlation_partition, а не вечно
			// переигрывала тот же провальный батч.
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "23514" {
				w.forgetAllPartitions()
			}
			return fmt.Errorf("batch insert dlr.dlr_correlation: %w", err)
		}
	}
	return nil
}
