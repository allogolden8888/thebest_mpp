package writer

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
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
}

func NewPgWriter(ctx context.Context, databaseURL string) (*PgWriter, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("pgxpool.New: %w", err)
	}
	return &PgWriter{pool: pool}, nil
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
	_, err := w.pool.Exec(ctx, "SELECT dlr.create_correlation_partition($1)", hourStart.Truncate(time.Hour))
	if err != nil {
		return fmt.Errorf("dlr.create_correlation_partition: %w", err)
	}
	return nil
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
			return fmt.Errorf("batch insert dlr.dlr_correlation: %w", err)
		}
	}
	return nil
}
