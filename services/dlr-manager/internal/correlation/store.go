// Package correlation — lookup_correlation (service_internal_methods.md
// §4.2): PostgreSQL read из dlr.dlr_correlation (написана
// dlr-correlation-writer, migrations/V009__dlr_correlation.sql).
package correlation

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Record struct {
	OperatorID       string
	SmscMessageID    string
	SegmentID        int32
	MessageID        string
	StageExecutionID string
	SubmittedAt      time.Time
	ExpiresAt        time.Time
}

type Store struct {
	pool *pgxpool.Pool
}

func NewStore(ctx context.Context, databaseURL string) (*Store, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("pgxpool.New: %w", err)
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Close() {
	s.pool.Close()
}

// Lookup — самый свежий (по submitted_at), но НЕ ПОЗЖЕ receivedAt,
// correlation-ряд для этой (operator_id, smsc_message_id, segment_id).
//
// HIGH находка кодревью (PART 2, dlr-manager #1) — исправлено здесь:
// раньше запрос не был привязан ко времени самой DLR (`ORDER BY
// submitted_at DESC LIMIT 1` без границы), из-за чего оператор,
// переиспользующий smsc_message_id (нормальная практика при wraparound
// счётчика на стороне SMSC), приводил к неверной корреляции — DLR,
// пришедшая вскоре после submission A (submitted_at=T1), после того как
// тот же smsc_message_id был повторно занят submission'ом B
// (submitted_at=T2 > T1), находила бы B вместо A, потому что B "более
// свежий" в таблице целиком, а не более свежий ОТНОСИТЕЛЬНО момента DLR.
// `submitted_at <= received_at` — DLR физически не может отчитываться о
// submission, которого ещё не было с её же точки зрения; среди подходящих
// строк `ORDER BY submitted_at DESC LIMIT 1` теперь корректно берёт САМУЮ
// БЛИЖАЙШУЮ ПРЕДШЕСТВУЮЩУЮ попытку, не самую свежую вообще.
//
// `nil, nil` — не найдено, не ошибка (обычный, ожидаемый исход при первой
// попытке до того, как DLR обогнал корреляцию).
func (s *Store) Lookup(ctx context.Context, operatorID, smscMessageID string, segmentID int32, receivedAt time.Time) (*Record, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT operator_id, smsc_message_id, segment_id, message_id, stage_execution_id, submitted_at, expires_at
		FROM dlr.dlr_correlation
		WHERE operator_id = $1 AND smsc_message_id = $2 AND segment_id = $3 AND submitted_at <= $4
		ORDER BY submitted_at DESC
		LIMIT 1
	`, operatorID, smscMessageID, segmentID, receivedAt)

	var rec Record
	err := row.Scan(&rec.OperatorID, &rec.SmscMessageID, &rec.SegmentID, &rec.MessageID, &rec.StageExecutionID, &rec.SubmittedAt, &rec.ExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("lookup dlr.dlr_correlation: %w", err)
	}
	return &rec, nil
}
