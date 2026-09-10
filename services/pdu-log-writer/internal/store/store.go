// Package store — batch insert per-PDU логов в ClickHouse
// (BACKOFFICE_DESIGN_SPEC.md Экраны 38-40). Отдельная таблица от
// analytics.stage_events (analytics-writer) в ТОЙ ЖЕ базе analytics —
// разная форма строки, общий backing store, тот же выбор, что уже
// работает для nескольких таблиц в одной ClickHouse-инсталляции.
package store

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"mpp/pdu-log-writer/internal/core"
)

// ENGINE = ReplacingMergeTree() — тот же dedup-принцип, что
// analytics.stage_events (analytics-writer/internal/store/store.go):
// at-least-once Kafka redelivery не должна задваивать строки. ORDER BY
// включает sequence_number (переиспользуется по кругу внутри сессии, но
// в паре с occurred_at/pdu_type/operator_id коллизия НЕ redelivery
// исключена) — читатели (backoffice-api) обязаны использовать FINAL,
// как и Report-запросы в analytics-writer/backoffice-api.
const createTableDDL = `
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
`

type Store struct {
	conn driver.Conn
}

func New(addr, database, username, password string) (*Store, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{Database: database, Username: username, Password: password},
	})
	if err != nil {
		return nil, fmt.Errorf("clickhouse.Open: %w", err)
	}
	return &Store{conn: conn}, nil
}

func NewFromConn(conn driver.Conn) *Store {
	return &Store{conn: conn}
}

// EnsureSchema — создаёт БД (общую с analytics-writer) + таблицу, если их
// ещё нет. Идемпотентно — безопасно вызывать при каждом старте.
func (s *Store) EnsureSchema(ctx context.Context) error {
	if err := s.conn.Exec(ctx, "CREATE DATABASE IF NOT EXISTS analytics"); err != nil {
		return fmt.Errorf("create database: %w", err)
	}
	if err := s.conn.Exec(ctx, createTableDDL); err != nil {
		return fmt.Errorf("create table: %w", err)
	}
	return nil
}

// FlushBatch — batch insert через ClickHouse native batch API (тот же
// паттерн, что analytics-writer/internal/store/store.go).
func (s *Store) FlushBatch(ctx context.Context, records []core.PduLogRecord) error {
	if len(records) == 0 {
		return nil
	}
	batch, err := s.conn.PrepareBatch(ctx, "INSERT INTO analytics.operator_pdu_log "+
		"(operator_id, protocol, direction, pdu_type, sequence_number, message_id, stage_execution_id, smsc_message_id, segment_id, status, occurred_at)")
	if err != nil {
		return fmt.Errorf("prepare batch: %w", err)
	}
	for _, r := range records {
		if err := batch.Append(r.OperatorID, r.Protocol, r.Direction, r.PduType, r.SequenceNumber,
			r.MessageID, r.StageExecutionID, r.SmscMessageID, r.SegmentID, r.Status, r.OccurredAt); err != nil {
			return fmt.Errorf("append to batch: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("send batch: %w", err)
	}
	return nil
}

func (s *Store) Close() error {
	return s.conn.Close()
}
