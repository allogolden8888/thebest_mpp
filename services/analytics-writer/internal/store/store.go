// Package store — batch_buffer + flush_batch + refresh_materialized_view
// (service_internal_methods.md §6.2), clickhouse-go/v2.
//
// **Открытый вопрос**: ни один документ этой сессии (data_infrastructure_spec.md
// не содержит раздела ClickHouse) не специфицирует схему таблиц ClickHouse —
// эта схема (analytics.stage_events + materialized view для почасовых
// агрегатов) спроектирована здесь как рабочее предположение, см. README.
package store

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"mpp/analytics-writer/internal/core"
)

// ENGINE = ReplacingMergeTree() — CODE_REVIEW.md finding: раньше
// MergeTree() без версии/dedup-ключа, а at-least-once Kafka redelivery
// (обычное дело на rebalance/restart) вставляла одно и то же событие
// второй раз как новую строку, молча раздувая count()-агрегаты, которые
// Backoffice/Partner API report-запросы строят прямо по этой таблице.
// ReplacingMergeTree дедуплицирует строки с одинаковым ORDER BY
// (occurred_at, event_id, message_id) в фоне при merge — НЕ синхронно
// на INSERT. Чтобы читатели видели корректные агрегаты немедленно, а не
// только после случайного merge, запросы обязаны читать таблицу с
// модификатором FINAL (см. backoffice-api/partner-api
// internal/store/clickhouse.go Report) — задокументированная,
// стандартная для ClickHouse цена корректности здесь: FINAL заставляет
// мерджить на чтении, дороже обычного SELECT.
const createTableDDL = `
CREATE TABLE IF NOT EXISTS analytics.stage_events (
	event_type       String,
	event_id         String,
	message_id       String,
	partner_id       String,
	stage_name       String,
	outcome          String,
	reason_code      String,
	lifecycle_status String,
	occurred_at      DateTime64(3),
	ingested_at      DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree()
ORDER BY (occurred_at, event_id, message_id)
`

// stage_events_hourly_mv — НЕ читается ни одним известным потребителем в
// этом репозитории (backoffice-api/partner-api Report-запросы читают
// analytics.stage_events напрямую, с собственным GROUP BY — grep
// подтверждает, что stage_events_hourly_mv нигде не упоминается вне этого
// файла). Оставлена как есть — предвычисленный агрегат для будущего
// использования, — но её собственный dedup-статус НЕ исправлен этим
// срезом: incremental materialized view в ClickHouse триггерится per
// INSERT-блок и не знает о будущих merge-time дедупликациях базовой
// ReplacingMergeTree-таблицы, поэтому count() здесь может по-прежнему
// задваиваться на редоставленных событиях, даже после исправления самой
// таблицы. Если этот MV когда-нибудь станет реально читаемым, его нужно
// будет пересобрать (например AggregatingMergeTree + uniqExact(event_id)
// вместо count()) — не сделано здесь, так как сейчас это мёртвый код.
const createHourlyAggDDL = `
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.stage_events_hourly_mv
ENGINE = SummingMergeTree()
ORDER BY (hour, stage_name, outcome)
POPULATE
AS SELECT
	toStartOfHour(occurred_at) AS hour,
	stage_name,
	outcome,
	count() AS event_count
FROM analytics.stage_events
WHERE event_type = 'stage_completed'
GROUP BY hour, stage_name, outcome
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

// EnsureSchema — создаёт таблицу + materialized view, если их ещё нет
// (подготовка аналитических таблиц/агрегатов, service_internal_methods.md §6.2).
func (s *Store) EnsureSchema(ctx context.Context) error {
	if err := s.conn.Exec(ctx, "CREATE DATABASE IF NOT EXISTS analytics"); err != nil {
		return fmt.Errorf("create database: %w", err)
	}
	if err := s.conn.Exec(ctx, createTableDDL); err != nil {
		return fmt.Errorf("create table: %w", err)
	}
	if err := s.conn.Exec(ctx, createHourlyAggDDL); err != nil {
		return fmt.Errorf("create materialized view: %w", err)
	}
	return nil
}

// FlushBatch — batch insert через ClickHouse native batch API.
func (s *Store) FlushBatch(ctx context.Context, records []core.NormalizedRecord) error {
	if len(records) == 0 {
		return nil
	}
	batch, err := s.conn.PrepareBatch(ctx, "INSERT INTO analytics.stage_events (event_type, event_id, message_id, partner_id, stage_name, outcome, reason_code, lifecycle_status, occurred_at)")
	if err != nil {
		return fmt.Errorf("prepare batch: %w", err)
	}
	for _, r := range records {
		if err := batch.Append(r.EventType, r.EventID, r.MessageID, r.PartnerID, r.StageName, r.Outcome, r.ReasonCode, r.LifecycleStatus, r.OccurredAt); err != nil {
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