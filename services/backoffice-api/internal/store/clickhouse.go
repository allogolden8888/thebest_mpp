// Package store — handle_report_query (service_internal_methods.md §7.3),
// clickhouse-go/v2 против той же схемы, что analytics-writer создал
// (analytics.stage_events) — см. services/analytics-writer/README.md
// "Открытый вопрос" и services/partner-api/internal/store/clickhouse.go
// (тот же приём, здесь без обязательного partner_id — Backoffice API
// видит все партнёры, партнёр — опциональный фильтр для оператора).
package store

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type ReportRow struct {
	Hour       time.Time
	PartnerID  string
	StageName  string
	Outcome    string
	EventCount uint64
}

type ClickHouse struct {
	conn driver.Conn
}

func NewClickHouse(addr, database, username, password string) (*ClickHouse, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{Database: database, Username: username, Password: password},
	})
	if err != nil {
		return nil, fmt.Errorf("clickhouse.Open: %w", err)
	}
	return &ClickHouse{conn: conn}, nil
}

func NewClickHouseFromConn(conn driver.Conn) *ClickHouse {
	return &ClickHouse{conn: conn}
}

// Ping — используется /readyz для реальной проверки состояния зависимости
// (CODE_REVIEW.md MEDIUM finding), а не для запросов приложения.
func (c *ClickHouse) Ping(ctx context.Context) error {
	return c.conn.Ping(ctx)
}

type ReportFilter struct {
	PartnerID string
	From      time.Time
	To        time.Time
	StageName string
}

func (c *ClickHouse) Report(ctx context.Context, filter ReportFilter) ([]ReportRow, error) {
	query := `
		SELECT toStartOfHour(occurred_at) AS hour, partner_id, stage_name, outcome, count() AS event_count
		FROM analytics.stage_events
		WHERE event_type = 'stage_completed'
	`
	var args []interface{}
	if filter.PartnerID != "" {
		query += " AND partner_id = ?"
		args = append(args, filter.PartnerID)
	}
	if !filter.From.IsZero() {
		query += " AND occurred_at >= ?"
		args = append(args, filter.From)
	}
	if !filter.To.IsZero() {
		query += " AND occurred_at <= ?"
		args = append(args, filter.To)
	}
	if filter.StageName != "" {
		query += " AND stage_name = ?"
		args = append(args, filter.StageName)
	}
	query += " GROUP BY hour, partner_id, stage_name, outcome ORDER BY hour ASC"

	rows, err := c.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("report query: %w", err)
	}
	defer rows.Close()

	var results []ReportRow
	for rows.Next() {
		var r ReportRow
		if err := rows.Scan(&r.Hour, &r.PartnerID, &r.StageName, &r.Outcome, &r.EventCount); err != nil {
			return nil, fmt.Errorf("report scan: %w", err)
		}
		results = append(results, r)
	}
	return results, rows.Err()
}