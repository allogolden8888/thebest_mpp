// Package store — handle_report_query (service_internal_methods.md §7.2),
// clickhouse-go/v2 против той же схемы, что analytics-writer создал
// (analytics.stage_events + stage_events_hourly_mv) — см.
// services/analytics-writer/README.md "Открытый вопрос": схема ClickHouse
// не специфицирована ни в одном документе, спроектирована на стороне
// Analytics Writer, здесь переиспользуется как есть, не изобретается заново.
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

// Report — handle_report_query: почасовые агрегаты по стадии/исходу за
// период. partner_id не хранится в analytics.stage_events_hourly_mv (агрегат
// уже свёрнут по всем партнёрам, см. analytics-writer/internal/store/store.go)
// — партнёрский срез строится напрямую из analytics.stage_events (сырые
// строки), не из MV. Открытый вопрос по этому расхождению — см. README.
func (c *ClickHouse) Report(ctx context.Context, filter ReportFilter) ([]ReportRow, error) {
	query := `
		SELECT toStartOfHour(occurred_at) AS hour, stage_name, outcome, count() AS event_count
		FROM analytics.stage_events
		WHERE event_type = 'stage_completed' AND partner_id = ?
	`
	args := []interface{}{filter.PartnerID}

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
	query += " GROUP BY hour, stage_name, outcome ORDER BY hour ASC"

	rows, err := c.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("report query: %w", err)
	}
	defer rows.Close()

	var results []ReportRow
	for rows.Next() {
		var r ReportRow
		if err := rows.Scan(&r.Hour, &r.StageName, &r.Outcome, &r.EventCount); err != nil {
			return nil, fmt.Errorf("report scan: %w", err)
		}
		results = append(results, r)
	}
	return results, rows.Err()
}