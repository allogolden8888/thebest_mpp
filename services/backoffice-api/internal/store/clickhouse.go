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
	"strings"
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

// FINAL — analytics-writer/CODE_REVIEW.md finding: analytics.stage_events
// is a ReplacingMergeTree keyed on (occurred_at, event_id, message_id) —
// at-least-once Kafka redelivery of the same event can leave duplicate
// rows until a background merge collapses them. FINAL forces merge-on-read
// so count() here isn't inflated by not-yet-merged duplicates (real cost:
// more expensive than a plain SELECT — acceptable for a low-QPS reporting
// endpoint, see services/analytics-writer/internal/store/store.go).
func (c *ClickHouse) Report(ctx context.Context, filter ReportFilter) ([]ReportRow, error) {
	query := `
		SELECT toStartOfHour(occurred_at) AS hour, partner_id, stage_name, outcome, count() AS event_count
		FROM analytics.stage_events FINAL
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

// StageTimelineEntry — одна стадия в хронологии сообщения.
type StageTimelineEntry struct {
	StageName  string
	Outcome    string
	ReasonCode string
	OccurredAt time.Time
}

// MessageStageTimeline — пер-стадийная хронология одного сообщения:
// "на какой стадии сколько провело". Длительность стадии вычисляется
// вызывающей стороной как разница между соседними occurred_at — здесь
// намеренно возвращаются сырые точки, а не готовые дельты: у первой
// стадии нет предшественника, и что считать её началом (приём сообщения
// или её собственное завершение) — решение уровня API, не хранилища.
//
// FINAL — та же цена корректности, что и в Report выше: stage_events это
// ReplacingMergeTree, без FINAL redelivered-дубликаты дали бы лишние
// точки в хронологии одного сообщения.
func (c *ClickHouse) MessageStageTimeline(ctx context.Context, messageID string) ([]StageTimelineEntry, error) {
	rows, err := c.conn.Query(ctx, `
		SELECT stage_name, outcome, reason_code, occurred_at
		FROM analytics.stage_events FINAL
		WHERE message_id = ? AND event_type = 'stage_completed'
		ORDER BY occurred_at ASC
	`, messageID)
	if err != nil {
		return nil, fmt.Errorf("message_stage_timeline query: %w", err)
	}
	defer rows.Close()

	var out []StageTimelineEntry
	for rows.Next() {
		var e StageTimelineEntry
		if err := rows.Scan(&e.StageName, &e.Outcome, &e.ReasonCode, &e.OccurredAt); err != nil {
			return nil, fmt.Errorf("message_stage_timeline scan: %w", err)
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// PduLogEntry — одна строка analytics.operator_pdu_log (Экраны 38-40,
// pdu-log-writer/internal/store/store.go createTableDDL): один реальный
// SMPP PDU (submit_sm/submit_sm_resp для A2P, deliver_sm/deliver_sm_resp
// для DLR), а не агрегат по стадии, как MessageStageTimeline выше.
type PduLogEntry struct {
	Direction        string
	PduType          string
	SequenceNumber   int32
	Protocol         string
	MessageID        string
	StageExecutionID string
	SmscMessageID    string
	SegmentID        int32
	Status           string
	OccurredAt       time.Time
}

// MessagePduLog — пер-PDU лог для одного сообщения. A2P-направление
// (submit_sm/_resp) несёт message_id напрямую, но DLR-направление
// (deliver_sm/_resp) — нет (см. OperatorPduLog.message_id doc-комментарий
// в platform-contracts/events/operator_events.proto: единственный ключ
// там smsc_message_id). Поэтому вызывающая сторона (httpapi/pdulog.go)
// сперва резолвит smsc_message_id этого сообщения через
// dlr.dlr_correlation (Postgres.OperatorEventsByMessage) и передаёт их
// сюда — читаем ИЛИ по message_id, ИЛИ по smsc_message_id, объединяя обе
// направления в одну ленту.
//
// FINAL — та же цена корректности, что и в MessageStageTimeline: таблица
// ReplacingMergeTree, без FINAL at-least-once redelivery от Kafka дала бы
// дубликаты PDU в ленте одного сообщения.
func (c *ClickHouse) MessagePduLog(ctx context.Context, messageID string, smscMessageIDs []string) ([]PduLogEntry, error) {
	conditions := []string{"message_id = ?"}
	args := []interface{}{messageID}
	if len(smscMessageIDs) > 0 {
		placeholders := make([]string, len(smscMessageIDs))
		for i, id := range smscMessageIDs {
			placeholders[i] = "?"
			args = append(args, id)
		}
		conditions = append(conditions, fmt.Sprintf("smsc_message_id IN (%s)", strings.Join(placeholders, ", ")))
	}

	query := fmt.Sprintf(`
		SELECT direction, pdu_type, sequence_number, protocol, message_id, stage_execution_id,
		       smsc_message_id, segment_id, status, occurred_at
		FROM analytics.operator_pdu_log FINAL
		WHERE (%s)
		ORDER BY occurred_at ASC, sequence_number ASC
	`, strings.Join(conditions, " OR "))

	rows, err := c.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("message_pdu_log query: %w", err)
	}
	defer rows.Close()

	var out []PduLogEntry
	for rows.Next() {
		var e PduLogEntry
		if err := rows.Scan(&e.Direction, &e.PduType, &e.SequenceNumber, &e.Protocol, &e.MessageID,
			&e.StageExecutionID, &e.SmscMessageID, &e.SegmentID, &e.Status, &e.OccurredAt); err != nil {
			return nil, fmt.Errorf("message_pdu_log scan: %w", err)
		}
		out = append(out, e)
	}
	return out, rows.Err()
}
