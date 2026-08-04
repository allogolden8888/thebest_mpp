// Package store — handle_status_query/handle_search_query
// (service_internal_methods.md §7.2), pgx против
// migrations/V004__message_read_model.sql (+ V005 для детализации).
// Каждый запрос обязательно фильтруется по partner_id — Partner API
// многотенантный, партнёр не должен увидеть чужие сообщения.
package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type MessageStatus struct {
	MessageID       string
	PartnerID       string
	ApplicationID   string
	TraceID         string
	PipelineID      string
	PipelineVersion string
	CurrentStatus   string
	Terminal        bool
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

type LifecycleEvent struct {
	LifecycleVersion int64
	Status           string
	EventID          string
	OccurredAt       time.Time
	Source           string
}

type SearchFilter struct {
	PartnerID     string
	ApplicationID string
	Status        string
	CreatedFrom   time.Time
	CreatedTo     time.Time
	Limit         int
	Offset        int
}

type Postgres struct {
	pool *pgxpool.Pool
}

func NewPostgres(pool *pgxpool.Pool) *Postgres {
	return &Postgres{pool: pool}
}

// Ping — используется /readyz (CODE_REVIEW.md MEDIUM finding), не запросами
// приложения.
func (p *Postgres) Ping(ctx context.Context) error {
	return p.pool.Ping(ctx)
}

func scanMessageStatus(row pgx.Row) (MessageStatus, error) {
	var m MessageStatus
	err := row.Scan(&m.MessageID, &m.PartnerID, &m.ApplicationID, &m.TraceID, &m.PipelineID,
		&m.PipelineVersion, &m.CurrentStatus, &m.Terminal, &m.CreatedAt, &m.UpdatedAt)
	return m, err
}

// StatusByMessageID — handle_status_query по message_id.
func (p *Postgres) StatusByMessageID(ctx context.Context, partnerID, messageID string) (MessageStatus, error) {
	row := p.pool.QueryRow(ctx, `
		SELECT message_id, partner_id, application_id, trace_id, pipeline_id, pipeline_version, current_status, terminal, created_at, updated_at
		FROM messaging.message_read_model
		WHERE message_id = $1 AND partner_id = $2
	`, messageID, partnerID)
	m, err := scanMessageStatus(row)
	if err != nil {
		return MessageStatus{}, fmt.Errorf("status_by_message_id: %w", err)
	}
	return m, nil
}

// StatusByTraceID — handle_status_query по trace_id (партнёр может искать
// по своему собственному коррелирующему идентификатору).
func (p *Postgres) StatusByTraceID(ctx context.Context, partnerID, traceID string) (MessageStatus, error) {
	row := p.pool.QueryRow(ctx, `
		SELECT message_id, partner_id, application_id, trace_id, pipeline_id, pipeline_version, current_status, terminal, created_at, updated_at
		FROM messaging.message_read_model
		WHERE trace_id = $1 AND partner_id = $2
	`, traceID, partnerID)
	m, err := scanMessageStatus(row)
	if err != nil {
		return MessageStatus{}, fmt.Errorf("status_by_trace_id: %w", err)
	}
	return m, nil
}

// LifecycleHistory — детализация статуса (message_lifecycle_history),
// используется handle_status_query для показа полной истории переходов.
func (p *Postgres) LifecycleHistory(ctx context.Context, partnerID, messageID string) ([]LifecycleEvent, error) {
	// message_lifecycle_history не содержит partner_id (см. migrations/V005) —
	// scoping через предварительную проверку владения в message_read_model.
	var owns bool
	err := p.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM messaging.message_read_model WHERE message_id = $1 AND partner_id = $2)`, messageID, partnerID).Scan(&owns)
	if err != nil {
		return nil, fmt.Errorf("lifecycle_history ownership check: %w", err)
	}
	if !owns {
		return nil, pgx.ErrNoRows
	}

	rows, err := p.pool.Query(ctx, `
		SELECT lifecycle_version, status, event_id, occurred_at, source
		FROM messaging.message_lifecycle_history
		WHERE message_id = $1
		ORDER BY lifecycle_version ASC
	`, messageID)
	if err != nil {
		return nil, fmt.Errorf("lifecycle_history query: %w", err)
	}
	defer rows.Close()

	var events []LifecycleEvent
	for rows.Next() {
		var e LifecycleEvent
		if err := rows.Scan(&e.LifecycleVersion, &e.Status, &e.EventID, &e.OccurredAt, &e.Source); err != nil {
			return nil, fmt.Errorf("lifecycle_history scan: %w", err)
		}
		events = append(events, e)
	}
	return events, rows.Err()
}

// Search — handle_search_query. filter.PartnerID всегда берётся из
// JWT-claims вызывающего кода, не из пользовательского ввода.
func (p *Postgres) Search(ctx context.Context, filter SearchFilter) ([]MessageStatus, error) {
	limit := filter.Limit
	if limit <= 0 || limit > 200 {
		limit = 50
	}

	query := `
		SELECT message_id, partner_id, application_id, trace_id, pipeline_id, pipeline_version, current_status, terminal, created_at, updated_at
		FROM messaging.message_read_model
		WHERE partner_id = $1
	`
	args := []interface{}{filter.PartnerID}

	if filter.ApplicationID != "" {
		args = append(args, filter.ApplicationID)
		query += fmt.Sprintf(" AND application_id = $%d", len(args))
	}
	if filter.Status != "" {
		args = append(args, filter.Status)
		query += fmt.Sprintf(" AND current_status = $%d", len(args))
	}
	if !filter.CreatedFrom.IsZero() {
		args = append(args, filter.CreatedFrom)
		query += fmt.Sprintf(" AND created_at >= $%d", len(args))
	}
	if !filter.CreatedTo.IsZero() {
		args = append(args, filter.CreatedTo)
		query += fmt.Sprintf(" AND created_at <= $%d", len(args))
	}

	args = append(args, limit)
	query += fmt.Sprintf(" ORDER BY created_at DESC LIMIT $%d", len(args))
	args = append(args, filter.Offset)
	query += fmt.Sprintf(" OFFSET $%d", len(args))

	rows, err := p.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("search query: %w", err)
	}
	defer rows.Close()

	var results []MessageStatus
	for rows.Next() {
		m, err := scanMessageStatus(rows)
		if err != nil {
			return nil, fmt.Errorf("search scan: %w", err)
		}
		results = append(results, m)
	}
	return results, rows.Err()
}
