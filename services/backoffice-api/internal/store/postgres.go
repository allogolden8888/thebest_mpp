// Package store — handle_dlq_browse/handle_reconciliation_browse
// (service_internal_methods.md §7.3), pgx против
// migrations/V006__dlq_record.sql, V010__reconciliation_cases.sql. В отличие
// от Partner API, здесь нет per-partner scoping — Backoffice API
// административный, оператор видит все записи.
package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type DlqRecord struct {
	StageExecutionID string
	MessageID        string
	StageName        string
	Attempt          int32
	ReasonCode       string
	ErrorDetail      string
	CreatedAt        time.Time
	ReplayStatus     string
}

type DlqFilter struct {
	StageName    string
	ReplayStatus string
	Limit        int
	Offset       int
}

type ReconciliationCase struct {
	CaseID            string
	MessageID         string
	StageExecutionID  string
	OperatorID        string
	Status            string
	OpenedAt          time.Time
	ResolvedAt        *time.Time
	DeadlineAt        time.Time
}

type ReconciliationFilter struct {
	Status     string
	OperatorID string
	Limit      int
	Offset     int
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

func clampLimit(limit int) int {
	if limit <= 0 || limit > 200 {
		return 50
	}
	return limit
}

// DlqBrowse — handle_dlq_browse: фильтры -> DlqRecords.
func (p *Postgres) DlqBrowse(ctx context.Context, filter DlqFilter) ([]DlqRecord, error) {
	query := `
		SELECT stage_execution_id, message_id, stage_name, attempt, reason_code, error_detail, created_at, replay_status
		FROM messaging.dlq_record
		WHERE true
	`
	var args []interface{}
	if filter.StageName != "" {
		args = append(args, filter.StageName)
		query += fmt.Sprintf(" AND stage_name = $%d", len(args))
	}
	if filter.ReplayStatus != "" {
		args = append(args, filter.ReplayStatus)
		query += fmt.Sprintf(" AND replay_status = $%d", len(args))
	}
	args = append(args, clampLimit(filter.Limit))
	query += fmt.Sprintf(" ORDER BY created_at DESC LIMIT $%d", len(args))
	args = append(args, filter.Offset)
	query += fmt.Sprintf(" OFFSET $%d", len(args))

	rows, err := p.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("dlq_browse query: %w", err)
	}
	defer rows.Close()

	var results []DlqRecord
	for rows.Next() {
		var r DlqRecord
		var errorDetail *string // migrations/V006__dlq_record.sql: error_detail TEXT, nullable
		if err := rows.Scan(&r.StageExecutionID, &r.MessageID, &r.StageName, &r.Attempt, &r.ReasonCode, &errorDetail, &r.CreatedAt, &r.ReplayStatus); err != nil {
			return nil, fmt.Errorf("dlq_browse scan: %w", err)
		}
		if errorDetail != nil {
			r.ErrorDetail = *errorDetail
		}
		results = append(results, r)
	}
	return results, rows.Err()
}

// ReconciliationBrowse — handle_reconciliation_browse: фильтры -> ReconciliationCases.
func (p *Postgres) ReconciliationBrowse(ctx context.Context, filter ReconciliationFilter) ([]ReconciliationCase, error) {
	query := `
		SELECT case_id, message_id, stage_execution_id, operator_id, status, opened_at, resolved_at, deadline_at
		FROM reconciliation.reconciliation_cases
		WHERE true
	`
	var args []interface{}
	if filter.Status != "" {
		args = append(args, filter.Status)
		query += fmt.Sprintf(" AND status = $%d", len(args))
	}
	if filter.OperatorID != "" {
		args = append(args, filter.OperatorID)
		query += fmt.Sprintf(" AND operator_id = $%d", len(args))
	}
	args = append(args, clampLimit(filter.Limit))
	query += fmt.Sprintf(" ORDER BY opened_at DESC LIMIT $%d", len(args))
	args = append(args, filter.Offset)
	query += fmt.Sprintf(" OFFSET $%d", len(args))

	rows, err := p.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("reconciliation_browse query: %w", err)
	}
	defer rows.Close()

	var results []ReconciliationCase
	for rows.Next() {
		var r ReconciliationCase
		if err := rows.Scan(&r.CaseID, &r.MessageID, &r.StageExecutionID, &r.OperatorID, &r.Status, &r.OpenedAt, &r.ResolvedAt, &r.DeadlineAt); err != nil {
			return nil, fmt.Errorf("reconciliation_browse scan: %w", err)
		}
		results = append(results, r)
	}
	return results, rows.Err()
}