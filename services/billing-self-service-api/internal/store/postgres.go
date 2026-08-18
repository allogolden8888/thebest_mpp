// Package store — прямое чтение billing.billing_ledger
// (migrations/V008__billing_ledger.sql), тот же паттерн, что
// services/partner-api/internal/store/postgres.go: каждый запрос
// обязательно фильтруется по partner_id вызывающего — billing-self-service-api
// многотенантный, партнёр не должен увидеть чужие списания.
//
// account_id = partner_id с Фазы 5a (multi-tenancy в billing-service) —
// фильтр по partner_id здесь корректен и соответствует реальному
// account_id, под которым billing-service действительно списывает.
package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// LedgerEntry.Amount — NUMERIC(18,4) читается как строка (amount::text в
// запросе), не float64: деньги не должны терять точность через
// binary-float округление на пути Postgres -> Go -> JSON.
type LedgerEntry struct {
	ID             int64     `json:"id"`
	ChargeID       string    `json:"charge_id"`
	AccountID      string    `json:"account_id"`
	PartnerID      string    `json:"partner_id"`
	Amount         string    `json:"amount"`
	Currency       string    `json:"currency"`
	EntryType      string    `json:"entry_type"`
	SourceChargeID *string   `json:"source_charge_id,omitempty"`
	CreatedAt      time.Time `json:"created_at"`
}

type LedgerFilter struct {
	PartnerID   string
	CreatedFrom time.Time
	CreatedTo   time.Time
	Limit       int
	Offset      int
}

type Postgres struct {
	pool *pgxpool.Pool
}

func NewPostgres(pool *pgxpool.Pool) *Postgres {
	return &Postgres{pool: pool}
}

func (p *Postgres) Ping(ctx context.Context) error {
	return p.pool.Ping(ctx)
}

// Ledger — постранично, всегда фильтровано по filter.PartnerID (из JWT,
// никогда из query-параметра запроса).
func (p *Postgres) Ledger(ctx context.Context, filter LedgerFilter) ([]LedgerEntry, error) {
	limit := filter.Limit
	if limit <= 0 || limit > 200 {
		limit = 50
	}

	query := `
		SELECT id, charge_id, account_id, partner_id, amount::text, currency, entry_type, source_charge_id, created_at
		FROM billing.billing_ledger
		WHERE partner_id = $1
	`
	args := []interface{}{filter.PartnerID}

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
		return nil, fmt.Errorf("ledger query: %w", err)
	}
	defer rows.Close()

	var entries []LedgerEntry
	for rows.Next() {
		var e LedgerEntry
		if err := rows.Scan(&e.ID, &e.ChargeID, &e.AccountID, &e.PartnerID, &e.Amount, &e.Currency, &e.EntryType, &e.SourceChargeID, &e.CreatedAt); err != nil {
			return nil, fmt.Errorf("ledger scan: %w", err)
		}
		entries = append(entries, e)
	}
	return entries, rows.Err()
}

// SpendSummary — сумма charge минус compensating за период, group by currency
// (billing_ledger не гарантирует одну валюту на партнёра — CHAR(3) per row).
type SpendSummary struct {
	Currency string `json:"currency"`
	Total    string `json:"total"`
}

func (p *Postgres) SpendSummary(ctx context.Context, partnerID string, from, to time.Time) ([]SpendSummary, error) {
	query := `
		SELECT currency,
			(SUM(CASE WHEN entry_type = 'charge' THEN amount ELSE 0 END)
			 - SUM(CASE WHEN entry_type = 'compensating' THEN amount ELSE 0 END))::text AS total
		FROM billing.billing_ledger
		WHERE partner_id = $1 AND created_at >= $2 AND created_at <= $3
		GROUP BY currency
	`
	rows, err := p.pool.Query(ctx, query, partnerID, from, to)
	if err != nil {
		return nil, fmt.Errorf("spend summary query: %w", err)
	}
	defer rows.Close()

	var out []SpendSummary
	for rows.Next() {
		var s SpendSummary
		if err := rows.Scan(&s.Currency, &s.Total); err != nil {
			return nil, fmt.Errorf("spend summary scan: %w", err)
		}
		out = append(out, s)
	}
	return out, rows.Err()
}
