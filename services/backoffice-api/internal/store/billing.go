// Package store, файл billing.go — чтение billing.billing_ledger и
// dlr.dlr_correlation для админских экранов "Биллинг" и блока
// "операторские события" в карточке сообщения (BACKOFFICE_DESIGN_SPEC.md
// разделы 3C и 5). Тот же принцип прямого чтения чужих схем, что уже
// принят для DlqBrowse/ReconciliationBrowse (см. package doc postgres.go).
package store

import (
	"context"
	"fmt"
	"time"
)

type LedgerEntry struct {
	ID             int64
	ChargeID       string
	AccountID      string
	PartnerID      string
	Amount         string // numeric(18,4) — строкой, не float: деньги
	Currency       string
	EntryType      string // charge | compensating
	SourceChargeID *string
	CreatedAt      time.Time
}

type LedgerFilter struct {
	PartnerID string
	EntryType string
	From      *time.Time
	To        *time.Time
	Limit     int
	Offset    int
}

// LedgerBrowse — лента списаний. amount читается как string: numeric(18,4)
// в float64 терял бы точность на деньгах, а pgx отдаёт его в
// pgtype.Numeric — строка здесь честнее и для JSON тоже.
func (p *Postgres) LedgerBrowse(ctx context.Context, f LedgerFilter) ([]LedgerEntry, error) {
	query := `
		SELECT id, charge_id, account_id, partner_id, amount::text, currency,
		       entry_type, source_charge_id, created_at
		FROM billing.billing_ledger
		WHERE true
	`
	var args []interface{}
	if f.PartnerID != "" {
		args = append(args, f.PartnerID)
		query += fmt.Sprintf(" AND partner_id = $%d", len(args))
	}
	if f.EntryType != "" {
		args = append(args, f.EntryType)
		query += fmt.Sprintf(" AND entry_type = $%d", len(args))
	}
	if f.From != nil {
		args = append(args, *f.From)
		query += fmt.Sprintf(" AND created_at >= $%d", len(args))
	}
	if f.To != nil {
		args = append(args, *f.To)
		query += fmt.Sprintf(" AND created_at < $%d", len(args))
	}
	args = append(args, clampLimit(f.Limit))
	query += fmt.Sprintf(" ORDER BY created_at DESC LIMIT $%d", len(args))
	args = append(args, f.Offset)
	query += fmt.Sprintf(" OFFSET $%d", len(args))

	rows, err := p.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("ledger_browse query: %w", err)
	}
	defer rows.Close()

	var out []LedgerEntry
	for rows.Next() {
		var e LedgerEntry
		if err := rows.Scan(&e.ID, &e.ChargeID, &e.AccountID, &e.PartnerID, &e.Amount,
			&e.Currency, &e.EntryType, &e.SourceChargeID, &e.CreatedAt); err != nil {
			return nil, fmt.Errorf("ledger_browse scan: %w", err)
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

type LedgerSummaryRow struct {
	Key           string
	Charges       string
	Compensations string
	Net           string
	EntryCount    int64
}

// LedgerSummary — агрегат по партнёру или по дню. group_by валидируется
// вызывающей стороной (httpapi), не интерполируется из запроса как есть —
// это единственное место здесь, где значение попадает в SQL не
// параметром, поэтому допустимые значения перечислены явно.
func (p *Postgres) LedgerSummary(ctx context.Context, groupBy string, from, to *time.Time) ([]LedgerSummaryRow, string, error) {
	var keyExpr string
	switch groupBy {
	case "partner":
		keyExpr = "partner_id"
	case "day":
		keyExpr = "to_char(date_trunc('day', created_at), 'YYYY-MM-DD')"
	default:
		return nil, "", fmt.Errorf("недопустимый group_by=%q (ожидалось partner|day)", groupBy)
	}

	query := fmt.Sprintf(`
		SELECT %s AS k,
		       COALESCE(SUM(amount) FILTER (WHERE entry_type = 'charge'), 0)::text,
		       COALESCE(SUM(amount) FILTER (WHERE entry_type = 'compensating'), 0)::text,
		       -- net = списания МИНУС компенсации. Компенсации лежат в
		       -- ledger положительными числами (entry_type их и различает,
		       -- не знак), поэтому простой SUM(amount) дал бы 100+30=130
		       -- вместо 100-30=70 — то есть завышал бы выручку ровно на
		       -- сумму возвратов.
		       (COALESCE(SUM(amount) FILTER (WHERE entry_type = 'charge'), 0)
		        - COALESCE(SUM(amount) FILTER (WHERE entry_type = 'compensating'), 0))::text,
		       count(*)
		FROM billing.billing_ledger
		WHERE true
	`, keyExpr)
	var args []interface{}
	if from != nil {
		args = append(args, *from)
		query += fmt.Sprintf(" AND created_at >= $%d", len(args))
	}
	if to != nil {
		args = append(args, *to)
		query += fmt.Sprintf(" AND created_at < $%d", len(args))
	}
	query += fmt.Sprintf(" GROUP BY %s ORDER BY 1", keyExpr)

	rows, err := p.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, "", fmt.Errorf("ledger_summary query: %w", err)
	}
	defer rows.Close()

	var out []LedgerSummaryRow
	for rows.Next() {
		var r LedgerSummaryRow
		if err := rows.Scan(&r.Key, &r.Charges, &r.Compensations, &r.Net, &r.EntryCount); err != nil {
			return nil, "", fmt.Errorf("ledger_summary scan: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}

	// Валюта — из тех же строк, что попали в агрегат. Платформа сегодня
	// одновалютная (UZS в billing_tariff), поэтому отдельная колонка в
	// агрегате была бы избыточной, но зашивать "UZS" константой в API
	// нельзя — читаем фактическую.
	var currency string
	if err := p.pool.QueryRow(ctx, `SELECT currency FROM billing.billing_ledger LIMIT 1`).Scan(&currency); err != nil {
		currency = ""
	}
	return out, currency, nil
}

// LedgerByMessage — списания по конкретному сообщению. Связь: charge_id
// стадии BILLING == stage_execution_id этой стадии, а он лежит в
// dlr.dlr_correlation только для DELIVERY. Прямой связи message_id ->
// charge_id в ledger нет, поэтому ищем по account_id+времени нельзя —
// достоверно доступен только путь через stage_execution_id, который знает
// вызывающая сторона. Здесь принимаем его явно.
func (p *Postgres) LedgerByChargeIDs(ctx context.Context, chargeIDs []string) ([]LedgerEntry, error) {
	if len(chargeIDs) == 0 {
		return nil, nil
	}
	rows, err := p.pool.Query(ctx, `
		SELECT id, charge_id, account_id, partner_id, amount::text, currency,
		       entry_type, source_charge_id, created_at
		FROM billing.billing_ledger
		WHERE charge_id = ANY($1)
		ORDER BY created_at
	`, chargeIDs)
	if err != nil {
		return nil, fmt.Errorf("ledger_by_charge_ids query: %w", err)
	}
	defer rows.Close()

	var out []LedgerEntry
	for rows.Next() {
		var e LedgerEntry
		if err := rows.Scan(&e.ID, &e.ChargeID, &e.AccountID, &e.PartnerID, &e.Amount,
			&e.Currency, &e.EntryType, &e.SourceChargeID, &e.CreatedAt); err != nil {
			return nil, fmt.Errorf("ledger_by_charge_ids scan: %w", err)
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

type OperatorSubmitRecord struct {
	OperatorID       string
	SmscMessageID    string
	SegmentID        int32
	StageExecutionID string
	SubmittedAt      time.Time
	ExpiresAt        time.Time
}

// OperatorEventsByMessage — SMPP-сторона по сообщению: что и когда реально
// ушло оператору (dlr.dlr_correlation, которую пишет
// dlr-correlation-writer из operator.submit.accepted).
//
// Чего здесь НЕТ и почему: сам текст DLR-receipt'а ("id:... stat:DELIVRD
// ...") нигде не персистится — OperatorDlr несёт только код статуса
// (raw_status), а полного текста в proto-контракте нет поля вообще.
// Момент прихода DLR виден косвенно — как переход в DELIVERED в
// messaging.message_lifecycle_history (см. GetMessageDetail).
func (p *Postgres) OperatorEventsByMessage(ctx context.Context, messageID string) ([]OperatorSubmitRecord, error) {
	rows, err := p.pool.Query(ctx, `
		SELECT operator_id, smsc_message_id, segment_id, stage_execution_id, submitted_at, expires_at
		FROM dlr.dlr_correlation
		WHERE message_id = $1
		ORDER BY segment_id, submitted_at
	`, messageID)
	if err != nil {
		return nil, fmt.Errorf("operator_events query: %w", err)
	}
	defer rows.Close()

	var out []OperatorSubmitRecord
	for rows.Next() {
		var r OperatorSubmitRecord
		if err := rows.Scan(&r.OperatorID, &r.SmscMessageID, &r.SegmentID,
			&r.StageExecutionID, &r.SubmittedAt, &r.ExpiresAt); err != nil {
			return nil, fmt.Errorf("operator_events scan: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
