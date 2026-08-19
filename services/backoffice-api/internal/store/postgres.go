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
	CaseID           string
	MessageID        string
	StageExecutionID string
	OperatorID       string
	Status           string
	OpenedAt         time.Time
	ResolvedAt       *time.Time
	DeadlineAt       time.Time
}

type ReconciliationFilter struct {
	Status     string
	OperatorID string
	Limit      int
	Offset     int
}

// AuditEntry — одна нормализованная строка объединённого Audit Log
// (GET /v1/audit, luminous-hugging-charm.md Фаза 0). Source называет
// исходную таблицу — actor/action/target у каждого источника со своим
// набором колонок, замаппленных на этот общий вид в AuditBrowse.
type AuditEntry struct {
	Source    string
	Actor     string
	Action    string
	Target    string
	CreatedAt time.Time
}

type AuditFilter struct {
	// Source — "" мержит все четыре источника; иначе фильтр по одному
	// ("replay"/"execution_control"/"billing_reconciliation"/"identity").
	Source string
	Limit  int
	Offset int
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

// AuditBrowse — GET /v1/audit: объединяет четыре *_audit таблицы этой
// сессии в один нормализованный лог. Прямой SQL против чужих схем (как
// DlqBrowse/ReconciliationBrowse выше), не gRPC — эти таблицы уже читаются
// напрямую из backoffice-api по тому же принципу "административный обзор,
// не транзакционная граница сервиса-владельца".
//
// Маппинг колонок в общий вид (actor/action/target) — по факту разных схем
// исходных таблиц, не единый контракт:
//   - messaging.replay_audit (V007): requested_by -> actor, outcome ->
//     action (например "ACCEPTED"/rejection reason), stage_execution_id ->
//     target.
//   - control.execution_control_audit (V016): requested_by -> actor,
//     state -> action, "scope[:scope_id]" -> target (scope_id может быть
//     пустым для GLOBAL-скоупа).
//   - billing.reconciliation_audit (V021): у таблицы НЕТ колонки-актёра —
//     это полностью автоматический процесс (billing-reconciliation-service
//     сам решает freeze/unfreeze по дрейфу баланса, ни один человек не
//     инициирует запись), поэтому actor — литерал
//     "billing-reconciliation-service", не NULL и не пустая строка (чтобы
//     UI мог показать источник единообразно с человеческими source). action
//     -> action column (NO_ACTION/FREEZE/UNFREEZE/...), account_id ->
//     target.
//   - iam.identity_audit (V025): actor/action/target уже 1:1 в исходной
//     схеме (append-only, тот же формат, что и остальные *_audit таблицы
//     сессии).
//
// source-фильтр применяется в ВНЕШНЕМ WHERE поверх UNION ALL, а не через
// условное построение запроса по ветвям — четыре простых SELECT без
// собственных фильтров проще проверить на корректность, чем динамическую
// сборку подмножества UNION-веток, а PostgreSQL planner в состоянии
// исключить непрошенные ветки по константному предикату source = $1 без
// обращения к остальным трём таблицам.
func (p *Postgres) AuditBrowse(ctx context.Context, filter AuditFilter) ([]AuditEntry, bool, error) {
	limit := clampLimit(filter.Limit)
	query := `
		SELECT source, actor, action, target, created_at FROM (
			SELECT 'replay' AS source, requested_by AS actor, outcome AS action,
			       stage_execution_id::text AS target, requested_at AS created_at
			FROM messaging.replay_audit
			UNION ALL
			SELECT 'execution_control' AS source, requested_by AS actor, state AS action,
			       CASE WHEN scope_id = '' THEN scope ELSE scope || ':' || scope_id END AS target,
			       created_at
			FROM control.execution_control_audit
			UNION ALL
			SELECT 'billing_reconciliation' AS source, 'billing-reconciliation-service' AS actor,
			       action, account_id AS target, created_at
			FROM billing.reconciliation_audit
			UNION ALL
			SELECT 'identity' AS source, actor, action, target, created_at
			FROM iam.identity_audit
		) merged
		WHERE ($1 = '' OR source = $1)
		ORDER BY created_at DESC
		LIMIT $2 OFFSET $3
	`
	// limit+1 — узнать, есть ли ещё строки за текущей страницей, без
	// отдельного COUNT(*) по всем четырём таблицам (next_offset semantics,
	// тот же облегчённый подход, что у "без total_count" остальных Browse
	// методов, README "Что НЕ реализовано").
	rows, err := p.pool.Query(ctx, query, filter.Source, limit+1, filter.Offset)
	if err != nil {
		return nil, false, fmt.Errorf("audit_browse query: %w", err)
	}
	defer rows.Close()

	var results []AuditEntry
	for rows.Next() {
		var e AuditEntry
		if err := rows.Scan(&e.Source, &e.Actor, &e.Action, &e.Target, &e.CreatedAt); err != nil {
			return nil, false, fmt.Errorf("audit_browse scan: %w", err)
		}
		results = append(results, e)
	}
	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf("audit_browse: %w", err)
	}

	hasMore := len(results) > limit
	if hasMore {
		results = results[:limit]
	}
	return results, hasMore, nil
}

// SupportMessage — luminous-hugging-charm.md Ф9, GET /v1/support/messages/search.
// Тот же набор колонок, что partner-api's MessageStatus
// (messaging.message_read_model) — сознательно НЕ дублируем структуру,
// но здесь СВОЙ тип (не импортируем partner-api как зависимость — два
// независимых Go-модуля, тот же класс "форма совпадает не потому что
// общий тип, а потому что общий источник данных", что уже принят в этой
// сессии повсеместно).
type SupportMessage struct {
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

type SupportMessageSearchFilter struct {
	// Хотя бы одно из двух — валидируется в httpapi (handleSupportMessagesSearch),
	// не здесь: пустой запрос без message_id/trace_id вернул бы ВСЮ таблицу
	// messaging.message_read_model кросс-партнёрски, это не "поиск", это
	// незапрошенный полный дамп.
	MessageID string
	TraceID   string
	Limit     int
	Offset    int
}

// SupportMessageSearch — handle_support_message_search. В отличие от
// partner-api's Search (внутри пакета store того сервиса), здесь СОЗНАТЕЛЬНО
// нет фильтра по partner_id — это весь смысл кросс-партнёрского поиска для
// саппорта (package doc этого файла: "здесь нет per-partner scoping").
//
// Поиск по msisdn НЕ реализован — честно, не молчаливый пробел, см.
// services/backoffice-api/README.md "Кросс-партнёрский поиск" за полным
// разбором: msisdn не персистится НИГДЕ queryable (ни в
// messaging.message_read_model/message_lifecycle_history, ни в ClickHouse
// analytics.stage_events) — единственное место, где он вообще есть,
// msgctx:{message_id} в Runtime Redis, keyed по message_id (не reverse-
// searchable без полного SCAN, который на заявленном масштабе платформы
// 27-40к сообщений/с, capacity_model.md §6.2, попросту неосуществим —
// не "неоптимально", а физически не отработает за разумное время).
func (p *Postgres) SupportMessageSearch(ctx context.Context, filter SupportMessageSearchFilter) ([]SupportMessage, error) {
	limit := filter.Limit
	if limit <= 0 || limit > 200 {
		limit = 50
	}

	query := `
		SELECT message_id, partner_id, application_id, trace_id, pipeline_id, pipeline_version, current_status, terminal, created_at, updated_at
		FROM messaging.message_read_model
		WHERE true
	`
	var args []interface{}
	if filter.MessageID != "" {
		args = append(args, filter.MessageID)
		query += fmt.Sprintf(" AND message_id = $%d", len(args))
	}
	if filter.TraceID != "" {
		args = append(args, filter.TraceID)
		query += fmt.Sprintf(" AND trace_id = $%d", len(args))
	}
	args = append(args, limit)
	query += fmt.Sprintf(" ORDER BY created_at DESC LIMIT $%d", len(args))
	args = append(args, filter.Offset)
	query += fmt.Sprintf(" OFFSET $%d", len(args))

	rows, err := p.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("support_message_search query: %w", err)
	}
	defer rows.Close()

	var results []SupportMessage
	for rows.Next() {
		var m SupportMessage
		if err := rows.Scan(&m.MessageID, &m.PartnerID, &m.ApplicationID, &m.TraceID, &m.PipelineID,
			&m.PipelineVersion, &m.CurrentStatus, &m.Terminal, &m.CreatedAt, &m.UpdatedAt); err != nil {
			return nil, fmt.Errorf("support_message_search scan: %w", err)
		}
		results = append(results, m)
	}
	return results, rows.Err()
}

type MessageBrowseFilter struct {
	PartnerID     string
	CurrentStatus string
	// nil — оба (terminal/не terminal), фильтр не задан.
	Terminal *bool
	Limit    int
	Offset   int
}

// MessageBrowse — handle_message_browse: список последних сообщений, тот
// же паттерн, что DlqBrowse/ReconciliationBrowse выше (LIMIT/OFFSET,
// ORDER BY created_at DESC, id НЕ обязателен) — в отличие от
// SupportMessageSearch, id здесь не требуется, потому что запрос уже
// ограничен LIMIT (clampLimit, максимум 200), а не "верни всю таблицу":
// то же разграничение "поиск по id" vs "browse с пагинацией", что уже
// проведено выше для DLQ/reconciliation.
func (p *Postgres) MessageBrowse(ctx context.Context, filter MessageBrowseFilter) ([]SupportMessage, error) {
	query := `
		SELECT message_id, partner_id, application_id, trace_id, pipeline_id, pipeline_version, current_status, terminal, created_at, updated_at
		FROM messaging.message_read_model
		WHERE true
	`
	var args []interface{}
	if filter.PartnerID != "" {
		args = append(args, filter.PartnerID)
		query += fmt.Sprintf(" AND partner_id = $%d", len(args))
	}
	if filter.CurrentStatus != "" {
		args = append(args, filter.CurrentStatus)
		query += fmt.Sprintf(" AND current_status = $%d", len(args))
	}
	if filter.Terminal != nil {
		args = append(args, *filter.Terminal)
		query += fmt.Sprintf(" AND terminal = $%d", len(args))
	}
	args = append(args, clampLimit(filter.Limit))
	query += fmt.Sprintf(" ORDER BY created_at DESC LIMIT $%d", len(args))
	args = append(args, filter.Offset)
	query += fmt.Sprintf(" OFFSET $%d", len(args))

	rows, err := p.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("message_browse query: %w", err)
	}
	defer rows.Close()

	var results []SupportMessage
	for rows.Next() {
		var m SupportMessage
		if err := rows.Scan(&m.MessageID, &m.PartnerID, &m.ApplicationID, &m.TraceID, &m.PipelineID,
			&m.PipelineVersion, &m.CurrentStatus, &m.Terminal, &m.CreatedAt, &m.UpdatedAt); err != nil {
			return nil, fmt.Errorf("message_browse scan: %w", err)
		}
		results = append(results, m)
	}
	return results, rows.Err()
}
