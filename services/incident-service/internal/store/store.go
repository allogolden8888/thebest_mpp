// Package store — Postgres-доступ incident-service: собственная схема
// incident.* (migrations/V028__incident.sql) плюс ПРЯМОЕ чтение
// control.execution_control_audit (владелец — execution-control-service) для
// таймлайна — тот же паттерн прямого cross-schema чтения, что backoffice-api
// уже использует для DLQ/reconciliation/audit browse
// (services/backoffice-api/internal/store/postgres.go package doc), не
// новая gRPC-зависимость от execution-control-service.
package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrIncidentNotFound — incident_id не найден в incident.incidents.
var ErrIncidentNotFound = errors.New("инцидент не найден")

type Incident struct {
	ID              int64
	Title           string
	Severity        string
	Status          string
	OpenedBy        string
	OpenedAt        time.Time
	ResolvedBy      string
	ResolvedAt      *time.Time
	PostmortemNotes string
}

type IncidentNote struct {
	ID         int64
	IncidentID int64
	Author     string
	Note       string
	CreatedAt  time.Time
}

// TimelineEntry — проекция строки control.execution_control_audit
// (migrations/V016__execution_control_audit.sql), связанной с инцидентом.
type TimelineEntry struct {
	ID            int64
	Scope         string
	ScopeID       string
	State         string
	AdmissionRate float64
	Reason        string
	RequestedBy   string
	CreatedAt     time.Time
	ExpiresAt     *time.Time
}

type Postgres struct {
	pool *pgxpool.Pool
}

func NewPostgres(pool *pgxpool.Pool) *Postgres {
	return &Postgres{pool: pool}
}

// Ping — используется /readyz, не запросами приложения.
func (p *Postgres) Ping(ctx context.Context) error {
	return p.pool.Ping(ctx)
}

// OpenIncident — вставляет новую строку incident.incidents, status=OPEN,
// opened_at=DEFAULT now(). Одна вставка, без audit-таблицы поверх — opened_at
// + opened_by УЖЕ аудит-след открытия (тот же принцип, что README этой фазы
// проговаривает явно: opened_at/resolved_at на incidents — аудит-след для
// переходов статуса).
func (p *Postgres) OpenIncident(ctx context.Context, title, severity, openedBy string) (Incident, error) {
	var inc Incident
	err := p.pool.QueryRow(ctx, `
		INSERT INTO incident.incidents (title, severity, opened_by)
		VALUES ($1, $2, $3)
		RETURNING id, title, severity, status, opened_by, opened_at`,
		title, severity, openedBy).
		Scan(&inc.ID, &inc.Title, &inc.Severity, &inc.Status, &inc.OpenedBy, &inc.OpenedAt)
	if err != nil {
		return Incident{}, fmt.Errorf("OpenIncident: %w", err)
	}
	return inc, nil
}

// ListIncidents — status == "" -> все инциденты; иначе фильтр по конкретному
// статусу (OPEN/RESOLVED). Сортировка по opened_at DESC — самые свежие
// сверху, backoffice-ui список "что горит сейчас".
func (p *Postgres) ListIncidents(ctx context.Context, status string) ([]Incident, error) {
	rows, err := p.pool.Query(ctx, `
		SELECT id, title, severity, status, opened_by, opened_at, resolved_by, resolved_at, postmortem_notes
		FROM incident.incidents
		WHERE $1 = '' OR status = $1
		ORDER BY opened_at DESC`, status)
	if err != nil {
		return nil, fmt.Errorf("ListIncidents: %w", err)
	}
	defer rows.Close()

	var incidents []Incident
	for rows.Next() {
		inc, err := scanIncident(rows)
		if err != nil {
			return nil, fmt.Errorf("ListIncidents: scan: %w", err)
		}
		incidents = append(incidents, inc)
	}
	return incidents, rows.Err()
}

// GetIncident — карточка инцидента без таймлайна/заметок (см.
// TimelineForIncident/ListNotes — раздельные запросы, собираются вместе на
// уровне grpcserver.GetIncident в IncidentDetail).
func (p *Postgres) GetIncident(ctx context.Context, incidentID int64) (Incident, error) {
	row := p.pool.QueryRow(ctx, `
		SELECT id, title, severity, status, opened_by, opened_at, resolved_by, resolved_at, postmortem_notes
		FROM incident.incidents WHERE id = $1`, incidentID)
	inc, err := scanIncident(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Incident{}, ErrIncidentNotFound
		}
		return Incident{}, fmt.Errorf("GetIncident: %w", err)
	}
	return inc, nil
}

// scanRow — общий интерфейс pgx.Row/pgx.Rows для scanIncident.
type scanRow interface {
	Scan(dest ...any) error
}

func scanIncident(row scanRow) (Incident, error) {
	var inc Incident
	var resolvedBy, postmortem *string
	var resolvedAt *time.Time
	if err := row.Scan(&inc.ID, &inc.Title, &inc.Severity, &inc.Status, &inc.OpenedBy, &inc.OpenedAt, &resolvedBy, &resolvedAt, &postmortem); err != nil {
		return Incident{}, err
	}
	if resolvedBy != nil {
		inc.ResolvedBy = *resolvedBy
	}
	if postmortem != nil {
		inc.PostmortemNotes = *postmortem
	}
	inc.ResolvedAt = resolvedAt
	return inc, nil
}

// TimelineForIncident — cross-schema чтение control.execution_control_audit
// WHERE incident_id = $1 ORDER BY created_at ASC (хронологически — читается
// как история, не "последнее сверху"). Обслуживается частичным индексом
// execution_control_audit_incident_id_idx (migrations/V028).
func (p *Postgres) TimelineForIncident(ctx context.Context, incidentID int64) ([]TimelineEntry, error) {
	rows, err := p.pool.Query(ctx, `
		SELECT id, scope, scope_id, state, admission_rate, reason, requested_by, created_at, expires_at
		FROM control.execution_control_audit
		WHERE incident_id = $1
		ORDER BY created_at ASC`, incidentID)
	if err != nil {
		return nil, fmt.Errorf("TimelineForIncident: %w", err)
	}
	defer rows.Close()

	var entries []TimelineEntry
	for rows.Next() {
		var e TimelineEntry
		if err := rows.Scan(&e.ID, &e.Scope, &e.ScopeID, &e.State, &e.AdmissionRate, &e.Reason, &e.RequestedBy, &e.CreatedAt, &e.ExpiresAt); err != nil {
			return nil, fmt.Errorf("TimelineForIncident: scan: %w", err)
		}
		entries = append(entries, e)
	}
	return entries, rows.Err()
}

// ListNotes — заметки инцидента, хронологически (created_at ASC) — таймлайн
// коллаборации, та же ориентация чтения, что TimelineForIncident.
func (p *Postgres) ListNotes(ctx context.Context, incidentID int64) ([]IncidentNote, error) {
	rows, err := p.pool.Query(ctx, `
		SELECT id, incident_id, author, note, created_at
		FROM incident.incident_notes
		WHERE incident_id = $1
		ORDER BY created_at ASC`, incidentID)
	if err != nil {
		return nil, fmt.Errorf("ListNotes: %w", err)
	}
	defer rows.Close()

	var notes []IncidentNote
	for rows.Next() {
		var n IncidentNote
		if err := rows.Scan(&n.ID, &n.IncidentID, &n.Author, &n.Note, &n.CreatedAt); err != nil {
			return nil, fmt.Errorf("ListNotes: scan: %w", err)
		}
		notes = append(notes, n)
	}
	return notes, rows.Err()
}

// AddNote — простая вставка, без audit-таблицы поверх: incident_notes САМА
// IS аудит-след для заметок (в отличие от iam.staff_role_assignments/
// credentials.issued_secrets, здесь нет отдельной мутации сверх вставки).
// FK на incident.incidents (id) даёт NotFound естественно через нарушение
// внешнего ключа, но мы явно проверяем существование сначала, чтобы вернуть
// ErrIncidentNotFound, а не голую ошибку constraint violation.
func (p *Postgres) AddNote(ctx context.Context, incidentID int64, author, note string) (IncidentNote, error) {
	var n IncidentNote
	err := p.pool.QueryRow(ctx, `
		INSERT INTO incident.incident_notes (incident_id, author, note)
		SELECT $1, $2, $3 WHERE EXISTS (SELECT 1 FROM incident.incidents WHERE id = $1)
		RETURNING id, incident_id, author, note, created_at`,
		incidentID, author, note).
		Scan(&n.ID, &n.IncidentID, &n.Author, &n.Note, &n.CreatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return IncidentNote{}, ErrIncidentNotFound
		}
		return IncidentNote{}, fmt.Errorf("AddNote: %w", err)
	}
	return n, nil
}

// ResolveIncident — status -> RESOLVED, resolved_by/resolved_at/
// postmortem_notes проставляются. Непустота postmortem_notes проверяется
// ВЫШЕ, на уровне grpcserver (codes.InvalidArgument) — тот же принцип, что
// execution-control-service валидирует admission_rate в коде до того, как
// запрос доходит до БД, не полагаясь на CHECK-ограничение. Здесь
// дополнительно защищаемся от гонки "уже RESOLVED" через
// WHERE status = 'OPEN' — 0 affected rows => не найден ИЛИ уже закрыт,
// различаем отдельным SELECT для точного сообщения об ошибке.
func (p *Postgres) ResolveIncident(ctx context.Context, incidentID int64, resolvedBy, postmortemNotes string) (Incident, error) {
	row := p.pool.QueryRow(ctx, `
		UPDATE incident.incidents
		SET status = 'RESOLVED', resolved_by = $2, resolved_at = now(), postmortem_notes = $3
		WHERE id = $1 AND status = 'OPEN'
		RETURNING id, title, severity, status, opened_by, opened_at, resolved_by, resolved_at, postmortem_notes`,
		incidentID, resolvedBy, postmortemNotes)
	inc, err := scanIncident(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// Различаем "не существует" от "уже RESOLVED" отдельным чтением —
			// разные ошибки заслуживают разных gRPC-кодов на уровне сервера.
			if _, getErr := p.GetIncident(ctx, incidentID); errors.Is(getErr, ErrIncidentNotFound) {
				return Incident{}, ErrIncidentNotFound
			}
			return Incident{}, ErrAlreadyResolved
		}
		return Incident{}, fmt.Errorf("ResolveIncident: %w", err)
	}
	return inc, nil
}

// ErrAlreadyResolved — инцидент существует, но уже в статусе RESOLVED
// (повторное закрытие — не находка, а гонка/дубликат клика в backoffice-ui).
var ErrAlreadyResolved = errors.New("инцидент уже закрыт")
