// Package store — persist_override_audit (service_internal_methods.md §3.1):
// запись примененного override в control.execution_control_audit
// (migrations/V016__execution_control_audit.sql).
package store

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"mpp/execution-control-service/internal/hysteresis"
)

// AuditEntry — одна запись override, готовая к вставке. Значения scope/state
// как TEXT (GLOBAL/STAGE/PARTNER/PARTNER_STAGE/OPERATOR_ROUTE,
// ACTIVE/DEGRADED/PAUSED) — те же CHECK-ограничения, что в V016.
type AuditEntry struct {
	Scope         string
	ScopeID       string
	State         string
	AdmissionRate float64
	Reason        string
	RequestedBy   string
	ExpiresAt     *time.Time
	// IncidentID — опциональная линковка к incident.incidents.id
	// (luminous-hugging-charm.md Ф7, incident-service, migrations/V028__incident.sql).
	// nil = не связано ни с одним инцидентом (обычный ручной override вне
	// разбора). Пишется как NULL в control.execution_control_audit.incident_id
	// — нет FK через границу схем (V028), поэтому здесь ничего не
	// валидируется против incident.incidents, значение просто прокидывается.
	IncidentID *int64
}

// ScopeName переводит hysteresis.Scope в TEXT-представление,
// зафиксированное CHECK-ограничением V016__execution_control_audit.sql.
func ScopeName(s hysteresis.Scope) string {
	switch s {
	case hysteresis.ScopeGlobal:
		return "GLOBAL"
	case hysteresis.ScopeStage:
		return "STAGE"
	case hysteresis.ScopePartner:
		return "PARTNER"
	case hysteresis.ScopePartnerStage:
		return "PARTNER_STAGE"
	case hysteresis.ScopeOperatorRoute:
		return "OPERATOR_ROUTE"
	default:
		return "GLOBAL"
	}
}

// StateName переводит hysteresis.State в TEXT-представление.
func StateName(s hysteresis.State) string {
	switch s {
	case hysteresis.StateActive:
		return "ACTIVE"
	case hysteresis.StateDegraded:
		return "DEGRADED"
	case hysteresis.StatePaused:
		return "PAUSED"
	default:
		return "ACTIVE"
	}
}

// AuditStore — тонкая обёртка над pgxpool.Pool для этой единственной таблицы.
type AuditStore struct {
	pool *pgxpool.Pool
}

func NewAuditStore(pool *pgxpool.Pool) *AuditStore {
	return &AuditStore{pool: pool}
}

// PersistOverrideAudit вставляет запись override в
// control.execution_control_audit — одна строка на каждое применение
// (ApplyOverride/ClearOverride уже вызвавшее registry.ApplyOverride).
func (s *AuditStore) PersistOverrideAudit(ctx context.Context, e AuditEntry) (id int64, createdAt time.Time, err error) {
	err = s.pool.QueryRow(ctx, `
		INSERT INTO control.execution_control_audit
			(scope, scope_id, state, admission_rate, reason, requested_by, expires_at, incident_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		RETURNING id, created_at
	`, e.Scope, e.ScopeID, e.State, e.AdmissionRate, e.Reason, e.RequestedBy, e.ExpiresAt, e.IncidentID).Scan(&id, &createdAt)
	return id, createdAt, err
}
