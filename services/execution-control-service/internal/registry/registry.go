// Package registry держит по одному hysteresis.ControlLoop на каждый scope
// (GLOBAL / STAGE / PARTNER / PARTNER_STAGE / OPERATOR_ROUTE), применяет
// manual override (apply_manual_override/persist_override_audit —
// service_internal_methods.md §3.1) поверх метрик-driven гистерезиса и
// раздаёт версии для fencing записей execution.control.
//
// Override имеет приоритет над метрикой: пока override активен (не
// clear'нут и не истёк по expires_at), evaluate игнорирует ControlLoop и
// возвращает состояние/rate из override напрямую — тем самым Billing
// Reconciliation freeze (PARTNER_STAGE=BILLING, HLD §15.5) и Backoffice
// manual override работают детерминированно, не соревнуясь с шумной
// метрикой в тот же тик.
package registry

import (
	"sync"
	"time"

	"mpp/execution-control-service/internal/hysteresis"
)

// ScopeKey однозначно определяет scope-запись, аналог (scope, scope_id) в
// ExecutionControlRecord.
type ScopeKey struct {
	Scope   hysteresis.Scope
	ScopeID string
}

// Override — активный ручной override для scope.
type Override struct {
	State         hysteresis.State
	AdmissionRate float64
	Reason        string
	RequestedBy   string
	ExpiresAt     *time.Time // nil => действует до явного ClearOverride
}

func (o *Override) expired(now time.Time) bool {
	return o != nil && o.ExpiresAt != nil && !o.ExpiresAt.After(now)
}

// Evaluation — итог evaluate для одного scope на конкретный момент.
type Evaluation struct {
	State         hysteresis.State
	AdmissionRate float64
	DispatchRate  float64
	Reason        string
	Version       int64
	ExpiresAt     *time.Time
}

type scopeEntry struct {
	loop     *hysteresis.ControlLoop
	override *Override
	version  int64
	tickNo   int
	rate     float64 // текущий admission/dispatch rate вне override — управляется ramp-логикой в Evaluate
}

// Registry — потокобезопасный держатель scope-состояний.
type Registry struct {
	mu       sync.Mutex
	defaults hysteresis.Thresholds
	entries  map[ScopeKey]*scopeEntry
}

func New(defaults hysteresis.Thresholds) *Registry {
	return &Registry{defaults: defaults, entries: make(map[ScopeKey]*scopeEntry)}
}

func (r *Registry) entry(key ScopeKey) *scopeEntry {
	e, ok := r.entries[key]
	if !ok {
		e = &scopeEntry{loop: hysteresis.NewControlLoop(r.defaults), rate: 1.0}
		r.entries[key] = e
	}
	return e
}

// Evaluate подаёт очередное значение метрики для scope и возвращает
// подтверждённый результат — с учётом активного override, если он есть
// (compose_effective_rate/evaluate_hysteresis).
func (r *Registry) Evaluate(key ScopeKey, metric float64, now time.Time) Evaluation {
	r.mu.Lock()
	defer r.mu.Unlock()

	e := r.entry(key)

	if e.override != nil && e.override.expired(now) {
		e.override = nil
	}

	if e.override != nil {
		return Evaluation{
			State:         e.override.State,
			AdmissionRate: e.override.AdmissionRate,
			DispatchRate:  e.override.AdmissionRate,
			Reason:        e.override.Reason,
			Version:       e.version,
			ExpiresAt:     e.override.ExpiresAt,
		}
	}

	e.tickNo++
	state := e.loop.Tick(e.tickNo, metric)

	switch state {
	case hysteresis.StatePaused:
		e.rate = 0.0
	case hysteresis.StateDegraded:
		// Точный процент DEGRADED не зафиксирован в LLD как отдельная
		// константа — взят консервативно как первый ramp-шаг выше нуля,
		// чтобы восстановление из DEGRADED->ACTIVE продолжало ramp с той
		// же лестницы (RampSteps), а не начинало с произвольного числа.
		e.rate = hysteresis.RampSteps[1]
	case hysteresis.StateActive:
		e.rate = hysteresis.ComputeRampStep(state, e.rate)
	}

	return Evaluation{
		State:         state,
		AdmissionRate: e.rate,
		DispatchRate:  e.rate,
		Reason:        "metric_driven_hysteresis",
		Version:       e.version,
	}
}

// ApplyOverride устанавливает ручной override для scope и возвращает новую
// версию (apply_manual_override). Вызывающая сторона (gRPC-хендлер) отвечает
// за persist_override_audit в PostgreSQL.
func (r *Registry) ApplyOverride(key ScopeKey, o Override) (version int64, appliedAt time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()

	e := r.entry(key)
	e.override = &o
	e.version++
	return e.version, time.Now().UTC()
}

// ClearOverride снимает ручной override — scope возвращается к
// метрика-driven гистерезису со следующего Evaluate.
func (r *Registry) ClearOverride(key ScopeKey) (version int64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	e := r.entry(key)
	e.override = nil
	e.version++
	return e.version
}
