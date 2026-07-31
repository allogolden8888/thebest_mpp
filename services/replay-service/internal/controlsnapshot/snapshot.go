// Package controlsnapshot — локальный immutable snapshot execution.control
// (compacted, local full-mirror — service_io_contracts.md, тот же принцип,
// что и в других сервисах этой сессии). Чистая структура данных + чистое
// вычисление (IsPaused); сетевая часть — internal/kafkaio/controlconsumer.go.
//
// CODE_REVIEW.md HIGH finding (replay-service): раньше Republish публиковал
// прошедшую все safety-проверки команду напрямую на любой из шести stage.*
// топиков без единой проверки execution.control — republish полностью
// игнорировал GLOBAL/PARTNER_STAGE-level паузы. Конкретный сценарий:
// Billing намеренно заморожен на время инцидента
// (PARTNER_STAGE=BILLING,PAUSED), а оператор, разбирающий тот же инцидент,
// реплеит backlog BILLING-стадийных DLQ-записей — Replay Service republish
// напрямую на stage.billing, в обход паузы, специально поставленной, чтобы
// остановить billing-активность.
package controlsnapshot

import (
	"sync"

	commonv1 "mpp/platformcontracts/common/v1"
)

type key struct {
	scope   commonv1.ExecutionControlScope
	scopeID string
}

// Snapshot — потокобезопасный держатель последних ExecutionControlRecord по
// (scope, scope_id), обновляемых консьюмером execution.control.
type Snapshot struct {
	mu    sync.RWMutex
	state map[key]commonv1.ExecutionControlState
}

func New() *Snapshot {
	return &Snapshot{state: make(map[key]commonv1.ExecutionControlState)}
}

// Apply — upsert по compaction key = (scope, scope_id).
func (s *Snapshot) Apply(scope commonv1.ExecutionControlScope, scopeID string, state commonv1.ExecutionControlState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state[key{scope, scopeID}] = state
}

// Delete — tombstone (нулевое value для ключа compacted-топика) — снятая
// запись означает "нет активного control для этого scope", не "PAUSED".
func (s *Snapshot) Delete(scope commonv1.ExecutionControlScope, scopeID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.state, key{scope, scopeID})
}

// IsPaused — GLOBAL или STAGE(stageName) в PAUSED блокирует republish
// (compose_effective_rate: наиболее строгое state по применимым scope
// побеждает, HLD §8.1). stageName — тот же plain-string формат, что
// core.DlqRecord.StageName ("BILLING", "DELIVERY" и т.п.).
func (s *Snapshot) IsPaused(stageName string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.state[key{commonv1.ExecutionControlScope_EXECUTION_CONTROL_SCOPE_GLOBAL, ""}] == commonv1.ExecutionControlState_EXECUTION_CONTROL_STATE_PAUSED {
		return true
	}
	return s.state[key{commonv1.ExecutionControlScope_EXECUTION_CONTROL_SCOPE_STAGE, stageName}] == commonv1.ExecutionControlState_EXECUTION_CONTROL_STATE_PAUSED
}
