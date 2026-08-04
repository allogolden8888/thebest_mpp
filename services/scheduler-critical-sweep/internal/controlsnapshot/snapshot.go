// Package controlsnapshot — локальный immutable snapshot execution.control
// (compacted, service_io_contracts.md: "execution.control — Kafka
// (compacted, local snapshot)"), реализует sweep.ControlSnapshot для
// check_execution_control. Чистая структура данных + чистое вычисление
// (IsPaused) — сетевая часть (консьюмер компактированного топика) не входит
// в этот файл, см. README "Что НЕ реализовано".
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

// Apply — обновление снапшота одной записью execution.control (upsert по
// compaction key = (scope, scope_id)).
func (s *Snapshot) Apply(scope commonv1.ExecutionControlScope, scopeID string, state commonv1.ExecutionControlState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state[key{scope, scopeID}] = state
}

// Delete — применение tombstone-записи (value=nil) компактированного
// execution.control: ключ (scope, scope_id) больше не имеет применимого
// override, удаляется из снапшота целиком, а не переводится в какое-то
// state-значение (CODE_REVIEW.md finding #4, internal/kafkaio.ControlSnapshotConsumer).
func (s *Snapshot) Delete(scope commonv1.ExecutionControlScope, scopeID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.state, key{scope, scopeID})
}

// Len — число примененных (scope, scope_id) записей — используется для
// диагностики warm-up консьюмера (main.go), не для бизнес-логики.
func (s *Snapshot) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.state)
}

// IsPaused — check_execution_control: PAUSED побеждает по всей
// scope-иерархии — если GLOBAL или STAGE(stageName) в PAUSED, retry
// придерживается (HLD §8, "повторная проверка перед retry"). PARTNER/
// PARTNER_STAGE/OPERATOR_ROUTE scope здесь не проверяются — Critical Sweep
// ретраит по stage-уровню, а не per-partner (partner-специфичный контекст
// уже был на диспетчеризации, Critical Sweep его не хранит, см. README).
func (s *Snapshot) IsPaused(stageName string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.state[key{commonv1.ExecutionControlScope_EXECUTION_CONTROL_SCOPE_GLOBAL, ""}] == commonv1.ExecutionControlState_EXECUTION_CONTROL_STATE_PAUSED {
		return true
	}
	return s.state[key{commonv1.ExecutionControlScope_EXECUTION_CONTROL_SCOPE_STAGE, stageName}] == commonv1.ExecutionControlState_EXECUTION_CONTROL_STATE_PAUSED
}
