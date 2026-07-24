// Package core — check_ttl/check_idempotency/check_billing_side_effect/
// check_delivery_ambiguity (service_internal_methods.md §7.4, HLD §20):
// чистые функции, без сети.
package core

import "time"

// DlqRecord — то, что load_dlq_record читает из messaging.dlq_record (migrations/V006).
type DlqRecord struct {
	StageExecutionID string
	MessageID        string
	StageName        string // "DESTINATION_RESOLUTION" | "POLICY" | "BILLING" | "ROUTING" | "DELIVERY" | "DELIVERY_RECONCILIATION"
	Attempt          int32
	OriginalCommand  []byte // сериализованный StageExecuteCommand целиком
	ReasonCode       string
	ErrorDetail      string
	CreatedAt        time.Time
	ReplayStatus     string // "pending" | "replayed" | "expired"
	MessageTTL       time.Time
}

type TTLDecision int

const (
	TTLValid TTLDecision = iota
	TTLExpired
)

// check_ttl — DlqRecord, текущее время -> Valid | Expired. message_ttl —
// из оригинальной StageExecuteCommand (не created_at + фиксированное окно):
// сообщение считается живым, пока не истёк его собственный TTL.
func CheckTTL(record DlqRecord, now time.Time) TTLDecision {
	if record.MessageTTL.IsZero() {
		return TTLValid // TTL не задан в команде — консервативно не блокируем на этом основании
	}
	if now.After(record.MessageTTL) {
		return TTLExpired
	}
	return TTLValid
}

type IdempotencyDecision int

const (
	IdempotencySafe IdempotencyDecision = iota
	IdempotencyUnsafe
)

// check_idempotency — DlqRecord.stage_execution_id уже безопасен по
// контракту §6 (stage_execution_id — единственный ключ идемпотентности,
// HLD §4), ЕСЛИ эта конкретная запись ещё не была реплеена — повторный
// replay уже реплеенной записи не идемпотентен на уровне Replay Service
// (создаст вторую попытку той же стадии с тем же attempt, если downstream
// уже успел применить CAS-переход по первому replay).
func CheckIdempotency(record DlqRecord) IdempotencyDecision {
	if record.ReplayStatus == "replayed" {
		return IdempotencyUnsafe
	}
	return IdempotencySafe
}

type SafetyDecision int

const (
	Safe SafetyDecision = iota
	Unsafe
)

// check_billing_side_effect — только для stage_name=BILLING: Unsafe, если
// charge_id (= stage_execution_id, HLD §15.1/§15.4) уже присутствует в
// billing.billing_ledger — списание уже произошло, повторный billing
// execute создал бы риск двойного списания (даже с идемпотентным
// apply_atomic_charge на стороне Billing Service, Replay Service не должен
// полагаться только на downstream-защиту).
func CheckBillingSideEffect(record DlqRecord, chargeExistsInLedger bool) SafetyDecision {
	if record.StageName != "BILLING" {
		return Safe
	}
	if chargeExistsInLedger {
		return Unsafe
	}
	return Safe
}

// check_delivery_ambiguity — только для stage_name=DELIVERY: Unsafe, если
// уже существует dlr_correlation для этого stage_execution_id — значит,
// submit уже дошёл до оператора (DLR Correlation Writer уже увидел
// operator.submit.accepted), и повторная Delivery execute рискует
// отправить абоненту физический дубликат SMS.
func CheckDeliveryAmbiguity(record DlqRecord, correlationExists bool) SafetyDecision {
	if record.StageName != "DELIVERY" {
		return Safe
	}
	if correlationExists {
		return Unsafe
	}
	return Safe
}