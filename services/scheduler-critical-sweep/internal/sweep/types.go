// Package sweep — чистая (без сети) бизнес-логика Critical Sweep
// (service_internal_methods.md §2.1): evaluate_retry_policy,
// check_execution_control, и решение, что делать с просроченной записью
// (publish_retry / publish_timeout_result / publish_dlq).
//
// Пересмотрено в services_specifictaion.md §3.1: Critical Sweep больше не
// Kafka Streams-группа со своим changelog — он раз в ~1с опрашивает
// шардированный Redis sorted set deadlines:{bucket} (Pipeline Engine пишет
// дедлайн туда же атомарно с CAS-переходом, cas_transition_and_track_deadline,
// §1.4) и публикует retry/timeout/DLQ.
package sweep

import "time"

// ExpiredEntry — одна просроченная запись из deadlines:{bucket}
// (ZRANGEBYSCORE ... -inf now).
type ExpiredEntry struct {
	StageExecutionID string
	Bucket           int
	DeadlineUnixMs   int64
}

// ExecutionState — то, что load_execution_state читает из exec:{message_id}
// в Runtime Redis (data_infrastructure_spec.md §2.1) — авторитетное
// состояние Pipeline Engine.
type ExecutionState struct {
	MessageID          string
	PipelineVersion     string
	NodeID              string
	StageExecutionID    string
	StageName           string // "DESTINATION_RESOLUTION" | "POLICY" | "BILLING" | "ROUTING" | "DELIVERY" | "DELIVERY_RECONCILIATION"
	Attempt             int32
	Deadline            time.Time
	LastAppliedEventID  string
}

// RetryPolicy — параметры повторной попытки для стадии. Не описаны отдельной
// JSON Schema в config_schemas/ (pipeline.schema.json описывает только граф
// переходов по Outcome, не retry-параметры) — значения по умолчанию,
// задокументированные как рабочее предположение первого среза, см. README.
type RetryPolicy struct {
	MaxAttempts int32
	// BackoffFor вычисляет новый deadline для следующей попытки — по
	// умолчанию экспоненциальный backoff с потолком, задаётся вызывающей
	// стороной, чтобы не завязывать чистую логику на time.Now().
	Backoff func(attempt int32, now time.Time) time.Time
}

// RetryDecision — результат evaluate_retry_policy. Три исхода, не два:
// MaxAttempts=0 означает "эта стадия вообще не ретраится по таймауту"
// (сразу publish_timeout_result, TIMED_OUT) — отдельно от "ретраится, но
// попытки исчерпаны" (publish_dlq, RETRY_EXHAUSTED), см.
// service_internal_methods.md §2.1: "publish_timeout_result | ExpiredEntry
// (без retry)" и "publish_dlq | Exhausted" — два разных метода на два
// разных исхода.
type RetryDecision int

const (
	DecisionRetry RetryDecision = iota
	DecisionExhausted
	DecisionNotRetryable
)

// ControlDecision — результат check_execution_control.
type ControlDecision int

const (
	DecisionProceed ControlDecision = iota
	DecisionHold
)

// ControlSnapshot — минимальный интерфейс, который нужен check_execution_control
// от локального снапшота execution.control (compacted, тот же паттерн, что
// в остальных потребителях — "чистое вычисление без внешнего эффекта",
// service_io_contracts.md).
type ControlSnapshot interface {
	// IsPaused возвращает true, если состояние для данного stage_name (и
	// глобальный scope) сейчас PAUSED — Critical Sweep не должен публиковать
	// retry в стадию, которая сейчас на паузе (HLD §8, "повторная проверка
	// перед retry").
	IsPaused(stageName string) bool
}
