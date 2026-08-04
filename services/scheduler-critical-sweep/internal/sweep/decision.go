package sweep

import "time"

// EvaluateRetryPolicy — evaluate_retry_policy: attempt, retry policy стадии
// -> Retry | Exhausted | NotRetryable (MaxAttempts=0 — стадия не ретраится
// по таймауту вовсе, см. RetryDecision).
func EvaluateRetryPolicy(attempt int32, policy RetryPolicy) RetryDecision {
	if policy.MaxAttempts == 0 {
		return DecisionNotRetryable
	}
	if attempt < policy.MaxAttempts {
		return DecisionRetry
	}
	return DecisionExhausted
}

// CheckExecutionControl — check_execution_control: повторная проверка перед
// retry, чтобы не публиковать в стадию, которая ушла в PAUSED уже после
// исходной диспетчеризации.
func CheckExecutionControl(stageName string, snapshot ControlSnapshot) ControlDecision {
	if snapshot != nil && snapshot.IsPaused(stageName) {
		return DecisionHold
	}
	return DecisionProceed
}

// Action — что нужно сделать с ExpiredEntry после прогонки через
// retry policy + execution control. Ровно одно из полей ниже ненулевое
// (или ни одного — Hold).
type Action int

const (
	ActionHold Action = iota
	ActionRetry
	ActionTimeout
	ActionDlq
)

// Decide — оркестрация tick_sweep -> {evaluate_retry_policy,
// check_execution_control} -> Action. Чистая функция, без сети: вызывающая
// сторона (internal/kafkaio, internal/redisio) отвечает за публикацию и
// ZREM по результату.
//
// Порядок проверок: сначала execution control (Hold не расходует retry
// attempt — сообщение просто остаётся с тем же дедлайном до следующего
// тика sweep, deadline не продлевается здесь намеренно, см. README), затем
// retry policy.
func Decide(state ExecutionState, policy RetryPolicy, snapshot ControlSnapshot) Action {
	if CheckExecutionControl(state.StageName, snapshot) == DecisionHold {
		return ActionHold
	}

	switch EvaluateRetryPolicy(state.Attempt, policy) {
	case DecisionRetry:
		return ActionRetry
	case DecisionNotRetryable:
		return ActionTimeout
	default:
		return ActionDlq
	}
}

// NextDeadline — обёртка над policy.Backoff с фолбэком на фиксированный
// backoff, если policy.Backoff не задан (тестовое удобство).
func NextDeadline(attempt int32, policy RetryPolicy, now time.Time) time.Time {
	if policy.Backoff != nil {
		return policy.Backoff(attempt, now)
	}
	return now.Add(time.Duration(attempt) * time.Second)
}
