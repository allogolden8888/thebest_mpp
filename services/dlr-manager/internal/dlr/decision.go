package dlr

import (
	"time"

	"mpp/dlr-manager/internal/correlation"
)

type Kind int

const (
	// KindPublishDeliveryStatus — correlation найдена, статус распознан.
	KindPublishDeliveryStatus Kind = iota
	// KindScheduleRetry — correlation не найдена, окно ещё не истекло.
	KindScheduleRetry
	// KindPublishDlq — correlation не найдена, окно истекло
	// (evaluate_correlation_window -> Expire).
	KindPublishDlq
	// KindDropUnrecognizedStatus — raw_status вне известного словаря
	// (см. NormalizeStatus) — нет смысла ни публиковать delivery.status
	// (нечего публиковать), ни ретраить корреляцию (проблема не в
	// корреляции). Известное ограничение: нет DLQ-пути для ЭТОГО класса
	// сбоя — operator.dlr.dlq предназначен именно для
	// "correlation window expired", не для "статус не распознан" — см. README.
	KindDropUnrecognizedStatus
)

type Decision struct {
	Kind             Kind
	NormalizedStatus string
}

// Decide — handle_delivery_execute-эквивалент для DLR Manager: чистая
// функция, объединяющая normalize_operator_status/lookup_correlation/
// evaluate_correlation_window в один детерминированный результат
// (service_internal_methods.md §4.2, порядок методов в таблице).
func Decide(
	receivedAt time.Time,
	normalizedStatus string,
	statusRecognized bool,
	found *correlation.Record,
	now time.Time,
	correlationWindow time.Duration,
) Decision {
	if !statusRecognized {
		return Decision{Kind: KindDropUnrecognizedStatus}
	}
	if found != nil {
		return Decision{Kind: KindPublishDeliveryStatus, NormalizedStatus: normalizedStatus}
	}
	deadline := receivedAt.Add(correlationWindow)
	if now.Before(deadline) {
		return Decision{Kind: KindScheduleRetry}
	}
	return Decision{Kind: KindPublishDlq}
}
