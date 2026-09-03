// Package core — on_event (service_internal_methods.md §6.2): чистые
// функции, строящие NormalizedRecord из incoming.messages/stage.completed/
// message.lifecycle — детальная пер-стадийная история (services_specifictaion.md
// §7.2), в отличие от Lifecycle Writer, который stage.completed не читает.
package core

import (
	"time"

	commonv1 "mpp/platformcontracts/common/v1"
	eventsv1 "mpp/platformcontracts/events/v1"
)

// NormalizedRecord — одна строка analytics.stage_events (ClickHouse,
// широкая денормализованная таблица, см. store/store.go).
//
// EventID — CODE_REVIEW.md MEDIUM/HIGH finding: raw-таблица не имела
// никакого dedup-ключа (`MergeTree()`, без `ReplacingMergeTree`/version
// column), а redelivery при at-least-once Kafka (обычное дело на
// rebalance/restart) вставляла событие второй раз как новую строку,
// молча раздувая `count()`-агрегаты, которые Backoffice/Partner API
// report-запросы строят прямо по этой таблице. EventID — стабильный
// идентификатор конкретного события (не сообщения в целом), одинаковый
// при повторной доставке того же самого события: для incoming —
// message_id (одно "incoming"-событие на сообщение), для stage_completed
// — stage_execution_id (тот же ключ идемпотентности, что используется
// по всей платформе), для lifecycle — event_id из самого события. См.
// store.go — таблица теперь ReplacingMergeTree по (occurred_at, event_id,
// message_id), а Report-запросы в backoffice-api/partner-api читают её
// с FINAL, чтобы дубликаты не попадали в агрегаты на чтении.
type NormalizedRecord struct {
	EventType       string // "incoming" | "stage_completed" | "lifecycle"
	EventID         string
	MessageID       string
	PartnerID       string
	StageName       string
	Outcome         string
	ReasonCode      string
	LifecycleStatus string
	OccurredAt      time.Time
}

func stageNameString(s commonv1.StageName) string {
	switch s {
	case commonv1.StageName_STAGE_NAME_DESTINATION_RESOLUTION:
		return "DESTINATION_RESOLUTION"
	case commonv1.StageName_STAGE_NAME_POLICY:
		return "POLICY"
	case commonv1.StageName_STAGE_NAME_BILLING:
		return "BILLING"
	case commonv1.StageName_STAGE_NAME_ROUTING:
		return "ROUTING"
	case commonv1.StageName_STAGE_NAME_DELIVERY:
		return "DELIVERY"
	case commonv1.StageName_STAGE_NAME_DELIVERY_RECONCILIATION:
		return "DELIVERY_RECONCILIATION"
	default:
		return "UNSPECIFIED"
	}
}

func outcomeString(o commonv1.Outcome) string {
	switch o {
	case commonv1.Outcome_OUTCOME_SUCCEEDED:
		return "SUCCEEDED"
	case commonv1.Outcome_OUTCOME_REJECTED:
		return "REJECTED"
	case commonv1.Outcome_OUTCOME_FAILED:
		return "FAILED"
	case commonv1.Outcome_OUTCOME_TIMED_OUT:
		return "TIMED_OUT"
	case commonv1.Outcome_OUTCOME_RETRY_EXHAUSTED:
		return "RETRY_EXHAUSTED"
	case commonv1.Outcome_OUTCOME_SUBMISSION_OUTCOME_UNKNOWN:
		return "SUBMISSION_OUTCOME_UNKNOWN"
	case commonv1.Outcome_OUTCOME_DELIVERY_UNRESOLVED:
		return "DELIVERY_UNRESOLVED"
	default:
		return "UNSPECIFIED"
	}
}

func lifecycleStatusString(status commonv1.MessageLifecycleStatus) string {
	switch status {
	case commonv1.MessageLifecycleStatus_MESSAGE_LIFECYCLE_STATUS_SUBMITTED:
		return "SUBMITTED"
	case commonv1.MessageLifecycleStatus_MESSAGE_LIFECYCLE_STATUS_DELIVERED:
		return "DELIVERED"
	case commonv1.MessageLifecycleStatus_MESSAGE_LIFECYCLE_STATUS_UNDELIVERABLE:
		return "UNDELIVERABLE"
	case commonv1.MessageLifecycleStatus_MESSAGE_LIFECYCLE_STATUS_DELIVERY_UNRESOLVED:
		return "DELIVERY_UNRESOLVED"
	case commonv1.MessageLifecycleStatus_MESSAGE_LIFECYCLE_STATUS_LATE_DELIVERY_CONFIRMED:
		return "LATE_DELIVERY_CONFIRMED"
	case commonv1.MessageLifecycleStatus_MESSAGE_LIFECYCLE_STATUS_REJECTED:
		return "REJECTED"
	case commonv1.MessageLifecycleStatus_MESSAGE_LIFECYCLE_STATUS_FAILED:
		return "FAILED"
	case commonv1.MessageLifecycleStatus_MESSAGE_LIFECYCLE_STATUS_SYSTEM_UNAVAILABLE:
		return "SYSTEM_UNAVAILABLE"
	default:
		return "UNSPECIFIED"
	}
}

// FromIncomingMessage — event_id = message_id: ровно одно "incoming"
// событие публикуется на сообщение, так что message_id уже однозначно
// идентифицирует его для дедупликации при redelivery.
func FromIncomingMessage(msg *eventsv1.IncomingMessage) NormalizedRecord {
	return NormalizedRecord{
		EventType:  "incoming",
		EventID:    msg.GetMessageId(),
		MessageID:  msg.GetMessageId(),
		PartnerID:  msg.GetPartnerId(),
		OccurredAt: msg.GetReceivedAt().AsTime(),
	}
}

func FromStageCompleted(event *commonv1.StageCompletedEvent) NormalizedRecord {
	return FromStageCompletedAt(event, time.Time{})
}

// FromStageCompletedAt — то же, но с fallback-временем (таймстамп самой
// Kafka-записи), когда payload не несёт completed_at.
//
// Исторически НИ ОДИН из шести сервисов-стадий не заполнял
// StageCompletedEvent.completed_at — поле было объявлено в контракте и
// оставалось protobuf-нулём, из-за чего все пер-стадийные строки в
// ClickHouse ложились с occurred_at = 1970-01-01 и пер-стадийная
// хронология не вычислялась в принципе. Сейчас все шесть продюсеров
// заполняют его по-настоящему, и эта ветка в норме не срабатывает —
// оставлена как защита от старых событий, которые ещё лежат в топике с
// прошлой ретенцией, и от возможного будущего продюсера, который снова
// забудет это поле.
//
// Таймстамп Kafka-записи ставится продюсером в момент отправки события,
// то есть в пределах миллисекунд от фактического завершения стадии — для
// измерения длительности стадий этого достаточно. Это ЯВНЫЙ fallback, а
// не замена: правильное исправление — заполнить completed_at во всех
// шести продюсерах, тогда эта ветка перестанет срабатывать сама собой
// (payload-значение всегда имеет приоритет).
func FromStageCompletedAt(event *commonv1.StageCompletedEvent, fallback time.Time) NormalizedRecord {
	occurredAt := event.GetCompletedAt().AsTime()
	// protobuf-ноль -> 1970-01-01. Именно его и подменяем; любое реальное
	// значение из payload побеждает fallback.
	if occurredAt.Unix() <= 0 && !fallback.IsZero() {
		occurredAt = fallback
	}
	return NormalizedRecord{
		EventType: "stage_completed",
		EventID:   event.GetEventId(),
		MessageID: event.GetMessageId(),
		// partner_id теперь приходит в самом событии (верхнеуровневое поле
		// StageCompletedEvent, эхо StageExecuteCommand.partner_id). Раньше
		// его в контракте не было вообще, поэтому все stage_completed-строки
		// в ClickHouse писались с пустым partner_id и отчёты не
		// группировались по партнёру.
		PartnerID:  event.GetPartnerId(),
		StageName:  stageNameString(event.GetStageName()),
		Outcome:    outcomeString(event.GetOutcome()),
		ReasonCode: event.GetReasonCode(),
		OccurredAt: occurredAt,
	}
}

func FromLifecycleEvent(event *eventsv1.MessageLifecycleEvent) NormalizedRecord {
	return NormalizedRecord{
		EventType:       "lifecycle",
		EventID:         event.GetEventId(),
		MessageID:       event.GetMessageId(),
		LifecycleStatus: lifecycleStatusString(event.GetStatus()),
		OccurredAt:      event.GetOccurredAt().AsTime(),
	}
}