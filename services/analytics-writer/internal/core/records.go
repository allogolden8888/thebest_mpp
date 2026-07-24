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
// широкая денормализованная таблица, см. store/schema.go).
type NormalizedRecord struct {
	EventType     string // "incoming" | "stage_completed" | "lifecycle"
	MessageID     string
	PartnerID     string
	StageName     string
	Outcome       string
	ReasonCode    string
	LifecycleStatus string
	OccurredAt    time.Time
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

func FromIncomingMessage(msg *eventsv1.IncomingMessage) NormalizedRecord {
	return NormalizedRecord{
		EventType:  "incoming",
		MessageID:  msg.GetMessageId(),
		PartnerID:  msg.GetPartnerId(),
		OccurredAt: msg.GetReceivedAt().AsTime(),
	}
}

func FromStageCompleted(event *commonv1.StageCompletedEvent) NormalizedRecord {
	return NormalizedRecord{
		EventType:  "stage_completed",
		MessageID:  event.GetMessageId(),
		StageName:  stageNameString(event.GetStageName()),
		Outcome:    outcomeString(event.GetOutcome()),
		ReasonCode: event.GetReasonCode(),
		OccurredAt: event.GetCompletedAt().AsTime(),
	}
}

func FromLifecycleEvent(event *eventsv1.MessageLifecycleEvent) NormalizedRecord {
	return NormalizedRecord{
		EventType:       "lifecycle",
		MessageID:       event.GetMessageId(),
		LifecycleStatus: lifecycleStatusString(event.GetStatus()),
		OccurredAt:      event.GetOccurredAt().AsTime(),
	}
}