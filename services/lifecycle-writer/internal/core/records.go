// Package core — on_event (service_internal_methods.md §6.1): чистые
// функции, строящие NormalizedRecord из incoming.messages/message.lifecycle/
// DLQ-топиков (stage.completed сознательно не читается — детальная история
// идёт в ClickHouse через Analytics Writer, services_specifictaion.md §7.1).
package core

import (
	"time"

	commonv1 "mpp/platformcontracts/common/v1"
	eventsv1 "mpp/platformcontracts/events/v1"
)

// ReadModelRow — messaging.message_read_model (migrations/V004).
type ReadModelRow struct {
	MessageID       string
	PartnerID       string
	ApplicationID   string
	TraceID         string
	PipelineID      string
	PipelineVersion string
	CurrentStatus   string
	Terminal        bool
	Timestamp       time.Time
}

// LifecycleHistoryRow — messaging.message_lifecycle_history (migrations/V005).
type LifecycleHistoryRow struct {
	MessageID        string
	LifecycleVersion int64
	Status           string
	EventID          string
	OccurredAt       time.Time
	Source           string
}

// DlqRow — messaging.dlq_record (migrations/V006).
type DlqRow struct {
	StageExecutionID string
	MessageID        string
	StageName        string
	Attempt          int32
	OriginalCommand  []byte
	ReasonCode       string
	ErrorDetail      string
	CreatedAt        time.Time
}

// FromIncomingMessage — строка read model на первое появление сообщения.
//
// **Открытый вопрос**: IncomingMessage (platform-contracts/events/message_events.proto)
// не несёт pipeline_id/pipeline_version — они резолвятся Pipeline Engine
// после приёма (resolve_pipeline_version, service_internal_methods.md §1.4),
// не публикуются обратно в отдельном событии, которое Lifecycle Writer мог
// бы прочитать. message_read_model.pipeline_id/pipeline_version — NOT NULL
// (migrations/V004) — здесь заполняются пустой строкой как placeholder,
// не выдуманным значением, см. README "Открытый вопрос".
func FromIncomingMessage(msg *eventsv1.IncomingMessage) ReadModelRow {
	return ReadModelRow{
		MessageID:       msg.GetMessageId(),
		PartnerID:       msg.GetPartnerId(),
		ApplicationID:   msg.GetApplicationId(),
		TraceID:         msg.GetTraceId(),
		PipelineID:      "",
		PipelineVersion: "",
		CurrentStatus:   "RECEIVED",
		Terminal:        false,
		Timestamp:       msg.GetReceivedAt().AsTime(),
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

// FromLifecycleEvent — обновление read model (current_status/terminal) + новая строка истории.
func FromLifecycleEvent(event *eventsv1.MessageLifecycleEvent) (ReadModelUpdate, LifecycleHistoryRow) {
	status := lifecycleStatusString(event.GetStatus())
	occurredAt := event.GetOccurredAt().AsTime()

	update := ReadModelUpdate{
		MessageID:        event.GetMessageId(),
		CurrentStatus:    status,
		Terminal:         event.GetTerminal(),
		UpdatedAt:        occurredAt,
		LifecycleVersion: event.GetLifecycleVersion(),
	}
	history := LifecycleHistoryRow{
		MessageID:        event.GetMessageId(),
		LifecycleVersion: event.GetLifecycleVersion(),
		Status:           status,
		EventID:          event.GetEventId(),
		OccurredAt:       occurredAt,
		Source:           "message.lifecycle",
	}
	return update, history
}

// ReadModelUpdate — частичное обновление message_read_model по message.lifecycle.
//
// LifecycleVersion — CODE_REVIEW.md MEDIUM finding: без него UpdateReadModel
// не может отличить редоставленное старое событие (после rebalance) от
// настоящего нового — см. store.Store.UpdateReadModel/BatchUpdateReadModel.
type ReadModelUpdate struct {
	MessageID        string
	CurrentStatus    string
	Terminal         bool
	UpdatedAt        time.Time
	LifecycleVersion int64
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

// FromDlqRecord — messaging.dlq_record строка из DlqRecord (stage.*.dlq).
func FromDlqRecord(rec *eventsv1.DlqRecord) (DlqRow, error) {
	original, err := marshalOriginalCommand(rec)
	if err != nil {
		return DlqRow{}, err
	}
	return DlqRow{
		StageExecutionID: rec.GetStageExecutionId(),
		MessageID:        rec.GetMessageId(),
		StageName:        stageNameString(rec.GetStageName()),
		Attempt:          rec.GetAttempt(),
		OriginalCommand:  original,
		ReasonCode:       rec.GetReasonCode(),
		ErrorDetail:      rec.GetErrorDetail(),
		CreatedAt:        rec.GetCreatedAt().AsTime(),
	}, nil
}