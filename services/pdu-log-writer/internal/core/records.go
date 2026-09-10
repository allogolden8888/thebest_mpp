// Package core — чистые функции, строящие PduLogRecord из OperatorPduLog
// (BACKOFFICE_DESIGN_SPEC.md Экраны 38-40, тот же принцип, что
// analytics-writer/internal/core: on_event, без побочных эффектов).
package core

import (
	"time"

	commonv1 "mpp/platformcontracts/common/v1"
	eventsv1 "mpp/platformcontracts/events/v1"
)

// PduLogRecord — одна строка analytics.operator_pdu_log (ClickHouse, см.
// internal/store/store.go). Отдельная от analytics.stage_events таблица
// analytics-writer: разная форма (тут нет stage_name/outcome/
// lifecycle_status, зато есть direction/pdu_type/sequence_number/
// smsc_message_id) и на порядок другой объём (по PDU, не по сообщению).
type PduLogRecord struct {
	OperatorID       string
	Protocol         string
	Direction        string // "A2P" | "DLR"
	PduType          string // "SUBMIT_SM" | "SUBMIT_SM_RESP" | "DELIVER_SM" | "DELIVER_SM_RESP"
	SequenceNumber   int32
	MessageID        string
	StageExecutionID string
	SmscMessageID    string
	SegmentID        int32
	Status           string
	OccurredAt       time.Time
}

func protocolString(p commonv1.Protocol) string {
	switch p {
	case commonv1.Protocol_PROTOCOL_SMPP:
		return "SMPP"
	case commonv1.Protocol_PROTOCOL_HTTP:
		return "HTTP"
	default:
		return "UNSPECIFIED"
	}
}

func directionString(d eventsv1.PduDirection) string {
	switch d {
	case eventsv1.PduDirection_PDU_DIRECTION_A2P:
		return "A2P"
	case eventsv1.PduDirection_PDU_DIRECTION_DLR:
		return "DLR"
	default:
		return "UNSPECIFIED"
	}
}

// FromOperatorPduLog — event_id для дедупликации (ReplacingMergeTree, см.
// store.go) строится из (operator_id, sequence_number, pdu_type,
// occurred_at): sequence_number один переиспользуется по кругу внутри
// сессии, но в паре с occurred_at (миллисекунды) и pdu_type коллизия при
// redelivery того же самого события практически исключена, а именно
// redelivery (at-least-once Kafka) — единственный случай дублирования,
// который дедуп должен ловить.
func FromOperatorPduLog(event *eventsv1.OperatorPduLog) PduLogRecord {
	return PduLogRecord{
		OperatorID:       event.GetOperatorId(),
		Protocol:         protocolString(event.GetProtocol()),
		Direction:        directionString(event.GetDirection()),
		PduType:          event.GetPduType(),
		SequenceNumber:   event.GetSequenceNumber(),
		MessageID:        event.GetMessageId(),
		StageExecutionID: event.GetStageExecutionId(),
		SmscMessageID:    event.GetSmscMessageId(),
		SegmentID:        event.GetSegmentId(),
		Status:           event.GetStatus(),
		OccurredAt:       event.GetOccurredAt().AsTime(),
	}
}
