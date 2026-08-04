package kafkaio

import (
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "mpp/platformcontracts/common/v1"
	eventsv1 "mpp/platformcontracts/events/v1"

	"mpp/operator-http-gateway/internal/webhook"
)

// BuildSubmitAcceptedEvent — чистая функция.
func BuildSubmitAcceptedEvent(messageID, stageExecutionID, operatorID, smscMessageID string, segmentID int32, now time.Time) *eventsv1.OperatorSubmitAccepted {
	return &eventsv1.OperatorSubmitAccepted{
		MessageId:        messageID,
		StageExecutionId: stageExecutionID,
		OperatorId:       operatorID,
		Protocol:         commonv1.Protocol_PROTOCOL_HTTP,
		SmscMessageId:    smscMessageID,
		SegmentId:        segmentID,
		SubmittedAt:      timestamppb.New(now),
	}
}

// BuildDlrEvent — normalize_and_publish_dlr: тот же формат, что сырой SMPP DLR.
func BuildDlrEvent(operatorID string, dlr webhook.RawDlr, now time.Time) *eventsv1.OperatorDlr {
	return &eventsv1.OperatorDlr{
		OperatorId:    operatorID,
		Protocol:      commonv1.Protocol_PROTOCOL_HTTP,
		SmscMessageId: dlr.SmscMessageID,
		SegmentId:     dlr.SegmentID,
		RawStatus:     dlr.RawStatus,
		ReceivedAt:    timestamppb.New(now),
	}
}