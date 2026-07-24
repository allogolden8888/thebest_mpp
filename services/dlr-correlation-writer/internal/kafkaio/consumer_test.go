package kafkaio

import (
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "mpp/platformcontracts/common/v1"
	eventsv1 "mpp/platformcontracts/events/v1"
)

func validEvent() *eventsv1.OperatorSubmitAccepted {
	now := time.Now().UTC().Truncate(time.Second)
	return &eventsv1.OperatorSubmitAccepted{
		MessageId:            "m1",
		StageExecutionId:     "se1",
		OperatorId:           "beeline",
		Protocol:             commonv1.Protocol_PROTOCOL_SMPP,
		SmscMessageId:        "smsc-123",
		SegmentId:            1,
		SubmittedAt:          timestamppb.New(now),
		CorrelationExpiresAt: timestamppb.New(now.Add(48 * time.Hour)),
	}
}

func TestDecodeValidEventRoundTrips(t *testing.T) {
	event := validEvent()
	payload, err := proto.Marshal(event)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	record, err := DecodeOperatorSubmitAccepted(payload)
	if err != nil {
		t.Fatalf("DecodeOperatorSubmitAccepted: %v", err)
	}
	if record.OperatorID != "beeline" {
		t.Errorf("OperatorID = %q, want beeline", record.OperatorID)
	}
	if record.SmscMessageID != "smsc-123" {
		t.Errorf("SmscMessageID = %q, want smsc-123", record.SmscMessageID)
	}
	if record.MessageID != "m1" || record.StageExecutionID != "se1" {
		t.Errorf("MessageID/StageExecutionID = %q/%q, want m1/se1", record.MessageID, record.StageExecutionID)
	}
	if record.SegmentID != 1 {
		t.Errorf("SegmentID = %d, want 1", record.SegmentID)
	}
	if !record.ExpiresAt.After(record.SubmittedAt) {
		t.Errorf("ExpiresAt (%v) должен быть после SubmittedAt (%v)", record.ExpiresAt, record.SubmittedAt)
	}
}

func TestDecodeEmptySmscMessageIdIsAcceptedNotRejected(t *testing.T) {
	// "не все операторы возвращают его синхронно" (operator_events.proto) —
	// пустая строка легитимна, не ошибка декодирования.
	event := validEvent()
	event.SmscMessageId = ""
	payload, _ := proto.Marshal(event)

	record, err := DecodeOperatorSubmitAccepted(payload)
	if err != nil {
		t.Fatalf("пустой smsc_message_id не должен быть ошибкой декодирования: %v", err)
	}
	if record.SmscMessageID != "" {
		t.Errorf("SmscMessageID = %q, want пустую строку", record.SmscMessageID)
	}
}

func TestDecodeRejectsMissingOperatorId(t *testing.T) {
	event := validEvent()
	event.OperatorId = ""
	payload, _ := proto.Marshal(event)

	if _, err := DecodeOperatorSubmitAccepted(payload); err == nil {
		t.Fatal("ожидали ошибку на отсутствующий operator_id")
	}
}

func TestDecodeRejectsMissingMessageId(t *testing.T) {
	event := validEvent()
	event.MessageId = ""
	payload, _ := proto.Marshal(event)

	if _, err := DecodeOperatorSubmitAccepted(payload); err == nil {
		t.Fatal("ожидали ошибку на отсутствующий message_id")
	}
}

func TestDecodeRejectsMissingStageExecutionId(t *testing.T) {
	event := validEvent()
	event.StageExecutionId = ""
	payload, _ := proto.Marshal(event)

	if _, err := DecodeOperatorSubmitAccepted(payload); err == nil {
		t.Fatal("ожидали ошибку на отсутствующий stage_execution_id")
	}
}

func TestDecodeRejectsGarbageBytes(t *testing.T) {
	if _, err := DecodeOperatorSubmitAccepted([]byte{0xFF, 0xFE, 0x00, 0x01}); err == nil {
		t.Fatal("ожидали ошибку unmarshal на мусорные байты")
	}
}
