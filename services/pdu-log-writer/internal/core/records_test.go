package core

import (
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "mpp/platformcontracts/common/v1"
	eventsv1 "mpp/platformcontracts/events/v1"
)

func TestFromOperatorPduLogA2P(t *testing.T) {
	occurredAt := time.Date(2026, 9, 10, 11, 31, 38, 0, time.UTC)
	event := &eventsv1.OperatorPduLog{
		OperatorId:       "beeline_uz",
		Protocol:         commonv1.Protocol_PROTOCOL_SMPP,
		Direction:        eventsv1.PduDirection_PDU_DIRECTION_A2P,
		PduType:          "SUBMIT_SM_RESP",
		SequenceNumber:   42,
		MessageId:        "msg-1",
		StageExecutionId: "stage-exec-1",
		SmscMessageId:    "dkr87lit9o7b",
		SegmentId:        1,
		Status:           "OK",
		OccurredAt:       timestamppb.New(occurredAt),
	}

	record := FromOperatorPduLog(event)

	if record.OperatorID != "beeline_uz" || record.Protocol != "SMPP" || record.Direction != "A2P" ||
		record.PduType != "SUBMIT_SM_RESP" || record.SequenceNumber != 42 || record.MessageID != "msg-1" ||
		record.StageExecutionID != "stage-exec-1" || record.SmscMessageID != "dkr87lit9o7b" ||
		record.SegmentID != 1 || record.Status != "OK" || !record.OccurredAt.Equal(occurredAt) {
		t.Fatalf("неверная нормализация A2P-события: %+v", record)
	}
}

func TestFromOperatorPduLogDlrHasEmptyMessageCorrelation(t *testing.T) {
	// message_id/stage_execution_id НЕ известны на уровне DLR PDU — тот же
	// барьер, что уже задокументирован у OperatorDlr (единственный ключ
	// там smsc_message_id).
	event := &eventsv1.OperatorPduLog{
		OperatorId:     "ucell_uz",
		Direction:      eventsv1.PduDirection_PDU_DIRECTION_DLR,
		PduType:        "DELIVER_SM",
		SmscMessageId:  "dkr87lit9o7b",
		Status:         "DELIVRD",
		OccurredAt:     timestamppb.Now(),
	}

	record := FromOperatorPduLog(event)

	if record.Direction != "DLR" || record.MessageID != "" || record.StageExecutionID != "" {
		t.Fatalf("DLR-запись не должна нести message_id/stage_execution_id: %+v", record)
	}
	if record.Status != "DELIVRD" {
		t.Fatalf("ожидали сырой DLR stat DELIVRD, получили %q", record.Status)
	}
}
