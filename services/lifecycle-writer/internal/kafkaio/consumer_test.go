package kafkaio

import (
	"testing"

	"google.golang.org/protobuf/proto"

	eventsv1 "mpp/platformcontracts/events/v1"
)

func TestDecodeIncomingMessageRoundTrips(t *testing.T) {
	payload, _ := proto.Marshal(&eventsv1.IncomingMessage{MessageId: "msg-1"})
	msg, err := DecodeIncomingMessage(payload)
	if err != nil {
		t.Fatalf("DecodeIncomingMessage failed: %v", err)
	}
	if msg.GetMessageId() != "msg-1" {
		t.Fatalf("round-trip mismatch")
	}
}

func TestDecodeLifecycleEventRoundTrips(t *testing.T) {
	payload, _ := proto.Marshal(&eventsv1.MessageLifecycleEvent{MessageId: "msg-1", LifecycleVersion: 2})
	event, err := DecodeLifecycleEvent(payload)
	if err != nil {
		t.Fatalf("DecodeLifecycleEvent failed: %v", err)
	}
	if event.GetLifecycleVersion() != 2 {
		t.Fatalf("round-trip mismatch")
	}
}

func TestDecodeDlqRecordRoundTrips(t *testing.T) {
	payload, _ := proto.Marshal(&eventsv1.DlqRecord{StageExecutionId: "exec-1"})
	rec, err := DecodeDlqRecord(payload)
	if err != nil {
		t.Fatalf("DecodeDlqRecord failed: %v", err)
	}
	if rec.GetStageExecutionId() != "exec-1" {
		t.Fatalf("round-trip mismatch")
	}
}

func TestDecodeRejectsGarbage(t *testing.T) {
	if _, err := DecodeIncomingMessage([]byte{0xff, 0x00, 0xff}); err == nil {
		t.Fatalf("ожидали ошибку разбора мусорных байтов")
	}
}