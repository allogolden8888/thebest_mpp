package kafkaio

import (
	"testing"

	"google.golang.org/protobuf/proto"

	commonv1 "mpp/platformcontracts/common/v1"
	eventsv1 "mpp/platformcontracts/events/v1"
)

func TestDecodeIncomingMessageRoundTrips(t *testing.T) {
	payload, _ := proto.Marshal(&eventsv1.IncomingMessage{MessageId: "msg-1"})
	msg, err := DecodeIncomingMessage(payload)
	if err != nil || msg.GetMessageId() != "msg-1" {
		t.Fatalf("round-trip failed: %v", err)
	}
}

func TestDecodeStageCompletedRoundTrips(t *testing.T) {
	payload, _ := proto.Marshal(&commonv1.StageCompletedEvent{MessageId: "msg-1", StageName: commonv1.StageName_STAGE_NAME_BILLING})
	event, err := DecodeStageCompleted(payload)
	if err != nil || event.GetStageName() != commonv1.StageName_STAGE_NAME_BILLING {
		t.Fatalf("round-trip failed: %v", err)
	}
}

func TestDecodeLifecycleEventRoundTrips(t *testing.T) {
	payload, _ := proto.Marshal(&eventsv1.MessageLifecycleEvent{MessageId: "msg-1"})
	event, err := DecodeLifecycleEvent(payload)
	if err != nil || event.GetMessageId() != "msg-1" {
		t.Fatalf("round-trip failed: %v", err)
	}
}

func TestDecodeRejectsGarbage(t *testing.T) {
	if _, err := DecodeStageCompleted([]byte{0xff, 0x00, 0xff}); err == nil {
		t.Fatalf("ожидали ошибку разбора мусорных байтов")
	}
}