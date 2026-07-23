package kafkaio

import (
	"testing"

	"google.golang.org/protobuf/proto"

	commonv1 "mpp/platformcontracts/common/v1"
	eventsv1 "mpp/platformcontracts/events/v1"
)

func TestDecodeConfigChangeEventRoundTrips(t *testing.T) {
	original := &eventsv1.ConfigChangeEvent{
		EntityType:  commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_PARTNER,
		EntityId:    "acme",
		Version:     1,
		PayloadJson: []byte(`{}`),
		Status:      "active",
	}
	payload, err := proto.Marshal(original)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	got, err := DecodeConfigChangeEvent(payload)
	if err != nil {
		t.Fatalf("DecodeConfigChangeEvent failed: %v", err)
	}
	if got.GetEntityId() != "acme" || got.GetVersion() != 1 {
		t.Fatalf("round-trip mismatch: %+v", got)
	}
}

func TestDecodeConfigChangeEventRejectsMissingEntityID(t *testing.T) {
	payload, _ := proto.Marshal(&eventsv1.ConfigChangeEvent{Version: 1})
	_, err := DecodeConfigChangeEvent(payload)
	if err == nil {
		t.Fatalf("ожидали ошибку для записи без entity_id")
	}
}

func TestDecodeConfigChangeEventRejectsGarbage(t *testing.T) {
	_, err := DecodeConfigChangeEvent([]byte{0xff, 0x00, 0xff, 0x01})
	if err == nil {
		t.Fatalf("ожидали ошибку разбора мусорных байтов")
	}
}
