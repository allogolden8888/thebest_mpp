package kafkaio

import (
	"testing"

	"google.golang.org/protobuf/proto"

	commonv1 "mpp/platformcontracts/common/v1"
	eventsv1 "mpp/platformcontracts/events/v1"
)

func TestDecodeConsentEventReturnsNilForOtherEntityTypes(t *testing.T) {
	payload, _ := proto.Marshal(&eventsv1.ConfigChangeEvent{
		EntityType: commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_PARTNER,
		EntityId:   "acme",
	})
	event, err := DecodeConsentEvent(payload)
	if err != nil {
		t.Fatalf("не должно быть ошибки на другом entity_type: %v", err)
	}
	if event != nil {
		t.Fatalf("ожидали nil для entity_type != subscriber_consent")
	}
}

func TestDecodeConsentEventReturnsEventForSubscriberConsent(t *testing.T) {
	payload, _ := proto.Marshal(&eventsv1.ConfigChangeEvent{
		EntityType: commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_SUBSCRIBER_CONSENT,
		EntityId:   "998901234567:CATEGORY:ADVERTISING:SMS",
	})
	event, err := DecodeConsentEvent(payload)
	if err != nil {
		t.Fatalf("DecodeConsentEvent failed: %v", err)
	}
	if event == nil {
		t.Fatalf("ожидали непустое событие для subscriber_consent")
	}
}

func TestDecodeConsentEventRejectsMissingEntityID(t *testing.T) {
	payload, _ := proto.Marshal(&eventsv1.ConfigChangeEvent{
		EntityType: commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_SUBSCRIBER_CONSENT,
	})
	_, err := DecodeConsentEvent(payload)
	if err == nil {
		t.Fatalf("ожидали ошибку без entity_id")
	}
}

func TestDecodeConsentEventRejectsGarbage(t *testing.T) {
	_, err := DecodeConsentEvent([]byte{0xff, 0x00, 0xff, 0x01})
	if err == nil {
		t.Fatalf("ожидали ошибку разбора мусорных байтов")
	}
}