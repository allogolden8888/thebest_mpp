package kafkaio

import (
	"testing"
	"time"

	commonv1 "mpp/platformcontracts/common/v1"

	"mpp/config-event-publisher/internal/outbox"
)

func TestBuildConfigChangeEventMapsFieldsCorrectly(t *testing.T) {
	createdAt := time.Date(2026, 7, 23, 10, 0, 0, 0, time.UTC)
	e := outbox.Entry{
		ID: 1, EntityType: "partner", EntityID: "acme", Payload: []byte(`{"partner_id":"acme"}`),
		Version: 3, Status: "active", CreatedAt: createdAt,
	}

	event, err := BuildConfigChangeEvent(e)
	if err != nil {
		t.Fatalf("BuildConfigChangeEvent failed: %v", err)
	}
	if event.GetEntityType() != commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_PARTNER {
		t.Fatalf("неверный entity_type: %v", event.GetEntityType())
	}
	if event.GetEntityId() != "acme" || event.GetVersion() != 3 || event.GetStatus() != "active" {
		t.Fatalf("неверные поля: %+v", event)
	}
	if !event.GetCreatedAt().AsTime().Equal(createdAt) {
		t.Fatalf("created_at не совпадает: %v", event.GetCreatedAt().AsTime())
	}
	if string(event.GetPayloadJson()) != `{"partner_id":"acme"}` {
		t.Fatalf("payload_json не совпадает: %s", event.GetPayloadJson())
	}
}

func TestBuildConfigChangeEventErrorsOnUnknownEntityType(t *testing.T) {
	e := outbox.Entry{ID: 1, EntityType: "not_a_real_type", EntityID: "x"}
	_, err := BuildConfigChangeEvent(e)
	if err == nil {
		t.Fatalf("ожидали ошибку для неизвестного entity_type")
	}
}

func TestBuildConfigChangeEventAllKnownEntityTypesMap(t *testing.T) {
	known := []string{
		"pipeline", "policy_ruleset", "policy_template", "billing_tariff",
		"routing_table", "number_range", "partner", "operator", "subscriber_consent",
	}
	for _, et := range known {
		event, err := BuildConfigChangeEvent(outbox.Entry{EntityType: et, EntityID: "x"})
		if err != nil {
			t.Fatalf("entity_type %q должен маппиться без ошибки: %v", et, err)
		}
		if event.GetEntityType() == commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_UNSPECIFIED {
			t.Fatalf("entity_type %q не должен маппиться в UNSPECIFIED", et)
		}
	}
}