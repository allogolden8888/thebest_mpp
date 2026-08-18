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
		payload := []byte(`{}`)
		if et == "policy_template" || et == "subscriber_consent" {
			// Оба резолвят status из payload_json, не из config_versions
			// (см. TestResolveStatus*) — для этого теста достаточно
			// валидного payload, не сам факт резолвинга.
			payload = []byte(`{"status":"active"}`)
		}
		event, err := BuildConfigChangeEvent(outbox.Entry{EntityType: et, EntityID: "x", Payload: payload})
		if err != nil {
			t.Fatalf("entity_type %q должен маппиться без ошибки: %v", et, err)
		}
		if event.GetEntityType() == commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_UNSPECIFIED {
			t.Fatalf("entity_type %q не должен маппиться в UNSPECIFIED", et)
		}
	}
}

// CODE_REVIEW.md Critical finding: COALESCE(cv.status, 'active') in
// PollOutbox's SQL always resolved to 'active' for policy_template /
// subscriber_consent (config_version_id is always NULL for them), so a
// consent revocation could never publish as status=archived. Fixed via
// ResolveStatus — these tests exercise the orchestration logic directly
// (previously untestable in principle, per CODE_REVIEW.md test-quality note).

func TestResolveStatusUsesConfigVersionsStatusWhenPresent(t *testing.T) {
	status, unresolved, err := ResolveStatus(outbox.Entry{EntityType: "partner", Status: "archived"})
	if err != nil {
		t.Fatalf("ResolveStatus failed: %v", err)
	}
	if unresolved {
		t.Fatalf("status пришёл из config_versions — unresolved должен быть false")
	}
	if status != "archived" {
		t.Fatalf("ожидали status=archived, получили %q", status)
	}
}

func TestResolveStatusReadsPolicyTemplateStatusFromPayload(t *testing.T) {
	status, unresolved, err := ResolveStatus(outbox.Entry{
		EntityType: "policy_template",
		Status:     "", // config_version_id всегда NULL для policy_template
		Payload:    []byte(`{"template_id":"x","status":"archived"}`),
	})
	if err != nil {
		t.Fatalf("ResolveStatus failed: %v", err)
	}
	if unresolved {
		t.Fatalf("policy_template status читается из payload_json — должен быть настоящим фиксом, unresolved=false")
	}
	if status != "archived" {
		t.Fatalf("ожидали status=archived из payload_json, получили %q — старый баг: COALESCE(cv.status,'active') всегда давал 'active'", status)
	}
}

func TestResolveStatusRejectsMalformedPolicyTemplatePayload(t *testing.T) {
	_, _, err := ResolveStatus(outbox.Entry{
		EntityType: "policy_template",
		Payload:    []byte(`{"template_id":"x"}`), // без status
	})
	if err == nil {
		t.Fatalf("ожидали ошибку для policy_template payload без status")
	}
}

// TestResolveStatusReadsSubscriberConsentStatusFromPayload — Фаза 6 плана
// закрытия API-пробелов (compliance-api): раньше subscriber_consent
// payload_json не содержал поля status (revocation была структурно
// недостижима, см. package doc ResolveStatus), теперь схема требует его —
// тот же фикс, что уже был сделан для policy_template.
func TestResolveStatusReadsSubscriberConsentStatusFromPayload(t *testing.T) {
	status, unresolved, err := ResolveStatus(outbox.Entry{
		EntityType: "subscriber_consent",
		Status:     "",
		Payload:    []byte(`{"msisdn":"998901234567","scope_type":"CATEGORY","scope_value":"ADVERTISING","channel":"SMS","status":"archived"}`),
	})
	if err != nil {
		t.Fatalf("ResolveStatus failed: %v", err)
	}
	if unresolved {
		t.Fatalf("subscriber_consent status теперь читается из payload_json — должен быть настоящим фиксом, unresolved=false")
	}
	if status != "archived" {
		t.Fatalf("ожидали status=archived из payload_json, получили %q — старый баг: revocation была структурно недостижима", status)
	}
}

func TestResolveStatusRejectsMalformedSubscriberConsentPayload(t *testing.T) {
	_, _, err := ResolveStatus(outbox.Entry{
		EntityType: "subscriber_consent",
		Payload:    []byte(`{"msisdn":"998901234567","scope_type":"CATEGORY","scope_value":"ADVERTISING","channel":"SMS"}`), // без status
	})
	if err == nil {
		t.Fatalf("ожидали ошибку для subscriber_consent payload без status")
	}
}

func TestBuildConfigChangeEventPublishesSubscriberConsentStatusFromPayload(t *testing.T) {
	// Фаза 6 (compliance-api): status теперь обязателен в payload_json
	// (config_schemas/subscriber_consent.schema.json), больше не тихий
	// дефолт "active" — событие несёт реальное намерение вызывающей
	// стороны, archived реально публикуется как archived.
	event, err := BuildConfigChangeEvent(outbox.Entry{
		EntityType: "subscriber_consent",
		EntityID:   "998901234567:CATEGORY:ADVERTISING:SMS",
		Payload:    []byte(`{"msisdn":"998901234567","scope_type":"CATEGORY","scope_value":"ADVERTISING","channel":"SMS","status":"archived"}`),
	})
	if err != nil {
		t.Fatalf("BuildConfigChangeEvent failed: %v", err)
	}
	if event.GetStatus() != "archived" {
		t.Fatalf("ожидали status=archived из payload_json, получили %q", event.GetStatus())
	}
}

func TestBuildConfigChangeEventRejectsSubscriberConsentWithoutStatus(t *testing.T) {
	// Старое поведение (тихий дефолт "active" для payload без status)
	// было ровно тем багом, который Фаза 6 закрывает — теперь payload без
	// status обязан провалиться громко, не молча опубликоваться.
	_, err := BuildConfigChangeEvent(outbox.Entry{
		EntityType: "subscriber_consent",
		EntityID:   "998901234567:CATEGORY:ADVERTISING:SMS",
		Payload:    []byte(`{"msisdn":"998901234567","scope_type":"CATEGORY","scope_value":"ADVERTISING","channel":"SMS"}`),
	})
	if err == nil {
		t.Fatalf("ожидали ошибку для subscriber_consent payload без status")
	}
}
