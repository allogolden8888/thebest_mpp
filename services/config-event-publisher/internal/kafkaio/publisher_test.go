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
		if et == "policy_template" {
			// policy_template без config_versions строки резолвит status
			// из payload_json (см. TestResolveStatus*) — для этого теста
			// достаточно валидного payload, не сам факт резолвинга.
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

// TestResolveStatusSubscriberConsentDefaultsButFlagsUnresolved — CODE_REVIEW.md
// Critical, compliance-sensitive: subscriber_consent payload_json (по схеме
// config_schemas/subscriber_consent.schema.json) не содержит поля status —
// revocation структурно невозможно закодировать через текущий контракт,
// и ничто в репозитории сегодня не пишет outbox-строку, сигнализирующую
// archived для этого entity_type (configuration-service.ArchiveVersion не
// трогает config_outbox вообще, см. README). "active" — единственное
// безопасное предположение без изобретения нового контракта — но
// unresolved=true теперь делает это явным вызывающей стороне (main.go
// логирует WARNING), а не тихим, как раньше.
func TestResolveStatusSubscriberConsentDefaultsButFlagsUnresolved(t *testing.T) {
	status, unresolved, err := ResolveStatus(outbox.Entry{
		EntityType: "subscriber_consent",
		Status:     "",
		Payload:    []byte(`{"msisdn":"998901234567","scope_type":"CATEGORY","scope_value":"ADVERTISING","channel":"SMS"}`),
	})
	if err != nil {
		t.Fatalf("ResolveStatus failed: %v", err)
	}
	if !unresolved {
		t.Fatalf("subscriber_consent status структурно неизвестен — ожидали unresolved=true, чтобы вызывающая сторона громко залогировала предположение")
	}
	if status != "active" {
		t.Fatalf("ожидали дефолт status=active, получили %q", status)
	}
}

func TestBuildConfigChangeEventPublishesSubscriberConsentAsActiveDespiteUnresolvedStatus(t *testing.T) {
	// Документирует текущее (ограниченное scope этого сервиса) поведение:
	// событие всё равно публикуется (не блокируется), потому что
	// permanently отклонять subscriber_consent записи было бы хуже, чем
	// публиковать их с известным предположением — но main.go обязан
	// залогировать WARNING в этом случае (см. ResolveStatus doc-comment).
	event, err := BuildConfigChangeEvent(outbox.Entry{
		EntityType: "subscriber_consent",
		EntityID:   "998901234567:CATEGORY:ADVERTISING:SMS",
		Payload:    []byte(`{"msisdn":"998901234567","scope_type":"CATEGORY","scope_value":"ADVERTISING","channel":"SMS"}`),
	})
	if err != nil {
		t.Fatalf("BuildConfigChangeEvent failed: %v", err)
	}
	if event.GetStatus() != "active" {
		t.Fatalf("ожидали status=active (дефолт), получили %q", event.GetStatus())
	}
}