package validate

import "testing"

// Примеры ниже — те же данные, что config_schemas/examples/{number_range,
// partner}.valid.json (реальные значения из миграций/чата, не выдуманные) —
// сверены на равенство схеме, не переизобретены.

const validNumberRange = `{
	"range_start": 998900000000,
	"range_end": 998909999999,
	"operator_id": "beeline_uz",
	"version": 1,
	"status": "active"
}`

const invalidNumberRangeEndBeforeStart = `{
	"range_start": 998909999999,
	"range_end": 998900000000,
	"operator_id": "beeline_uz",
	"version": 1,
	"status": "active"
}`

const validPartner = `{
	"partner_id": "click_uz",
	"version": 4,
	"status": "active",
	"applications": [
		{
			"application_id": "click_uz_main",
			"display_name": "Click main billing notifications",
			"auth": {"type": "API_KEY", "credential_ref": "vault://partners/click_uz/main/api_key"},
			"ip_allowlist": ["185.65.212.0/24"],
			"rate_limit_tps": 300,
			"allowed_channels": ["SMS"]
		}
	]
}`

func TestValidateRejectsNumberRangeWithEndBeforeStart(t *testing.T) {
	v, err := NewValidator()
	if err != nil {
		t.Fatalf("NewValidator failed: %v", err)
	}
	// JSON Schema в одиночку это бы пропустило (не выражает арифметику
	// между полями) — должна сработать семантическая проверка.
	err = v.Validate(EntityNumberRange, []byte(invalidNumberRangeEndBeforeStart))
	if err == nil {
		t.Fatalf("ожидали ошибку семантической проверки range_end < range_start")
	}
}

func TestValidateAcceptsValidNumberRange(t *testing.T) {
	v, err := NewValidator()
	if err != nil {
		t.Fatalf("NewValidator failed: %v", err)
	}
	if err := v.Validate(EntityNumberRange, []byte(validNumberRange)); err != nil {
		t.Fatalf("ожидали успешную валидацию, получили: %v", err)
	}
}

func TestValidateRejectsValidPartnerAgainstWrongSchema(t *testing.T) {
	v, err := NewValidator()
	if err != nil {
		t.Fatalf("NewValidator failed: %v", err)
	}
	// partner-payload не должен пройти как number_range — разные обязательные поля.
	err = v.Validate(EntityNumberRange, []byte(validPartner))
	if err == nil {
		t.Fatalf("ожидали ошибку валидации при несовпадении entity_type и формы payload")
	}
}

func TestValidateAcceptsValidPartner(t *testing.T) {
	v, err := NewValidator()
	if err != nil {
		t.Fatalf("NewValidator failed: %v", err)
	}
	if err := v.Validate(EntityPartner, []byte(validPartner)); err != nil {
		t.Fatalf("ожидали успешную валидацию, получили: %v", err)
	}
}

func TestValidateRejectsUnknownEntityType(t *testing.T) {
	v, err := NewValidator()
	if err != nil {
		t.Fatalf("NewValidator failed: %v", err)
	}
	err = v.Validate(EntityType("not_a_real_entity"), []byte(`{}`))
	if err == nil {
		t.Fatalf("ожидали ошибку для неизвестного entity_type")
	}
}

func TestValidateRejectsMalformedJSON(t *testing.T) {
	v, err := NewValidator()
	if err != nil {
		t.Fatalf("NewValidator failed: %v", err)
	}
	err = v.Validate(EntityNumberRange, []byte(`{not valid json`))
	if err == nil {
		t.Fatalf("ожидали ошибку для невалидного JSON")
	}
}

func TestIsValidEntityTypeMatchesDbCheckConstraint(t *testing.T) {
	// migrations/V002__config_versions.sql config_versions_entity_type_check
	dbAllowed := []EntityType{
		"pipeline", "policy_ruleset", "policy_template", "billing_tariff",
		"routing_table", "number_range", "partner", "operator", "subscriber_consent",
	}
	if len(dbAllowed) != len(ValidEntityTypes) {
		t.Fatalf("список entity_type в коде (%d) разошёлся с DB CHECK (%d)", len(ValidEntityTypes), len(dbAllowed))
	}
	for _, e := range dbAllowed {
		if !IsValidEntityType(e) {
			t.Fatalf("entity_type %q разрешён в DB CHECK, но не в IsValidEntityType", e)
		}
	}
}