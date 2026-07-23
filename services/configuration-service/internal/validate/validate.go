// Package validate — validate_config_change (service_internal_methods.md
// §3.2): структурная валидация payload_json против JSON Schema, привязанной
// к entity_type. Схемы — копия config_schemas/*.schema.json (источник
// истины остаётся там; drift risk задокументирован в README — регенерация
// вручную, единого механизма синхронизации в этой сессии не заведено).
package validate

import (
	"embed"
	"encoding/json"
	"fmt"

	"github.com/santhosh-tekuri/jsonschema/v5"
)

//go:embed schemas/*.json
var schemaFS embed.FS

// EntityType — те же значения, что config.config_versions.entity_type
// (migrations/V002__config_versions.sql CHECK) и ConfigEntityType (proto).
type EntityType string

const (
	EntityPipeline           EntityType = "pipeline"
	EntityPolicyRuleset      EntityType = "policy_ruleset"
	EntityPolicyTemplate     EntityType = "policy_template"
	EntityBillingTariff      EntityType = "billing_tariff"
	EntityRoutingTable       EntityType = "routing_table"
	EntityNumberRange        EntityType = "number_range"
	EntityPartner            EntityType = "partner"
	EntityOperator           EntityType = "operator"
	EntitySubscriberConsent  EntityType = "subscriber_consent"
)

var schemaFile = map[EntityType]string{
	EntityPipeline:          "pipeline.schema.json",
	EntityPolicyRuleset:     "policy_ruleset.schema.json",
	EntityPolicyTemplate:    "policy_template.schema.json",
	EntityBillingTariff:     "billing_tariff.schema.json",
	EntityRoutingTable:      "routing_table.schema.json",
	EntityNumberRange:       "number_range.schema.json",
	EntityPartner:           "partner.schema.json",
	EntityOperator:          "operator.schema.json",
	EntitySubscriberConsent: "subscriber_consent.schema.json",
}

// ValidEntityTypes — тот же список, что CHECK-ограничение
// config_versions_entity_type_check.
var ValidEntityTypes = []EntityType{
	EntityPipeline, EntityPolicyRuleset, EntityPolicyTemplate, EntityBillingTariff,
	EntityRoutingTable, EntityNumberRange, EntityPartner, EntityOperator, EntitySubscriberConsent,
}

func IsValidEntityType(e EntityType) bool {
	for _, v := range ValidEntityTypes {
		if v == e {
			return true
		}
	}
	return false
}

// Validator — компилирует и кеширует JSON Schema по entity_type.
type Validator struct {
	compiled map[EntityType]*jsonschema.Schema
}

func NewValidator() (*Validator, error) {
	compiler := jsonschema.NewCompiler()
	compiler.Draft = jsonschema.Draft2020

	compiled := make(map[EntityType]*jsonschema.Schema, len(schemaFile))
	for entity, filename := range schemaFile {
		raw, err := schemaFS.ReadFile("schemas/" + filename)
		if err != nil {
			return nil, fmt.Errorf("read embedded schema %s: %w", filename, err)
		}
		var doc any
		if err := json.Unmarshal(raw, &doc); err != nil {
			return nil, fmt.Errorf("unmarshal schema %s: %w", filename, err)
		}
		url := "mem://" + filename
		if err := compiler.AddResource(url, doc); err != nil {
			return nil, fmt.Errorf("add schema resource %s: %w", filename, err)
		}
		schema, err := compiler.Compile(url)
		if err != nil {
			return nil, fmt.Errorf("compile schema %s: %w", filename, err)
		}
		compiled[entity] = schema
	}

	return &Validator{compiled: compiled}, nil
}

// ValidationError — validate_config_change | ValidationError.
type ValidationError struct {
	EntityType EntityType
	Errors     []string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("validation failed for entity_type=%s: %v", e.EntityType, e.Errors)
}

// Validate — validate_config_change: CRUD-запрос (entityType, payloadJSON)
// -> ValidatedChange (nil error) | ValidationError.
func (v *Validator) Validate(entityType EntityType, payloadJSON []byte) error {
	if !IsValidEntityType(entityType) {
		return &ValidationError{EntityType: entityType, Errors: []string{"unknown entity_type"}}
	}

	schema, ok := v.compiled[entityType]
	if !ok {
		return &ValidationError{EntityType: entityType, Errors: []string{"no schema registered for entity_type"}}
	}

	var instance any
	if err := json.Unmarshal(payloadJSON, &instance); err != nil {
		return &ValidationError{EntityType: entityType, Errors: []string{"payload is not valid JSON: " + err.Error()}}
	}

	if err := schema.Validate(instance); err != nil {
		if valErr, ok := err.(*jsonschema.ValidationError); ok {
			var messages []string
			for _, cause := range valErr.BasicOutput().Errors {
				if cause.Error != "" {
					messages = append(messages, fmt.Sprintf("%s: %s", cause.KeywordLocation, cause.Error))
				}
			}
			if len(messages) == 0 {
				messages = []string{err.Error()}
			}
			return &ValidationError{EntityType: entityType, Errors: messages}
		}
		return &ValidationError{EntityType: entityType, Errors: []string{err.Error()}}
	}

	if semanticErrors := runSemanticChecks(entityType, payloadJSON); len(semanticErrors) > 0 {
		return &ValidationError{EntityType: entityType, Errors: semanticErrors}
	}

	return nil
}
