package validate

import "testing"

// Payloads below are the same real examples as config_schemas/examples/
// pipeline.*.json and routing_table.*.json — ported verbatim, not
// reinvented, so the semantic checks stay traceable to the Python originals
// in config_schemas/validate_all.py.

const validPipeline = `{
	"pipeline_id": "sms-default-v1",
	"version": 1,
	"status": "active",
	"partner_id": null,
	"application_id": null,
	"entry_node_id": "n1_destination_resolution",
	"nodes": [
		{"node_id": "n1_destination_resolution", "stage_name": "DESTINATION_RESOLUTION", "next": {"SUCCEEDED": "n2_policy", "FAILED": null}},
		{"node_id": "n2_policy", "stage_name": "POLICY", "next": {"SUCCEEDED": "n3_billing", "REJECTED": "n_billing_blocked", "FAILED": null}},
		{"node_id": "n3_billing", "stage_name": "BILLING", "next": {"SUCCEEDED": "n4_routing", "FAILED": null}},
		{"node_id": "n_billing_blocked", "stage_name": "BILLING", "implicit_category": "BLOCKED", "next": {"SUCCEEDED": null, "FAILED": null}},
		{"node_id": "n4_routing", "stage_name": "ROUTING", "next": {"SUCCEEDED": "n5_delivery", "FAILED": null, "RETRY_EXHAUSTED": null}},
		{"node_id": "n5_delivery", "stage_name": "DELIVERY", "next": {"SUCCEEDED": "n6_reconciliation", "FAILED": null, "TIMED_OUT": "n6_reconciliation", "RETRY_EXHAUSTED": "n6_reconciliation"}},
		{"node_id": "n6_reconciliation", "stage_name": "DELIVERY_RECONCILIATION", "next": {"SUCCEEDED": null, "SUBMISSION_OUTCOME_UNKNOWN": null, "DELIVERY_UNRESOLVED": null}}
	]
}`

const invalidPipelineRejectedNotToBilling = `{
	"pipeline_id": "sms-broken-rejected-route-v1",
	"version": 1,
	"status": "active",
	"partner_id": null,
	"application_id": null,
	"entry_node_id": "n1_destination_resolution",
	"nodes": [
		{"node_id": "n1_destination_resolution", "stage_name": "DESTINATION_RESOLUTION", "next": {"SUCCEEDED": "n2_policy", "FAILED": null}},
		{"node_id": "n2_policy", "stage_name": "POLICY", "next": {"SUCCEEDED": "n3_billing", "REJECTED": null, "FAILED": null}},
		{"node_id": "n3_billing", "stage_name": "BILLING", "next": {"SUCCEEDED": null, "FAILED": null}}
	]
}`

const invalidPipelineEntryNotDestinationResolution = `{
	"pipeline_id": "sms-broken-entry-v1",
	"version": 1,
	"status": "active",
	"partner_id": null,
	"application_id": null,
	"entry_node_id": "n1_policy",
	"nodes": [
		{"node_id": "n1_policy", "stage_name": "POLICY", "next": {"SUCCEEDED": null, "REJECTED": null, "FAILED": null}}
	]
}`

const validRoutingTable = `{
	"operator_id": "beeline_uz",
	"version": 3,
	"status": "active",
	"active_route_id": "beeline_smpp_primary",
	"routes": [
		{"route_id": "beeline_smpp_primary", "protocol": "SMPP", "failover_priority": 1, "tps_limit": 500},
		{"route_id": "beeline_http_reserve", "protocol": "HTTP", "failover_priority": 2, "tps_limit": 200}
	]
}`

const invalidRoutingTableActiveNotInList = `{
	"operator_id": "ucell_uz",
	"version": 1,
	"status": "active",
	"active_route_id": "ucell_smpp_primary_typo",
	"routes": [
		{"route_id": "ucell_smpp_primary", "protocol": "SMPP", "failover_priority": 1, "tps_limit": 400}
	]
}`

func TestValidateAcceptsValidPipelineGraph(t *testing.T) {
	v, err := NewValidator()
	if err != nil {
		t.Fatalf("NewValidator failed: %v", err)
	}
	if err := v.Validate(EntityPipeline, []byte(validPipeline)); err != nil {
		t.Fatalf("ожидали успешную валидацию, получили: %v", err)
	}
}

func TestValidateRejectsPipelineWherePolicyRejectedDoesNotLeadToBilling(t *testing.T) {
	v, err := NewValidator()
	if err != nil {
		t.Fatalf("NewValidator failed: %v", err)
	}
	if err := v.Validate(EntityPipeline, []byte(invalidPipelineRejectedNotToBilling)); err == nil {
		t.Fatalf("ожидали ошибку: hld.md §5.3.1 требует POLICY.REJECTED -> BILLING")
	}
}

func TestValidateRejectsPipelineWithWrongEntryStage(t *testing.T) {
	v, err := NewValidator()
	if err != nil {
		t.Fatalf("NewValidator failed: %v", err)
	}
	if err := v.Validate(EntityPipeline, []byte(invalidPipelineEntryNotDestinationResolution)); err == nil {
		t.Fatalf("ожидали ошибку: hld.md §6 требует entry_node_id -> DESTINATION_RESOLUTION")
	}
}

func TestValidateAcceptsValidRoutingTable(t *testing.T) {
	v, err := NewValidator()
	if err != nil {
		t.Fatalf("NewValidator failed: %v", err)
	}
	if err := v.Validate(EntityRoutingTable, []byte(validRoutingTable)); err != nil {
		t.Fatalf("ожидали успешную валидацию, получили: %v", err)
	}
}

func TestValidateRejectsRoutingTableWithActiveRouteNotInList(t *testing.T) {
	v, err := NewValidator()
	if err != nil {
		t.Fatalf("NewValidator failed: %v", err)
	}
	if err := v.Validate(EntityRoutingTable, []byte(invalidRoutingTableActiveNotInList)); err == nil {
		t.Fatalf("ожидали ошибку: active_route_id должен входить в routes[]")
	}
}
