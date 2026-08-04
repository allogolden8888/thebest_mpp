package kafkaio

import (
	"testing"

	"google.golang.org/protobuf/proto"

	commonv1 "mpp/platformcontracts/common/v1"
	eventsv1 "mpp/platformcontracts/events/v1"
)

func TestDecodeManualCommandRoundTrips(t *testing.T) {
	original := &eventsv1.SchedulerCriticalCommand{
		StageExecutionId: "exec-1",
		TaskType:         commonv1.CriticalCommandType_CRITICAL_COMMAND_TYPE_FORCE_RETRY,
		RequestedBy:      "ops@mpp",
		Reason:           "operator incident cleared early",
	}
	payload, err := proto.Marshal(original)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	got, err := DecodeManualCommand(payload)
	if err != nil {
		t.Fatalf("DecodeManualCommand failed: %v", err)
	}
	if got.GetStageExecutionId() != "exec-1" || got.GetTaskType() != commonv1.CriticalCommandType_CRITICAL_COMMAND_TYPE_FORCE_RETRY {
		t.Fatalf("round-trip mismatch: %+v", got)
	}
}

func TestDecodeManualCommandRejectsMissingStageExecutionId(t *testing.T) {
	payload, _ := proto.Marshal(&eventsv1.SchedulerCriticalCommand{RequestedBy: "ops"})
	_, err := DecodeManualCommand(payload)
	if err == nil {
		t.Fatalf("ожидали ошибку для команды без stage_execution_id")
	}
}

func TestDecodeManualCommandRejectsGarbage(t *testing.T) {
	_, err := DecodeManualCommand([]byte{0xff, 0x00, 0xff, 0x00, 0x01, 0x02})
	if err == nil {
		t.Fatalf("ожидали ошибку разбора на мусорных байтах")
	}
}

// TestParseRecordKeyRoundTripsAllScopes — CODE_REVIEW.md finding #4:
// ParseRecordKey должен разбирать формат ключа, реально производимый
// execution-control-service (services/execution-control-service/internal/
// kafkaio/publisher.go::RecordKey) — единственным producer'ом этого топика.
func TestParseRecordKeyRoundTripsAllScopes(t *testing.T) {
	cases := []struct {
		scope   commonv1.ExecutionControlScope
		scopeID string
	}{
		{commonv1.ExecutionControlScope_EXECUTION_CONTROL_SCOPE_GLOBAL, ""},
		{commonv1.ExecutionControlScope_EXECUTION_CONTROL_SCOPE_STAGE, "BILLING"},
		{commonv1.ExecutionControlScope_EXECUTION_CONTROL_SCOPE_PARTNER, "partner-1"},
		{commonv1.ExecutionControlScope_EXECUTION_CONTROL_SCOPE_PARTNER_STAGE, "partner-1:BILLING"},
		{commonv1.ExecutionControlScope_EXECUTION_CONTROL_SCOPE_OPERATOR_ROUTE, "route-7"},
	}
	for _, tc := range cases {
		key := []byte(tc.scope.String() + ":" + tc.scopeID)
		gotScope, gotScopeID, err := ParseRecordKey(key)
		if err != nil {
			t.Fatalf("ParseRecordKey(%q) failed: %v", key, err)
		}
		if gotScope != tc.scope || gotScopeID != tc.scopeID {
			t.Fatalf("ParseRecordKey(%q) = (%v, %q), want (%v, %q)", key, gotScope, gotScopeID, tc.scope, tc.scopeID)
		}
	}
}

func TestParseRecordKeyRejectsMissingSeparator(t *testing.T) {
	if _, _, err := ParseRecordKey([]byte("no-separator-here")); err == nil {
		t.Fatalf("ожидали ошибку для ключа без ':'")
	}
}

func TestParseRecordKeyRejectsUnknownScope(t *testing.T) {
	if _, _, err := ParseRecordKey([]byte("NOT_A_REAL_SCOPE:foo")); err == nil {
		t.Fatalf("ожидали ошибку для неизвестного scope")
	}
}

func TestDecodeControlRecordRoundTrips(t *testing.T) {
	original := &eventsv1.ExecutionControlRecord{
		Scope:   commonv1.ExecutionControlScope_EXECUTION_CONTROL_SCOPE_STAGE,
		ScopeId: "BILLING",
		State:   commonv1.ExecutionControlState_EXECUTION_CONTROL_STATE_PAUSED,
		Version: 5,
	}
	payload, err := proto.Marshal(original)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	got, err := DecodeControlRecord(payload)
	if err != nil {
		t.Fatalf("DecodeControlRecord failed: %v", err)
	}
	if got.GetScopeId() != "BILLING" || got.GetState() != commonv1.ExecutionControlState_EXECUTION_CONTROL_STATE_PAUSED {
		t.Fatalf("round-trip mismatch: %+v", got)
	}
}
