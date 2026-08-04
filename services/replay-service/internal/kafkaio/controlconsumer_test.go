package kafkaio

import (
	"testing"

	"google.golang.org/protobuf/proto"

	commonv1 "mpp/platformcontracts/common/v1"
	eventsv1 "mpp/platformcontracts/events/v1"
)

func TestDecodeExecutionControlRecordRoundTrip(t *testing.T) {
	rec := &eventsv1.ExecutionControlRecord{
		Scope: commonv1.ExecutionControlScope_EXECUTION_CONTROL_SCOPE_STAGE,
		State: commonv1.ExecutionControlState_EXECUTION_CONTROL_STATE_PAUSED,
	}
	payload, err := proto.Marshal(rec)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	decoded, err := DecodeExecutionControlRecord(payload)
	if err != nil {
		t.Fatalf("DecodeExecutionControlRecord failed: %v", err)
	}
	if decoded.GetState() != commonv1.ExecutionControlState_EXECUTION_CONTROL_STATE_PAUSED {
		t.Fatalf("неверный state: %v", decoded.GetState())
	}
}

func TestParseRecordKeyGlobalScope(t *testing.T) {
	scope, scopeID, err := ParseRecordKey([]byte("EXECUTION_CONTROL_SCOPE_GLOBAL:"))
	if err != nil {
		t.Fatalf("ParseRecordKey failed: %v", err)
	}
	if scope != commonv1.ExecutionControlScope_EXECUTION_CONTROL_SCOPE_GLOBAL || scopeID != "" {
		t.Fatalf("неверный разбор: scope=%v scopeID=%q", scope, scopeID)
	}
}

func TestParseRecordKeyStageScope(t *testing.T) {
	scope, scopeID, err := ParseRecordKey([]byte("EXECUTION_CONTROL_SCOPE_STAGE:BILLING"))
	if err != nil {
		t.Fatalf("ParseRecordKey failed: %v", err)
	}
	if scope != commonv1.ExecutionControlScope_EXECUTION_CONTROL_SCOPE_STAGE || scopeID != "BILLING" {
		t.Fatalf("неверный разбор: scope=%v scopeID=%q", scope, scopeID)
	}
}

func TestParseRecordKeyRejectsMalformed(t *testing.T) {
	if _, _, err := ParseRecordKey([]byte("no-colon-here")); err == nil {
		t.Fatalf("ожидали ошибку для ключа без разделителя")
	}
	if _, _, err := ParseRecordKey([]byte("NOT_A_REAL_SCOPE:x")); err == nil {
		t.Fatalf("ожидали ошибку для неизвестного scope")
	}
}
