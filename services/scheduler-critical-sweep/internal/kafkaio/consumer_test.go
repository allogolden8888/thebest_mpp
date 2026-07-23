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
