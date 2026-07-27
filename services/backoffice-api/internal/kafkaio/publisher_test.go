package kafkaio

import (
	"testing"
	"time"

	commonv1 "mpp/platformcontracts/common/v1"
)

func TestBuildCriticalCommandMapsFieldsCorrectly(t *testing.T) {
	now := time.Date(2026, 7, 27, 10, 0, 0, 0, time.UTC)

	cmd := BuildCriticalCommand("stage-exec-1", commonv1.CriticalCommandType_CRITICAL_COMMAND_TYPE_FORCE_RETRY, "ops@mpp", "stuck operator route", now)

	if cmd.GetStageExecutionId() != "stage-exec-1" {
		t.Fatalf("неверный stage_execution_id: %v", cmd.GetStageExecutionId())
	}
	if cmd.GetTaskType() != commonv1.CriticalCommandType_CRITICAL_COMMAND_TYPE_FORCE_RETRY {
		t.Fatalf("неверный task_type: %v", cmd.GetTaskType())
	}
	if cmd.GetRequestedBy() != "ops@mpp" {
		t.Fatalf("неверный requested_by: %v", cmd.GetRequestedBy())
	}
	if cmd.GetReason() != "stuck operator route" {
		t.Fatalf("неверный reason: %v", cmd.GetReason())
	}
	if !cmd.GetRequestedAt().AsTime().Equal(now) {
		t.Fatalf("неверный requested_at: %v", cmd.GetRequestedAt().AsTime())
	}
}

func TestBuildCriticalCommandForceTimeout(t *testing.T) {
	cmd := BuildCriticalCommand("stage-exec-2", commonv1.CriticalCommandType_CRITICAL_COMMAND_TYPE_FORCE_TIMEOUT, "ops@mpp", "", time.Now())
	if cmd.GetTaskType() != commonv1.CriticalCommandType_CRITICAL_COMMAND_TYPE_FORCE_TIMEOUT {
		t.Fatalf("неверный task_type: %v", cmd.GetTaskType())
	}
}
