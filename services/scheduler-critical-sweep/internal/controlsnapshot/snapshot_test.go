package controlsnapshot

import (
	"testing"

	commonv1 "mpp/platformcontracts/common/v1"
)

func TestIsPausedFalseByDefault(t *testing.T) {
	s := New()
	if s.IsPaused("BILLING") {
		t.Fatalf("пустой снапшот не должен блокировать sweep")
	}
}

func TestIsPausedTrueOnGlobalPause(t *testing.T) {
	s := New()
	s.Apply(commonv1.ExecutionControlScope_EXECUTION_CONTROL_SCOPE_GLOBAL, "", commonv1.ExecutionControlState_EXECUTION_CONTROL_STATE_PAUSED)
	if !s.IsPaused("BILLING") {
		t.Fatalf("GLOBAL PAUSED должен блокировать любую стадию")
	}
	if !s.IsPaused("ROUTING") {
		t.Fatalf("GLOBAL PAUSED должен блокировать любую стадию")
	}
}

func TestIsPausedTrueOnlyForMatchingStage(t *testing.T) {
	s := New()
	s.Apply(commonv1.ExecutionControlScope_EXECUTION_CONTROL_SCOPE_STAGE, "BILLING", commonv1.ExecutionControlState_EXECUTION_CONTROL_STATE_PAUSED)

	if !s.IsPaused("BILLING") {
		t.Fatalf("STAGE=BILLING PAUSED должен блокировать BILLING")
	}
	if s.IsPaused("ROUTING") {
		t.Fatalf("STAGE=BILLING PAUSED не должен блокировать ROUTING")
	}
}

func TestApplyOverwritesPreviousStateForSameKey(t *testing.T) {
	s := New()
	s.Apply(commonv1.ExecutionControlScope_EXECUTION_CONTROL_SCOPE_STAGE, "BILLING", commonv1.ExecutionControlState_EXECUTION_CONTROL_STATE_PAUSED)
	s.Apply(commonv1.ExecutionControlScope_EXECUTION_CONTROL_SCOPE_STAGE, "BILLING", commonv1.ExecutionControlState_EXECUTION_CONTROL_STATE_ACTIVE)

	if s.IsPaused("BILLING") {
		t.Fatalf("более поздняя запись (compaction) должна перезаписывать предыдущую")
	}
}
