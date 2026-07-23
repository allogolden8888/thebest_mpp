package kafkaio

import (
	"testing"
	"time"

	commonv1 "mpp/platformcontracts/common/v1"

	"mpp/scheduler-critical-sweep/internal/sweep"
)

func testState() sweep.ExecutionState {
	return sweep.ExecutionState{
		MessageID:        "msg-1",
		PipelineVersion:  "3",
		NodeID:           "node-billing",
		StageExecutionID: "exec-1",
		StageName:        "BILLING",
		Attempt:          1,
	}
}

func TestBuildRetryCommandIncrementsAttempt(t *testing.T) {
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	cmd := BuildRetryCommand(testState(), "evt-retry-1", now, "trace-1")

	if cmd.GetAttempt() != 2 {
		t.Fatalf("ожидали attempt=2 (1+1), получили %d", cmd.GetAttempt())
	}
	if cmd.GetStageExecutionId() != "exec-1" {
		t.Fatalf("stage_execution_id должен остаться прежним при retry, получили %s", cmd.GetStageExecutionId())
	}
	if cmd.GetStageName() != commonv1.StageName_STAGE_NAME_BILLING {
		t.Fatalf("неверный stage_name: %v", cmd.GetStageName())
	}
	if !cmd.GetDeadline().AsTime().Equal(now) {
		t.Fatalf("новый deadline не совпадает: %v", cmd.GetDeadline().AsTime())
	}
}

func TestBuildTimeoutEventSetsTimedOutNotRetryable(t *testing.T) {
	now := time.Now()
	ev := BuildTimeoutEvent(testState(), "evt-timeout-1", now)

	if ev.GetOutcome() != commonv1.Outcome_OUTCOME_TIMED_OUT {
		t.Fatalf("ожидали OUTCOME_TIMED_OUT, получили %v", ev.GetOutcome())
	}
	if ev.GetRetryable() {
		t.Fatalf("timeout без retry не должен быть помечен retryable=true")
	}
	if ev.GetAttempt() != 1 {
		t.Fatalf("attempt не должен меняться при timeout (не retry), получили %d", ev.GetAttempt())
	}
}

func TestBuildDlqRecordCarriesReasonAndOriginalCommand(t *testing.T) {
	now := time.Now()
	state := testState()
	original := BuildRetryCommand(state, "evt-x", now, "")

	rec := BuildDlqRecord(state, original, "retries exhausted after 3 attempts", now)

	if rec.GetReasonCode() != "RETRY_EXHAUSTED" {
		t.Fatalf("неверный reason_code: %s", rec.GetReasonCode())
	}
	if rec.GetOriginalCommand() != original {
		t.Fatalf("original_command должен быть тем же объектом, что передан")
	}
	if rec.GetStageExecutionId() != state.StageExecutionID {
		t.Fatalf("stage_execution_id не совпадает")
	}
}

func TestStageTopicMapsAllKnownStages(t *testing.T) {
	cases := map[commonv1.StageName]string{
		commonv1.StageName_STAGE_NAME_DESTINATION_RESOLUTION:  "stage.destination-resolution",
		commonv1.StageName_STAGE_NAME_POLICY:                  "stage.policy",
		commonv1.StageName_STAGE_NAME_BILLING:                 "stage.billing",
		commonv1.StageName_STAGE_NAME_ROUTING:                 "stage.routing",
		commonv1.StageName_STAGE_NAME_DELIVERY:                "stage.delivery",
		commonv1.StageName_STAGE_NAME_DELIVERY_RECONCILIATION: "stage.delivery-reconciliation",
	}
	for stage, want := range cases {
		got, err := StageTopic(stage)
		if err != nil {
			t.Fatalf("StageTopic(%v) failed: %v", stage, err)
		}
		if got != want {
			t.Fatalf("StageTopic(%v) = %s, want %s", stage, got, want)
		}
		dlq, err := DlqTopic(stage)
		if err != nil || dlq != want+".dlq" {
			t.Fatalf("DlqTopic(%v) = %s, %v — want %s.dlq", stage, dlq, err, want)
		}
	}
}

func TestStageTopicErrorsOnUnspecified(t *testing.T) {
	if _, err := StageTopic(commonv1.StageName_STAGE_NAME_UNSPECIFIED); err == nil {
		t.Fatalf("ожидали ошибку для STAGE_NAME_UNSPECIFIED")
	}
}

func TestStageNameFromStringRoundTrips(t *testing.T) {
	cases := map[string]commonv1.StageName{
		"DESTINATION_RESOLUTION":  commonv1.StageName_STAGE_NAME_DESTINATION_RESOLUTION,
		"POLICY":                  commonv1.StageName_STAGE_NAME_POLICY,
		"BILLING":                 commonv1.StageName_STAGE_NAME_BILLING,
		"ROUTING":                 commonv1.StageName_STAGE_NAME_ROUTING,
		"DELIVERY":                commonv1.StageName_STAGE_NAME_DELIVERY,
		"DELIVERY_RECONCILIATION": commonv1.StageName_STAGE_NAME_DELIVERY_RECONCILIATION,
	}
	for str, want := range cases {
		if got := StageNameFromString(str); got != want {
			t.Fatalf("StageNameFromString(%q) = %v, want %v", str, got, want)
		}
	}
	if got := StageNameFromString("garbage"); got != commonv1.StageName_STAGE_NAME_UNSPECIFIED {
		t.Fatalf("неизвестная строка должна давать UNSPECIFIED, получили %v", got)
	}
}
