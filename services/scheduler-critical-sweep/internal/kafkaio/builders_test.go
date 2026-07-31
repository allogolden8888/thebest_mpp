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

// testStateWithBillingResult — то же состояние, но с накопленными
// результатами предыдущих стадий (resolved_operator_id/category), которых
// достаточно, чтобы buildStageExtension собрал BillingExtension — см.
// CODE_REVIEW.md finding #1.
func testStateWithBillingResult() sweep.ExecutionState {
	s := testState()
	s.ResolvedOperatorID = "beeline"
	s.Category = "TRANSACTION"
	s.SegmentCount = 2
	return s
}

func TestBuildRetryCommandIncrementsAttempt(t *testing.T) {
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	cmd, _ := BuildRetryCommand(testState(), "evt-retry-1", 2, now, "trace-1")

	if cmd.GetAttempt() != 2 {
		t.Fatalf("ожидали attempt=2, получили %d", cmd.GetAttempt())
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

// TestBuildRetryCommandAlwaysSetsPayloadRef — CODE_REVIEW.md finding #1:
// payload_ref детерминированно строится из message_id, ничего не зависит от
// накопленного состояния — должен быть заполнен всегда, включая случай без
// stage_extension.
func TestBuildRetryCommandAlwaysSetsPayloadRef(t *testing.T) {
	cmd, _ := BuildRetryCommand(testState(), "evt-retry-1", 2, time.Now(), "")
	ref := cmd.GetPayloadRef()
	if ref == nil {
		t.Fatalf("payload_ref не должен быть nil")
	}
	if ref.GetMessageId() != "msg-1" {
		t.Fatalf("payload_ref.message_id = %q, want msg-1", ref.GetMessageId())
	}
	if ref.GetRuntimeRedisKey() != "msgctx:msg-1" {
		t.Fatalf("payload_ref.runtime_redis_key = %q, want msgctx:msg-1", ref.GetRuntimeRedisKey())
	}
}

// TestBuildRetryCommandReportsMissingStageExtension — без накопленных
// результатов предыдущей стадии (сегодняшнее состояние Runtime Redis,
// см. README) republish не может собрать stage_extension: BuildRetryCommand
// обязан сигнализировать об этом через ok=false, а не молча публиковать
// частичную команду (CODE_REVIEW.md finding #1 — именно это раньше
// приводило к гарантированному REJECTED на destination-resolution-service).
func TestBuildRetryCommandReportsMissingStageExtension(t *testing.T) {
	cmd, ok := BuildRetryCommand(testState(), "evt-retry-1", 2, time.Now(), "")
	if ok {
		t.Fatalf("ожидали ok=false — состояние без накопленных результатов не может собрать stage_extension")
	}
	if cmd.GetStageExtension() != nil {
		t.Fatalf("stage_extension должен остаться nil, когда ok=false")
	}
}

// TestBuildRetryCommandSetsStageExtensionWhenDataAvailable — как только
// ExecutionState несёт накопленные результаты предыдущих стадий (Pipeline
// Engine начинает писать их в exec:{message_id} — см. README "Открытый
// вопрос"), BuildRetryCommand обязан собрать корректный stage_extension и
// вернуть ok=true.
func TestBuildRetryCommandSetsStageExtensionWhenDataAvailable(t *testing.T) {
	cmd, ok := BuildRetryCommand(testStateWithBillingResult(), "evt-retry-1", 2, time.Now(), "")
	if !ok {
		t.Fatalf("ожидали ok=true — достаточно данных для BillingExtension")
	}
	billing := cmd.GetBilling()
	if billing == nil {
		t.Fatalf("stage_extension должен быть BillingExtension")
	}
	if billing.GetResolvedOperatorId() != "beeline" || billing.GetCategory() != "TRANSACTION" || billing.GetSegmentCount() != 2 {
		t.Fatalf("неверный BillingExtension: %+v", billing)
	}
}

func TestBuildStageExtensionPerStageName(t *testing.T) {
	cases := []struct {
		name  string
		state sweep.ExecutionState
		check func(t *testing.T, cmd *commonv1.StageExecuteCommand)
	}{
		{
			name:  "destination_resolution",
			state: sweep.ExecutionState{StageName: "DESTINATION_RESOLUTION", DestinationAddress: "998901234567"},
			check: func(t *testing.T, cmd *commonv1.StageExecuteCommand) {
				if got := cmd.GetDestinationResolution().GetDestinationAddress(); got != "998901234567" {
					t.Fatalf("destination_address = %q", got)
				}
			},
		},
		{
			name:  "policy",
			state: sweep.ExecutionState{StageName: "POLICY", ResolvedOperatorID: "ucell"},
			check: func(t *testing.T, cmd *commonv1.StageExecuteCommand) {
				if got := cmd.GetPolicy().GetResolvedOperatorId(); got != "ucell" {
					t.Fatalf("resolved_operator_id = %q", got)
				}
			},
		},
		{
			name:  "routing",
			state: sweep.ExecutionState{StageName: "ROUTING", ResolvedOperatorID: "ucell"},
			check: func(t *testing.T, cmd *commonv1.StageExecuteCommand) {
				if got := cmd.GetRouting().GetResolvedOperatorId(); got != "ucell" {
					t.Fatalf("resolved_operator_id = %q", got)
				}
			},
		},
		{
			name:  "delivery",
			state: sweep.ExecutionState{StageName: "DELIVERY", RouteID: "route-7", Protocol: int32(commonv1.Protocol_PROTOCOL_SMPP), RouteVersion: "v2"},
			check: func(t *testing.T, cmd *commonv1.StageExecuteCommand) {
				d := cmd.GetDelivery()
				if d.GetRouteId() != "route-7" || d.GetProtocol() != commonv1.Protocol_PROTOCOL_SMPP || d.GetRouteVersion() != "v2" {
					t.Fatalf("неверный DeliveryExtension: %+v", d)
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cmd, ok := BuildRetryCommand(tc.state, "evt-1", 1, time.Now(), "")
			if !ok {
				t.Fatalf("%s: ожидали ok=true", tc.name)
			}
			tc.check(t, cmd)
		})
	}
}

func TestBuildRetryCommandDeliveryReconciliationNeverReconstructable(t *testing.T) {
	state := sweep.ExecutionState{StageName: "DELIVERY_RECONCILIATION"}
	_, ok := BuildRetryCommand(state, "evt-1", 1, time.Now(), "")
	if ok {
		t.Fatalf("DELIVERY_RECONCILIATION extension никогда не восстанавливается из накопленного состояния — ожидали ok=false")
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
	original, _ := BuildRetryCommand(state, "evt-x", state.Attempt, now, "")

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

// TestBuildDlqRecordOriginalCommandUsesExhaustedAttemptNotAttemptPlusOne —
// CODE_REVIEW.md finding #2: original_command раньше всегда строился с
// state.Attempt+1 (BuildRetryCommand считала +1 сама), даже когда речь шла о
// DLQ-снимке уже исчерпанной попытки — на единицу больше, чем реально
// исчерпано. Теперь attempt передаётся явно вызывающей стороной.
func TestBuildDlqRecordOriginalCommandUsesExhaustedAttemptNotAttemptPlusOne(t *testing.T) {
	state := testState()
	state.Attempt = 3 // попытки исчерпаны на attempt=3

	original, _ := BuildRetryCommand(state, "evt-dlq-original", state.Attempt, state.Deadline, "")
	if original.GetAttempt() != 3 {
		t.Fatalf("original_command.attempt = %d, want 3 (реально исчерпанная попытка, не +1)", original.GetAttempt())
	}

	rec := BuildDlqRecord(state, original, "retry attempts exhausted", time.Now())
	if rec.GetAttempt() != 3 {
		t.Fatalf("DlqRecord.attempt = %d, want 3", rec.GetAttempt())
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
