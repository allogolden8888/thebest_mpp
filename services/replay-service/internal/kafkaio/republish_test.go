package kafkaio

import (
	"testing"

	"google.golang.org/protobuf/proto"

	commonv1 "mpp/platformcontracts/common/v1"
)

func TestDecodeOriginalCommandRoundTrips(t *testing.T) {
	original := &commonv1.StageExecuteCommand{MessageId: "msg-1", StageName: commonv1.StageName_STAGE_NAME_BILLING}
	payload, _ := proto.Marshal(original)

	cmd, err := DecodeOriginalCommand(payload)
	if err != nil {
		t.Fatalf("DecodeOriginalCommand failed: %v", err)
	}
	if cmd.GetMessageId() != "msg-1" || cmd.GetStageName() != commonv1.StageName_STAGE_NAME_BILLING {
		t.Fatalf("round-trip mismatch: %+v", cmd)
	}
}

func TestDecodeOriginalCommandRejectsGarbage(t *testing.T) {
	if _, err := DecodeOriginalCommand([]byte{0xff, 0x00, 0xff}); err == nil {
		t.Fatalf("ожидали ошибку разбора мусорных байтов")
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
		if err != nil || got != want {
			t.Fatalf("StageTopic(%v) = %s, %v — want %s", stage, got, err, want)
		}
	}
}

func TestStageTopicErrorsOnUnspecified(t *testing.T) {
	if _, err := StageTopic(commonv1.StageName_STAGE_NAME_UNSPECIFIED); err == nil {
		t.Fatalf("ожидали ошибку для STAGE_NAME_UNSPECIFIED")
	}
}