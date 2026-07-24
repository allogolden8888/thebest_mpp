package core

import (
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "mpp/platformcontracts/common/v1"
	eventsv1 "mpp/platformcontracts/events/v1"
)

func TestFromIncomingMessage(t *testing.T) {
	now := time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC)
	rec := FromIncomingMessage(&eventsv1.IncomingMessage{MessageId: "msg-1", PartnerId: "acme", ReceivedAt: timestamppb.New(now)})
	if rec.EventType != "incoming" || rec.MessageID != "msg-1" || rec.PartnerID != "acme" {
		t.Fatalf("неверные поля: %+v", rec)
	}
	if !rec.OccurredAt.Equal(now) {
		t.Fatalf("occurred_at не совпадает")
	}
}

func TestFromStageCompleted(t *testing.T) {
	now := time.Now()
	event := &commonv1.StageCompletedEvent{
		MessageId: "msg-1", StageName: commonv1.StageName_STAGE_NAME_BILLING,
		Outcome: commonv1.Outcome_OUTCOME_SUCCEEDED, ReasonCode: "", CompletedAt: timestamppb.New(now),
	}
	rec := FromStageCompleted(event)
	if rec.EventType != "stage_completed" || rec.StageName != "BILLING" || rec.Outcome != "SUCCEEDED" {
		t.Fatalf("неверные поля: %+v", rec)
	}
}

func TestFromLifecycleEvent(t *testing.T) {
	now := time.Now()
	event := &eventsv1.MessageLifecycleEvent{
		MessageId: "msg-1", Status: commonv1.MessageLifecycleStatus_MESSAGE_LIFECYCLE_STATUS_DELIVERED, OccurredAt: timestamppb.New(now),
	}
	rec := FromLifecycleEvent(event)
	if rec.EventType != "lifecycle" || rec.LifecycleStatus != "DELIVERED" {
		t.Fatalf("неверные поля: %+v", rec)
	}
}

func TestOutcomeStringCoversAllKnownValues(t *testing.T) {
	cases := map[commonv1.Outcome]string{
		commonv1.Outcome_OUTCOME_SUCCEEDED:                  "SUCCEEDED",
		commonv1.Outcome_OUTCOME_REJECTED:                   "REJECTED",
		commonv1.Outcome_OUTCOME_FAILED:                     "FAILED",
		commonv1.Outcome_OUTCOME_TIMED_OUT:                  "TIMED_OUT",
		commonv1.Outcome_OUTCOME_RETRY_EXHAUSTED:            "RETRY_EXHAUSTED",
		commonv1.Outcome_OUTCOME_SUBMISSION_OUTCOME_UNKNOWN: "SUBMISSION_OUTCOME_UNKNOWN",
		commonv1.Outcome_OUTCOME_DELIVERY_UNRESOLVED:        "DELIVERY_UNRESOLVED",
	}
	for proto, want := range cases {
		event := &commonv1.StageCompletedEvent{Outcome: proto, CompletedAt: timestamppb.Now()}
		if got := FromStageCompleted(event).Outcome; got != want {
			t.Fatalf("outcomeString(%v) = %s, want %s", proto, got, want)
		}
	}
}