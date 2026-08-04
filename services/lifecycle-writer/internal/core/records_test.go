package core

import (
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "mpp/platformcontracts/common/v1"
	eventsv1 "mpp/platformcontracts/events/v1"
)

func TestFromIncomingMessageMapsFields(t *testing.T) {
	now := time.Date(2026, 7, 24, 10, 0, 0, 0, time.UTC)
	msg := &eventsv1.IncomingMessage{
		MessageId:     "msg-1",
		TraceId:       "trace-1",
		PartnerId:     "acme",
		ApplicationId: "acme_main",
		ReceivedAt:    timestamppb.New(now),
	}

	row := FromIncomingMessage(msg)
	if row.MessageID != "msg-1" || row.PartnerID != "acme" || row.ApplicationID != "acme_main" {
		t.Fatalf("неверные поля: %+v", row)
	}
	if row.Terminal {
		t.Fatalf("новое сообщение не должно быть terminal")
	}
	if !row.Timestamp.Equal(now) {
		t.Fatalf("timestamp не совпадает: %v", row.Timestamp)
	}
}

func TestFromLifecycleEventBuildsUpdateAndHistory(t *testing.T) {
	now := time.Date(2026, 7, 24, 11, 0, 0, 0, time.UTC)
	event := &eventsv1.MessageLifecycleEvent{
		EventId:          "evt-1",
		MessageId:        "msg-1",
		Status:           commonv1.MessageLifecycleStatus_MESSAGE_LIFECYCLE_STATUS_DELIVERED,
		LifecycleVersion: 3,
		Terminal:         true,
		OccurredAt:       timestamppb.New(now),
	}

	update, history := FromLifecycleEvent(event)

	if update.MessageID != "msg-1" || update.CurrentStatus != "DELIVERED" || !update.Terminal {
		t.Fatalf("неверный update: %+v", update)
	}
	if history.LifecycleVersion != 3 || history.EventID != "evt-1" || history.Status != "DELIVERED" {
		t.Fatalf("неверная history-строка: %+v", history)
	}
	if !history.OccurredAt.Equal(now) {
		t.Fatalf("occurred_at не совпадает: %v", history.OccurredAt)
	}
}

func TestFromDlqRecordMarshalsOriginalCommand(t *testing.T) {
	originalCmd := &commonv1.StageExecuteCommand{
		EventId:   "evt-orig",
		MessageId: "msg-1",
		StageName: commonv1.StageName_STAGE_NAME_BILLING,
	}
	rec := &eventsv1.DlqRecord{
		StageExecutionId: "exec-1",
		MessageId:        "msg-1",
		StageName:        commonv1.StageName_STAGE_NAME_BILLING,
		Attempt:          3,
		OriginalCommand:  originalCmd,
		ReasonCode:       "RETRY_EXHAUSTED",
		CreatedAt:        timestamppb.Now(),
	}

	row, err := FromDlqRecord(rec)
	if err != nil {
		t.Fatalf("FromDlqRecord failed: %v", err)
	}
	if row.StageExecutionID != "exec-1" || row.StageName != "BILLING" || row.Attempt != 3 {
		t.Fatalf("неверные поля: %+v", row)
	}
	if len(row.OriginalCommand) == 0 {
		t.Fatalf("original_command не должен быть пустым")
	}
}

func TestFromDlqRecordRejectsMissingOriginalCommand(t *testing.T) {
	rec := &eventsv1.DlqRecord{StageExecutionId: "exec-1", MessageId: "msg-1"}
	_, err := FromDlqRecord(rec)
	if err == nil {
		t.Fatalf("ожидали ошибку без original_command")
	}
}
