package dlr

import (
	"testing"
	"time"

	commonv1 "mpp/platformcontracts/common/v1"
	eventsv1 "mpp/platformcontracts/events/v1"

	"mpp/dlr-manager/internal/correlation"
)

func TestBuildDeliveryStatusEventCarriesMessageIdFromCorrelation(t *testing.T) {
	rec := &correlation.Record{MessageID: "m1", StageExecutionID: "se1"}
	d := &eventsv1.OperatorDlr{OperatorId: "beeline", RawStatus: "DELIVRD"}
	event := BuildDeliveryStatusEvent("evt1", rec, d, "DELIVERED", time.Now())

	if event.GetMessageId() != "m1" {
		t.Errorf("MessageId = %q, want m1 (из correlation, не из DLR)", event.GetMessageId())
	}
	if event.GetOperatorId() != "beeline" {
		t.Errorf("OperatorId = %q, want beeline", event.GetOperatorId())
	}
	if event.GetNormalizedStatus() != "DELIVERED" {
		t.Errorf("NormalizedStatus = %q, want DELIVERED", event.GetNormalizedStatus())
	}
	if event.GetRawOperatorStatus() != "DELIVRD" {
		t.Errorf("RawOperatorStatus = %q, want DELIVRD", event.GetRawOperatorStatus())
	}
}

func TestBuildRetryTaskCarriesAttemptPassthroughNotIncremented(t *testing.T) {
	// Регрессия на находку в комментарии builders.go: DLR Manager не должен
	// сам инкрементировать attempt — это делает Scheduler Background Lane
	// при dispatch (уже реализовано Субагентом 1).
	now := time.Now()
	receivedAt := now.Add(-time.Minute)
	task := BuildRetryTask("evt1", 3, receivedAt, time.Hour, 30*time.Second, now)

	if task.GetAttempt() != 3 {
		t.Errorf("Attempt = %d, want 3 (passthrough, не инкремент)", task.GetAttempt())
	}
	if task.GetTaskType() != commonv1.BackgroundTaskType_BACKGROUND_TASK_TYPE_DLR_CORRELATION_RETRY {
		t.Errorf("TaskType = %v, want DLR_CORRELATION_RETRY", task.GetTaskType())
	}
	if task.GetTargetTopic() != "operator.dlr.unresolved" {
		t.Errorf("TargetTopic = %q, want operator.dlr.unresolved", task.GetTargetTopic())
	}
	if task.GetSourceEventId() != "evt1" {
		t.Errorf("SourceEventId = %q, want evt1", task.GetSourceEventId())
	}
}

func TestBuildRetryTaskDeadlineTracksCorrelationWindowFromReceivedAt(t *testing.T) {
	now := time.Now()
	receivedAt := now.Add(-10 * time.Minute)
	window := time.Hour
	task := BuildRetryTask("evt1", 1, receivedAt, window, 30*time.Second, now)

	wantDeadline := receivedAt.Add(window)
	gotDeadline := task.GetDeadline().AsTime()
	if gotDeadline.Sub(wantDeadline).Abs() > time.Second {
		t.Errorf("Deadline = %v, want ~%v (received_at + window, не now + window)", gotDeadline, wantDeadline)
	}
}
