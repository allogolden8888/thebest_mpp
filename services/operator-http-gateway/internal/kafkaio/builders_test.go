package kafkaio

import (
	"testing"
	"time"

	commonv1 "mpp/platformcontracts/common/v1"

	"mpp/operator-http-gateway/internal/webhook"
)

func TestBuildSubmitAcceptedEventMapsFields(t *testing.T) {
	now := time.Date(2026, 7, 24, 10, 0, 0, 0, time.UTC)
	event := BuildSubmitAcceptedEvent("msg-1", "exec-1", "beeline_uz", "smsc-1", 1, now)

	if event.GetOperatorId() != "beeline_uz" || event.GetSmscMessageId() != "smsc-1" {
		t.Fatalf("неверные поля: %+v", event)
	}
	if event.GetProtocol() != commonv1.Protocol_PROTOCOL_HTTP {
		t.Fatalf("ожидали PROTOCOL_HTTP, получили %v", event.GetProtocol())
	}
	if !event.GetSubmittedAt().AsTime().Equal(now) {
		t.Fatalf("submitted_at не совпадает: %v", event.GetSubmittedAt().AsTime())
	}
}

func TestBuildDlrEventMapsFields(t *testing.T) {
	now := time.Now()
	dlr := webhook.RawDlr{SmscMessageID: "smsc-1", SegmentID: 2, RawStatus: "DELIVRD"}
	event := BuildDlrEvent("ucell_uz", dlr, now)

	if event.GetOperatorId() != "ucell_uz" || event.GetSmscMessageId() != "smsc-1" || event.GetRawStatus() != "DELIVRD" {
		t.Fatalf("неверные поля: %+v", event)
	}
	if event.GetProtocol() != commonv1.Protocol_PROTOCOL_HTTP {
		t.Fatalf("ожидали PROTOCOL_HTTP, получили %v", event.GetProtocol())
	}
	// CODE_REVIEW.md MEDIUM finding #6: segment_id раньше никогда не
	// заполнялся в OperatorDlr.
	if event.GetSegmentId() != 2 {
		t.Fatalf("segment_id = %d, want 2", event.GetSegmentId())
	}
}