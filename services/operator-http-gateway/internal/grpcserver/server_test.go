package grpcserver

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	eventsv1 "mpp/platformcontracts/events/v1"
	grpcv1 "mpp/platformcontracts/grpc/v1"

	"mpp/operator-http-gateway/internal/core"
	"mpp/operator-http-gateway/internal/httpio"
)

type fakeResolver struct {
	endpoint string
	err      error
}

func (f fakeResolver) ResolveEndpoint(operatorID, routeID string) (string, error) {
	return f.endpoint, f.err
}

type fakePublisher struct {
	events []*eventsv1.OperatorSubmitAccepted
}

func (f *fakePublisher) PublishSubmitAccepted(ctx context.Context, event *eventsv1.OperatorSubmitAccepted) error {
	f.events = append(f.events, event)
	return nil
}

func TestSubmitAcceptedPublishesEvent(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status":"accepted","smsc_message_id":"smsc-1"}`))
	}))
	defer srv.Close()

	publisher := &fakePublisher{}
	server := New(httpio.NewClient(2*time.Second), core.NewTokenBucket(100, 100, time.Now()), publisher, fakeResolver{endpoint: srv.URL})

	resp, err := server.Submit(context.Background(), &grpcv1.SubmitRequest{
		MessageId: "msg-1", OperatorId: "beeline_uz", RouteId: "route-1",
		DestinationAddress: "998901234567",
		Segments:           []*grpcv1.MessageSegment{{SegmentId: 1, Content: []byte("hello"), Encoding: "GSM7"}},
	})
	if err != nil {
		t.Fatalf("Submit failed: %v", err)
	}
	if resp.GetStatus() != grpcv1.SubmitOutcomeStatus_SUBMIT_OUTCOME_STATUS_ACCEPTED {
		t.Fatalf("ожидали ACCEPTED, получили %v", resp.GetStatus())
	}
	if resp.GetSmscMessageId() != "smsc-1" {
		t.Fatalf("неверный smsc_message_id: %s", resp.GetSmscMessageId())
	}
	if len(publisher.events) != 1 {
		t.Fatalf("ожидали 1 опубликованное событие, получили %d", len(publisher.events))
	}
}

func TestSubmitRejectedByOperatorDoesNotPublish(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()

	publisher := &fakePublisher{}
	server := New(httpio.NewClient(2*time.Second), core.NewTokenBucket(100, 100, time.Now()), publisher, fakeResolver{endpoint: srv.URL})

	resp, err := server.Submit(context.Background(), &grpcv1.SubmitRequest{
		MessageId: "msg-1", OperatorId: "beeline_uz", RouteId: "route-1",
		DestinationAddress: "998901234567",
		Segments:           []*grpcv1.MessageSegment{{SegmentId: 1, Content: []byte("hello")}},
	})
	if err != nil {
		t.Fatalf("Submit failed: %v", err)
	}
	if resp.GetStatus() != grpcv1.SubmitOutcomeStatus_SUBMIT_OUTCOME_STATUS_REJECTED {
		t.Fatalf("ожидали REJECTED, получили %v", resp.GetStatus())
	}
	if len(publisher.events) != 0 {
		t.Fatalf("rejected submit не должен публиковать событие")
	}
}

func TestSubmitThrottledByTps(t *testing.T) {
	publisher := &fakePublisher{}
	server := New(httpio.NewClient(2*time.Second), core.NewTokenBucket(0, 0, time.Now()), publisher, fakeResolver{endpoint: "http://unused"})

	resp, err := server.Submit(context.Background(), &grpcv1.SubmitRequest{
		MessageId: "msg-1", OperatorId: "beeline_uz", RouteId: "route-1",
	})
	if err != nil {
		t.Fatalf("Submit failed: %v", err)
	}
	if resp.GetStatus() != grpcv1.SubmitOutcomeStatus_SUBMIT_OUTCOME_STATUS_REJECTED || resp.GetReasonCode() != "TPS_THROTTLED" {
		t.Fatalf("ожидали REJECTED/TPS_THROTTLED, получили %v/%s", resp.GetStatus(), resp.GetReasonCode())
	}
}

func TestSubmitAmbiguousWhenRouteNotFound(t *testing.T) {
	publisher := &fakePublisher{}
	server := New(httpio.NewClient(2*time.Second), core.NewTokenBucket(100, 100, time.Now()), publisher,
		fakeResolver{err: context.DeadlineExceeded})

	resp, err := server.Submit(context.Background(), &grpcv1.SubmitRequest{
		MessageId: "msg-1", OperatorId: "beeline_uz", RouteId: "route-1",
	})
	if err != nil {
		t.Fatalf("Submit failed: %v", err)
	}
	if resp.GetStatus() != grpcv1.SubmitOutcomeStatus_SUBMIT_OUTCOME_STATUS_AMBIGUOUS {
		t.Fatalf("ожидали AMBIGUOUS при ненайденном route, получили %v", resp.GetStatus())
	}
}