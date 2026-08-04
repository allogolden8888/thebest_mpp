package grpcserver

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
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
	server := New(httpio.NewClientForTests(2*time.Second), core.NewTokenBucket(100, 100, time.Now()), publisher, fakeResolver{endpoint: srv.URL})

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
	server := New(httpio.NewClientForTests(2*time.Second), core.NewTokenBucket(100, 100, time.Now()), publisher, fakeResolver{endpoint: srv.URL})

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
	// CODE_REVIEW.md finding #7: TPS теперь проверяется внутри цикла по
	// сегментам, непосредственно перед реальным HTTP-вызовом — поэтому
	// запрос без сегментов вообще не потребовал бы токена. Сегмент нужен,
	// чтобы действительно упереться в TryAcquire.
	server := New(httpio.NewClientForTests(2*time.Second), core.NewTokenBucket(0, 0, time.Now()), publisher, fakeResolver{endpoint: "http://unused"})

	resp, err := server.Submit(context.Background(), &grpcv1.SubmitRequest{
		MessageId: "msg-1", OperatorId: "beeline_uz", RouteId: "route-1",
		Segments: []*grpcv1.MessageSegment{{SegmentId: 0, Content: []byte("hello")}},
	})
	if err != nil {
		t.Fatalf("Submit failed: %v", err)
	}
	if resp.GetStatus() != grpcv1.SubmitOutcomeStatus_SUBMIT_OUTCOME_STATUS_REJECTED || resp.GetReasonCode() != "TPS_THROTTLED" {
		t.Fatalf("ожидали REJECTED/TPS_THROTTLED, получили %v/%s", resp.GetStatus(), resp.GetReasonCode())
	}
}

// TestSubmitConsumesOneTpsTokenPerRealHttpRequest — CODE_REVIEW.md finding
// #7 (MEDIUM): раньше TPS проверялся один раз на весь Submit, сообщение из
// N сегментов тратило 1 токен, но генерировало N реальных запросов
// оператору — до Nx превышая настроенный лимит.
func TestSubmitConsumesOneTpsTokenPerRealHttpRequest(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status":"accepted","smsc_message_id":"smsc-x"}`))
	}))
	defer srv.Close()

	publisher := &fakePublisher{}
	// capacity=1, refill=0 — ровно один реальный HTTP-запрос пройдёт,
	// второй сегмент должен упереться в TPS.
	server := New(httpio.NewClientForTests(2*time.Second), core.NewTokenBucket(1, 0, time.Now()), publisher, fakeResolver{endpoint: srv.URL})

	resp, err := server.Submit(context.Background(), &grpcv1.SubmitRequest{
		MessageId: "msg-tps", OperatorId: "beeline_uz", RouteId: "route-1",
		DestinationAddress: "998901234567",
		Segments: []*grpcv1.MessageSegment{
			{SegmentId: 0, Content: []byte("part1")},
			{SegmentId: 1, Content: []byte("part2")},
		},
	})
	if err != nil {
		t.Fatalf("Submit failed: %v", err)
	}
	if resp.GetStatus() != grpcv1.SubmitOutcomeStatus_SUBMIT_OUTCOME_STATUS_REJECTED || resp.GetReasonCode() != "TPS_THROTTLED" {
		t.Fatalf("ожидали REJECTED/TPS_THROTTLED на втором сегменте, получили %v/%s", resp.GetStatus(), resp.GetReasonCode())
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("ожидали ровно 1 реальный HTTP-запрос (1 токен), получили %d", got)
	}
}

func TestSubmitAmbiguousWhenRouteNotFound(t *testing.T) {
	publisher := &fakePublisher{}
	server := New(httpio.NewClientForTests(2*time.Second), core.NewTokenBucket(100, 100, time.Now()), publisher,
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

// TestSubmitMultiSegmentPublishesEventPerSegmentWithCorrectIDs —
// CODE_REVIEW.md HIGH finding #3: раньше segment_id в опубликованном
// событии был захардкожен в 0, а smsc_message_id перезаписывался на
// каждой итерации — для 3-сегментного SMS данные сегментов 0 и 1
// терялись безвозвратно (позже DLR для них падал в
// operator.dlr.unresolved). Не было ни одного теста на >1 сегмент.
func TestSubmitMultiSegmentPublishesEventPerSegmentWithCorrectIDs(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&calls, 1)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status":"accepted","smsc_message_id":"smsc-seg-` + string(rune('0'+n)) + `"}`))
	}))
	defer srv.Close()

	publisher := &fakePublisher{}
	server := New(httpio.NewClientForTests(2*time.Second), core.NewTokenBucket(100, 100, time.Now()), publisher, fakeResolver{endpoint: srv.URL})

	resp, err := server.Submit(context.Background(), &grpcv1.SubmitRequest{
		MessageId: "msg-multi", OperatorId: "beeline_uz", RouteId: "route-1",
		DestinationAddress: "998901234567",
		Segments: []*grpcv1.MessageSegment{
			{SegmentId: 0, Content: []byte("part1")},
			{SegmentId: 1, Content: []byte("part2")},
			{SegmentId: 2, Content: []byte("part3")},
		},
	})
	if err != nil {
		t.Fatalf("Submit failed: %v", err)
	}
	if resp.GetStatus() != grpcv1.SubmitOutcomeStatus_SUBMIT_OUTCOME_STATUS_ACCEPTED {
		t.Fatalf("ожидали ACCEPTED, получили %v/%s", resp.GetStatus(), resp.GetReasonCode())
	}
	if len(publisher.events) != 3 {
		t.Fatalf("ожидали 3 опубликованных события (по одному на сегмент), получили %d", len(publisher.events))
	}
	for i, ev := range publisher.events {
		if ev.GetSegmentId() != int32(i) {
			t.Fatalf("событие %d: segment_id = %d, want %d", i, ev.GetSegmentId(), i)
		}
		if ev.GetSmscMessageId() == "" {
			t.Fatalf("событие %d: пустой smsc_message_id", i)
		}
	}
	if publisher.events[0].GetSmscMessageId() == publisher.events[1].GetSmscMessageId() {
		t.Fatalf("smsc_message_id сегментов 0 и 1 совпадают — данные одного из сегментов потеряны")
	}
}

// TestSubmitRetryAfterPartialFailureDoesNotResendAcceptedSegment —
// CODE_REVIEW.md HIGH finding #4: при частичном сбое (сегмент 0 принят —
// уже необратимо ушёл абоненту, — сегмент 1 отклонён/недоступен) весь
// Submit возвращал ошибку; ретрай всего запроса заново отправлял уже
// принятый сегмент 0 физически повторно, приводя к дублирующей доставке.
func TestSubmitRetryAfterPartialFailureDoesNotResendAcceptedSegment(t *testing.T) {
	var segment0Calls, segment1Calls int32
	failSegment1 := true
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(string(body), "cGFydDE="): // base64("part1") — сегмент 0
			atomic.AddInt32(&segment0Calls, 1)
			w.Write([]byte(`{"status":"accepted","smsc_message_id":"smsc-part1"}`))
		default: // сегмент 1
			atomic.AddInt32(&segment1Calls, 1)
			if failSegment1 {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			w.Write([]byte(`{"status":"accepted","smsc_message_id":"smsc-part2"}`))
		}
	}))
	defer srv.Close()

	publisher := &fakePublisher{}
	server := New(httpio.NewClientForTests(2*time.Second), core.NewTokenBucket(100, 100, time.Now()), publisher, fakeResolver{endpoint: srv.URL})

	req := &grpcv1.SubmitRequest{
		MessageId: "msg-retry", OperatorId: "beeline_uz", RouteId: "route-1",
		DestinationAddress: "998901234567",
		Segments: []*grpcv1.MessageSegment{
			{SegmentId: 0, Content: []byte("part1")},
			{SegmentId: 1, Content: []byte("part2")},
		},
	}

	resp1, err := server.Submit(context.Background(), req)
	if err != nil {
		t.Fatalf("первый Submit failed: %v", err)
	}
	if resp1.GetStatus() != grpcv1.SubmitOutcomeStatus_SUBMIT_OUTCOME_STATUS_AMBIGUOUS {
		t.Fatalf("ожидали AMBIGUOUS на первом вызове (сегмент 1 упал), получили %v", resp1.GetStatus())
	}
	if len(publisher.events) != 1 || publisher.events[0].GetSegmentId() != 0 {
		t.Fatalf("ожидали 1 событие для сегмента 0 после первого вызова, получили %+v", publisher.events)
	}
	if got := atomic.LoadInt32(&segment0Calls); got != 1 {
		t.Fatalf("сегмент 0 должен был отправиться оператору ровно 1 раз, получили %d", got)
	}

	// Delivery Service ретраит весь SubmitRequest тем же message_id после
	// AMBIGUOUS — теперь сегмент 1 у оператора отвечает успехом.
	failSegment1 = false
	resp2, err := server.Submit(context.Background(), req)
	if err != nil {
		t.Fatalf("второй Submit failed: %v", err)
	}
	if resp2.GetStatus() != grpcv1.SubmitOutcomeStatus_SUBMIT_OUTCOME_STATUS_ACCEPTED {
		t.Fatalf("ожидали ACCEPTED после ретрая, получили %v/%s", resp2.GetStatus(), resp2.GetReasonCode())
	}
	if got := atomic.LoadInt32(&segment0Calls); got != 1 {
		t.Fatalf("сегмент 0 НЕ должен был отправиться оператору повторно при ретрае, получили %d вызовов", got)
	}
	if got := atomic.LoadInt32(&segment1Calls); got != 2 {
		t.Fatalf("сегмент 1 должен был быть отправлен дважды (первая попытка упала, вторая — ретрай), получили %d", got)
	}
	if len(publisher.events) != 2 || publisher.events[1].GetSegmentId() != 1 {
		t.Fatalf("ожидали второе событие для сегмента 1 после ретрая, получили %+v", publisher.events)
	}
}