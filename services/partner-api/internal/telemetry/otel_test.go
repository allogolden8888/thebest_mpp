package telemetry

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestMiddlewareRecordsSpanWithAttributes(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	tp := NewProvider(recorder)
	defer tp.Shutdown(t.Context())

	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
	})

	mw := Middleware(tp, func(r *http.Request) string { return "/v1/messages/status" })
	req := httptest.NewRequest(http.MethodGet, "/v1/messages/status?message_id=abc", nil)
	rec := httptest.NewRecorder()
	mw(next).ServeHTTP(rec, req)

	spans := recorder.Ended()
	if len(spans) != 1 {
		t.Fatalf("ожидали 1 завершённый span, получили %d", len(spans))
	}
	span := spans[0]
	if span.Name() != "HTTP GET /v1/messages/status" {
		t.Fatalf("неверное имя span: %q", span.Name())
	}

	attrs := map[string]string{}
	for _, kv := range span.Attributes() {
		attrs[string(kv.Key)] = kv.Value.Emit()
	}
	if attrs["http.method"] != "GET" {
		t.Fatalf("http.method = %q, want GET", attrs["http.method"])
	}
	if attrs["http.route"] != "/v1/messages/status" {
		t.Fatalf("http.route = %q, want /v1/messages/status", attrs["http.route"])
	}
	if attrs["http.status_code"] != "201" {
		t.Fatalf("http.status_code = %q, want 201", attrs["http.status_code"])
	}
}
