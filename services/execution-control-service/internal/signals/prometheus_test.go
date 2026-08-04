package signals

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestParseInstantQueryResponseExtractsValue(t *testing.T) {
	body := []byte(`{
		"status": "success",
		"data": {
			"resultType": "vector",
			"result": [{"metric": {}, "value": [1700000000, "0.62"]}]
		}
	}`)

	sig, err := ParseInstantQueryResponse("kafka_consumer_lag", body)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if sig.Value != 0.62 {
		t.Fatalf("ожидали 0.62, получили %v", sig.Value)
	}
	if sig.MetricName != "kafka_consumer_lag" {
		t.Fatalf("неверное имя метрики: %v", sig.MetricName)
	}
}

func TestParseInstantQueryResponseErrorsOnEmptyResult(t *testing.T) {
	body := []byte(`{"status": "success", "data": {"resultType": "vector", "result": []}}`)
	_, err := ParseInstantQueryResponse("no_such_metric", body)
	if err == nil {
		t.Fatalf("ожидали ошибку на пустом result — не должны молча возвращать 0")
	}
}

func TestParseInstantQueryResponseErrorsOnFailedStatus(t *testing.T) {
	body := []byte(`{"status": "error", "error": "bad query"}`)
	_, err := ParseInstantQueryResponse("m", body)
	if err == nil || !strings.Contains(err.Error(), "bad query") {
		t.Fatalf("ожидали ошибку, пробрасывающую текст Prometheus, получили: %v", err)
	}
}

// TestClientQueryAgainstFakeHTTPServer — реальный HTTP round-trip (httptest
// сервер, не мок транспорта) — доказывает, что запрос реально уходит на
// /api/v1/query с параметром query, а разбор ответа реально проходит через
// ParseInstantQueryResponse.
func TestClientQueryAgainstFakeHTTPServer(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/query" {
			t.Errorf("неверный путь: %s", r.URL.Path)
		}
		if r.URL.Query().Get("query") == "" {
			t.Errorf("ожидали непустой query-параметр")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1700000000,"0.15"]}]}}`))
	}))
	defer srv.Close()

	c := NewClient(srv.URL)
	c.HTTPClient.Timeout = 2 * time.Second

	sig, err := c.Query(t.Context(), "error_rate", `rate(errors_total[1m])`)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if sig.Value != 0.15 {
		t.Fatalf("ожидали 0.15, получили %v", sig.Value)
	}
}
