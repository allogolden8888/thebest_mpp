// Package signals — collect_signals (service_internal_methods.md §3.1):
// сбор Kafka consumer lag / error rate по стадиям через Prometheus HTTP API
// (instant query), плюс gRPC-сигналы backlog/lateness/hold от Scheduler
// (см. RawSignals ниже — публикуются как Prometheus-метрики самим
// Scheduler'ом, services_specifictaion.md §3.1, не отдельный gRPC вызов на
// первой версии: "сигнал накопленного backlog" идёт через тот же
// Prometheus HTTP API client, что и остальные метрики).
package signals

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
)

// RawSignal — один снятый показатель метрики для конкретного scope в
// конкретный момент (вход evaluate_hysteresis).
type RawSignal struct {
	MetricName string
	Value      float64
}

// promInstantQueryResponse — минимальное подмножество ответа Prometheus
// instant query API (/api/v1/query), нужное здесь: vector result, первый
// элемент, значение метрики.
type promInstantQueryResponse struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Value []interface{} `json:"value"` // [unix_timestamp, "string_value"]
		} `json:"result"`
	} `json:"data"`
	Error string `json:"error"`
}

// ParseInstantQueryResponse — чистая функция, разбирает тело ответа
// Prometheus в RawSignal. Юнит-тестируется на зафиксированных JSON-фикстурах
// без живого Prometheus.
func ParseInstantQueryResponse(metricName string, body []byte) (RawSignal, error) {
	var resp promInstantQueryResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return RawSignal{}, fmt.Errorf("unmarshal prometheus response: %w", err)
	}
	if resp.Status != "success" {
		return RawSignal{}, fmt.Errorf("prometheus query failed: %s", resp.Error)
	}
	if len(resp.Data.Result) == 0 {
		return RawSignal{}, fmt.Errorf("prometheus query %q returned no series", metricName)
	}
	if len(resp.Data.Result[0].Value) != 2 {
		return RawSignal{}, fmt.Errorf("unexpected value shape for %q", metricName)
	}
	str, ok := resp.Data.Result[0].Value[1].(string)
	if !ok {
		return RawSignal{}, fmt.Errorf("expected string sample value for %q", metricName)
	}
	v, err := strconv.ParseFloat(str, 64)
	if err != nil {
		return RawSignal{}, fmt.Errorf("parse sample value %q: %w", str, err)
	}
	return RawSignal{MetricName: metricName, Value: v}, nil
}

// Client — тонкая обёртка над Prometheus HTTP API. Реальный HTTP-клиент, но
// против живого Prometheus в этой песочнице не проверялся (нет поднятого
// kube-prometheus-stack, infra/terraform/observability.tf валиден, но не
// задеплоен — Фаза 2.3 не начата).
type Client struct {
	BaseURL    string
	HTTPClient *http.Client
}

func NewClient(baseURL string) *Client {
	return &Client{BaseURL: baseURL, HTTPClient: http.DefaultClient}
}

// Query выполняет instant query promQL и возвращает RawSignal.
func (c *Client) Query(ctx context.Context, metricName, promQL string) (RawSignal, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+"/api/v1/query", nil)
	if err != nil {
		return RawSignal{}, err
	}
	q := req.URL.Query()
	q.Set("query", promQL)
	req.URL.RawQuery = q.Encode()

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return RawSignal{}, fmt.Errorf("prometheus query request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return RawSignal{}, fmt.Errorf("read prometheus response body: %w", err)
	}

	return ParseInstantQueryResponse(metricName, body)
}
