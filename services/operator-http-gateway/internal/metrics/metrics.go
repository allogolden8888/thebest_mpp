// Package metrics — BACKOFFICE_ROADMAP.md P1 "Observability": заменяет
// прежний хардкод-заглушку /metrics ("operator_http_gateway_up 1", ничего
// больше) на реальные счётчики/гистограмму хот-пути (handle_dlr_webhook,
// см. cmd/.../main.go + internal/webhook). Ни один Go-сервис в этом
// репозитории раньше не использовал github.com/prometheus/client_golang —
// это выбор промышленного стандарта Prometheus client для Go, не
// переизобретённый текстовый формат.
package metrics

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// WebhookRequestsTotal — handle_dlr_webhook исходы, по operator_id и
	// HTTP status code (строкой, "200"/"401"/... — тот же low-cardinality
	// набор, что уже enum'ится в webhook.go: BadRequest/Unauthorized/
	// RequestEntityTooLarge/OK).
	WebhookRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "operator_http_gateway_webhook_requests_total",
			Help: "Общее число запросов на /webhook/dlr/{operator_id}, по operator_id и HTTP-статусу ответа.",
		},
		[]string{"operator_id", "status"},
	)

	// WebhookRequestDuration — латентность handle_dlr_webhook целиком
	// (аутентификация + парсинг + normalize_and_publish_dlr до Kafka).
	WebhookRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "operator_http_gateway_webhook_request_duration_seconds",
			Help:    "Латентность обработки /webhook/dlr/{operator_id} целиком, включая normalize_and_publish_dlr.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"operator_id"},
	)
)

func init() {
	prometheus.MustRegister(WebhookRequestsTotal, WebhookRequestDuration)
}

// Handler — promhttp.Handler() поверх дефолтного registry (то есть той же
// go_* runtime-статистики, что client_golang собирает автоматически, плюс
// счётчики/гистограмма выше).
func Handler() http.Handler {
	return promhttp.Handler()
}

// statusRecorder — http.ResponseWriter, который запоминает записанный код
// статуса (webhook.Handler сам зовёт w.WriteHeader на каждом пути выхода —
// см. webhook.go), чтобы middleware мог замерить исход без переписывания
// самого обработчика.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

// InstrumentWebhookHandler — оборачивает handle_dlr_webhook (webhook.Handler
// из main.go), не меняя его внутреннюю логику. operator_id берётся из
// PathValue уже ПОСЛЕ вызова next (net/http заполняет PathValue при матче
// маршрута ДО вызова обработчика — см. net/http.ServeMux), пустая строка
// (маршрут не совпал / вызван напрямую) сведена к "unknown", чтобы не
// раздувать cardinality реальным мусором из внешнего интернета.
func InstrumentWebhookHandler(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		operatorID := r.PathValue("operator_id")
		if operatorID == "" {
			operatorID = "unknown"
		}
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		start := time.Now()
		next(rec, r)
		WebhookRequestDuration.WithLabelValues(operatorID).Observe(time.Since(start).Seconds())
		WebhookRequestsTotal.WithLabelValues(operatorID, statusText(rec.status)).Inc()
	}
}

func statusText(code int) string {
	switch code {
	case http.StatusOK:
		return "200"
	case http.StatusBadRequest:
		return "400"
	case http.StatusUnauthorized:
		return "401"
	case http.StatusRequestEntityTooLarge:
		return "413"
	default:
		return "other"
	}
}
