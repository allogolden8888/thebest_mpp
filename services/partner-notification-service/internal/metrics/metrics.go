// Package metrics — BACKOFFICE_ROADMAP.md P1 "Observability": заменяет
// прежнюю /metrics-заглушку ("partner_notification_service_up 1") реальными
// счётчиками/гистограммой хот-пути — kafkaio.HandleRecord (обработка
// message.lifecycle/notification.retry) и attemptDelivery (реальная
// доставка партнёру по SMPP/REST). github.com/prometheus/client_golang — тот
// же выбор, что operator-http-gateway (см. его internal/metrics package
// doc): ни один Go-сервис в репозитории раньше не тянул metrics-библиотеку
// вообще, это промышленный стандарт для Prometheus-клиента в Go, не
// изобретённый заново формат.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// RecordsProcessedTotal — каждый вызов kafkaio.HandleRecord, по topic и
	// исходу. "error" — HandleRecord вернул ошибку (инфраструктурный сбой,
	// офсет не коммитится, см. package doc HandleRecord) — "ok" покрывает
	// ВСЕ остальные ветки (доставлено/заскейджулен retry/архивировано),
	// т.к. они не являются processing-ошибкой (тот же принцип, что уже
	// закреплён в самом HandleRecord).
	RecordsProcessedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "partner_notification_records_processed_total",
			Help: "Число обработанных Kafka-записей (kafkaio.HandleRecord), по топику и исходу (ok/error).",
		},
		[]string{"topic", "result"},
	)

	// RecordProcessingDuration — латентность HandleRecord целиком (может
	// включать реальный SMPP/REST-вызов через attemptDelivery).
	RecordProcessingDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "partner_notification_record_processing_duration_seconds",
			Help:    "Латентность kafkaio.HandleRecord целиком (включая попытку доставки партнёру).",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"topic"},
	)

	// DeliveryAttemptsTotal — attemptDelivery исходы, по каналу
	// (smpp/rest) и результату (delivered/failed) — отдельно от
	// RecordsProcessedTotal выше, потому что неудачная доставка партнёру НЕ
	// является processing-ошибкой (планируется retry), но это ровно то, что
	// нужно видеть для alerting на "партнёр не получает уведомления".
	DeliveryAttemptsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "partner_notification_delivery_attempts_total",
			Help: "Попытки доставки уведомления партнёру (attemptDelivery), по каналу и результату.",
		},
		[]string{"channel", "outcome"},
	)
)

func init() {
	prometheus.MustRegister(RecordsProcessedTotal, RecordProcessingDuration, DeliveryAttemptsTotal)
}

// Handler — promhttp.Handler() поверх дефолтного registry.
func Handler() http.Handler {
	return promhttp.Handler()
}
