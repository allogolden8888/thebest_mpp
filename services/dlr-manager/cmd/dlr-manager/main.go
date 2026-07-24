package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"google.golang.org/protobuf/proto"

	"mpp/dlr-manager/internal/correlation"
	"mpp/dlr-manager/internal/dlr"
	"mpp/dlr-manager/internal/health"
	"mpp/dlr-manager/internal/kafkaio"
	"mpp/dlr-manager/internal/pending"

	eventsv1 "mpp/platformcontracts/events/v1"
)

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// buildDatabaseURL — реальная находка: `k8s/generate_manifests.py`'s
// `SECRET_DEPENDENCIES`/`infra/secrets/generate_external_secrets.py`
// инжектят через `envFrom: secretRef` НЕ единую `DATABASE_URL`, а
// дискретные `POSTGRES_HOST`/`POSTGRES_PORT`/`POSTGRES_DB`/`POSTGRES_USER`/
// `POSTGRES_PASSWORD` — раньше этот сервис (и, как выяснилось при этой же
// находке, ВСЕ предыдущие сервисы этой сессии — billing-service,
// policy-service, delivery-service, partner-rest-receiver,
// dlr-correlation-writer) читал единственную переменную `DATABASE_URL`/
// `REDIS_*_URL`, которую реальный под НИКОГДА не установит — сервис бы
// молча падал обратно на hardcoded localhost/in-cluster-DNS дефолт без
// креденшлов и никогда не подключился бы в реальном кластере. `DATABASE_URL`
// как явный override оставлен (для локальной разработки/тестов) —
// приоритет у него, если задан, иначе — сборка из дискретных переменных.
func buildDatabaseURL() string {
	if v := os.Getenv("DATABASE_URL"); v != "" {
		return v
	}
	host := getenv("POSTGRES_HOST", "postgresql.mpp.svc")
	port := getenv("POSTGRES_PORT", "5432")
	db := getenv("POSTGRES_DB", "mpp")
	user := getenv("POSTGRES_USER", "mpp")
	password := os.Getenv("POSTGRES_PASSWORD")
	if password == "" {
		return fmt.Sprintf("postgres://%s@%s:%s/%s?sslmode=disable", url.QueryEscape(user), host, port, db)
	}
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", url.QueryEscape(user), url.QueryEscape(password), host, port, db)
}

// buildRedisRuntimeURL — тот же класс находки, что buildDatabaseURL, для
// `REDIS_RUNTIME_HOST`/`REDIS_RUNTIME_PORT`/`REDIS_RUNTIME_PASSWORD`.
func buildRedisRuntimeURL() string {
	if v := os.Getenv("REDIS_RUNTIME_URL"); v != "" {
		return v
	}
	host := getenv("REDIS_RUNTIME_HOST", "redis-runtime.mpp.svc")
	port := getenv("REDIS_RUNTIME_PORT", "6379")
	password := os.Getenv("REDIS_RUNTIME_PASSWORD")
	if password == "" {
		return fmt.Sprintf("redis://%s:%s/0", host, port)
	}
	return fmt.Sprintf("redis://:%s@%s:%s/0", url.QueryEscape(password), host, port)
}

func durationEnv(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if parsed, err := time.ParseDuration(v); err == nil {
			return parsed
		}
	}
	return fallback
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	healthState := &health.State{}
	healthServer := &http.Server{Addr: ":9090", Handler: health.Router(healthState)}
	go func() {
		if err := healthServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("health server: %v", err)
		}
	}()

	correlationStore, err := correlation.NewStore(ctx, buildDatabaseURL())
	if err != nil {
		log.Fatalf("correlation.NewStore: %v", err)
	}
	defer correlationStore.Close()

	pendingStore, err := pending.NewStore(buildRedisRuntimeURL())
	if err != nil {
		log.Fatalf("pending.NewStore: %v", err)
	}
	defer pendingStore.Close()

	brokers := strings.Split(getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-bootstrap.mpp.svc:9092"), ",")
	consumer, err := kafkaio.NewConsumer(brokers, "dlr-manager")
	if err != nil {
		log.Fatalf("kafkaio.NewConsumer: %v", err)
	}
	defer consumer.Close()

	producer, err := kafkaio.NewProducer(brokers)
	if err != nil {
		log.Fatalf("kafkaio.NewProducer: %v", err)
	}
	defer producer.Close()

	// 48ч — та же граница, что retention correlation-записей в PostgreSQL
	// (`dlr.drop_old_correlation_partitions`, default 48ч,
	// `migrations/V009__dlr_correlation.sql`) — ретраить дольше этого
	// бессмысленно: даже если бы correlation появилась, партиция, где она
	// хранится, могла быть уже удалена retention'ом. Требует того же
	// уточнения по реальным SLA операторов, что и retention-константа.
	correlationWindow := durationEnv("DLR_CORRELATION_WINDOW", 48*time.Hour)
	// Backoff между попытками — не задокументирован нигде дословно,
	// разумный дефолт для первой итерации.
	retryBackoff := durationEnv("DLR_RETRY_BACKOFF", 30*time.Second)

	healthState.SetReady(true)

	errHandler := func(err error) { log.Printf("dlr-manager: %v", err) }

	for {
		select {
		case <-ctx.Done():
			_ = healthServer.Close()
			return
		default:
		}

		pollCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		consumer.PollOnce(pollCtx, func(fr kafkaio.FetchedRecord) {
			if err := handleRecord(ctx, fr, correlationStore, pendingStore, producer, correlationWindow, retryBackoff); err != nil {
				errHandler(err)
				return // не коммитим — at-least-once, переобработается
			}
			if err := consumer.CommitRecords(ctx, fr.Raw); err != nil {
				errHandler(err)
			}
		}, errHandler)
		cancel()
	}
}

// handleRecord — вся оркестрация on_raw_dlr для одной записи, включает
// реальный I/O (Postgres/Redis/Kafka publish) — тонкая обвязка вокруг
// dlr.Decide (чистая функция, тестируется отдельно в dlr/decision_test.go).
func handleRecord(
	ctx context.Context,
	fr kafkaio.FetchedRecord,
	correlationStore *correlation.Store,
	pendingStore *pending.Store,
	producer *kafkaio.Producer,
	correlationWindow, retryBackoff time.Duration,
) error {
	var dlrEvent *eventsv1.OperatorDlr
	var eventID string
	var attempt int32 = 1

	if fr.FromRetry {
		task, err := decodeSchedulerBackgroundTask(fr.Raw.Value)
		if err != nil {
			return err
		}
		eventID = task.GetSourceEventId()
		attempt = task.GetAttempt()
		cached, found, err := pendingStore.Get(ctx, eventID)
		if err != nil {
			return err
		}
		if !found {
			// Кэш истёк/эвиктнут, либо задача не наша — известное
			// ограничение, см. README. Ничего не можем сделать, но и
			// ошибкой это не считаем — оффсет коммитится.
			log.Printf("dlr-manager: pending DLR для event_id=%s не найден в кэше, retry невозможен", eventID)
			return nil
		}
		dlrEvent = cached
	} else {
		decoded, err := dlr.DecodeOperatorDlr(fr.Raw.Value)
		if err != nil {
			return err
		}
		dlrEvent = decoded
		eventID = dlr.DeriveEventID(dlrEvent)
	}

	normalizedStatus, recognized := dlr.NormalizeStatus(dlrEvent.GetRawStatus())

	rec, err := correlationStore.Lookup(ctx, dlrEvent.GetOperatorId(), dlrEvent.GetSmscMessageId(), dlrEvent.GetSegmentId())
	if err != nil {
		return err
	}

	receivedAt := dlrEvent.GetReceivedAt().AsTime()
	decision := dlr.Decide(receivedAt, normalizedStatus, recognized, rec, time.Now(), correlationWindow)

	switch decision.Kind {
	case dlr.KindPublishDeliveryStatus:
		event := dlr.BuildDeliveryStatusEvent(eventID, rec, dlrEvent, decision.NormalizedStatus, time.Now())
		if err := producer.PublishDeliveryStatus(ctx, event); err != nil {
			return err
		}
		if fr.FromRetry {
			return pendingStore.Delete(ctx, eventID)
		}
		return nil

	case dlr.KindScheduleRetry:
		if !fr.FromRetry {
			// Кэшируем только на первой попытке — на повторной уже
			// закэшировано под тем же eventID (детерминированный
			// DeriveEventID), перезаписывать нечем (тот же payload).
			if err := pendingStore.Save(ctx, eventID, dlrEvent, correlationWindow); err != nil {
				return err
			}
		}
		task := dlr.BuildRetryTask(eventID, attempt, receivedAt, correlationWindow, retryBackoff, time.Now())
		return producer.PublishRetryTask(ctx, task)

	case dlr.KindPublishDlq:
		if err := producer.PublishDlq(ctx, dlrEvent); err != nil {
			return err
		}
		if fr.FromRetry {
			return pendingStore.Delete(ctx, eventID)
		}
		return nil

	case dlr.KindDropUnrecognizedStatus:
		log.Printf("dlr-manager: нераспознанный raw_status=%q от operator_id=%q, событие отброшено", dlrEvent.GetRawStatus(), dlrEvent.GetOperatorId())
		if fr.FromRetry {
			return pendingStore.Delete(ctx, eventID)
		}
		return nil
	}
	return nil
}

func decodeSchedulerBackgroundTask(payload []byte) (*eventsv1.SchedulerBackgroundTask, error) {
	var task eventsv1.SchedulerBackgroundTask
	if err := proto.Unmarshal(payload, &task); err != nil {
		return nil, fmt.Errorf("unmarshal SchedulerBackgroundTask: %w", err)
	}
	if task.GetSourceEventId() == "" {
		return nil, fmt.Errorf("SchedulerBackgroundTask без source_event_id")
	}
	return &task, nil
}
