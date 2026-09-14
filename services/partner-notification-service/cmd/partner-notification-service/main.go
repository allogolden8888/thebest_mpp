package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	_ "github.com/KimMachineGun/automemlimit"
	"github.com/twmb/franz-go/pkg/kgo"

	"mpp/partner-notification-service/internal/config"
	"mpp/partner-notification-service/internal/health"
	"mpp/partner-notification-service/internal/kafkaio"
	"mpp/partner-notification-service/internal/msgctx"
	"mpp/partner-notification-service/internal/notify"
	"mpp/partner-notification-service/internal/pending"
	"mpp/partner-notification-service/internal/registry"
)

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// buildRedisRuntimeURL — тот же реальный, ранее найденный и исправленный
// во всех затронутых сервисах пробел (см. services/dlr-manager/README.md,
// "Реальная находка (систематическая...)"): k8s инжектит
// REDIS_RUNTIME_HOST/PORT/PASSWORD дискретно, не единую REDIS_RUNTIME_URL
// — применено здесь с самого начала, не задним числом.
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

// buildRedisConfigurationURL — same env-var discretization as
// buildRedisRuntimeURL, but for Configuration Redis (the third Redis
// instance, `redis-configuration.mpp.svc`) — matches the
// REDIS_CONFIGURATION_HOST/PORT/PASSWORD convention already used by
// config-cache-projector (cmd/config-cache-projector/main.go) and
// billing-service (RedisUrl.buildConfigurationUrl()).
func buildRedisConfigurationURL() string {
	if v := os.Getenv("REDIS_CONFIGURATION_URL"); v != "" {
		return v
	}
	host := getenv("REDIS_CONFIGURATION_HOST", "redis-configuration.mpp.svc")
	port := getenv("REDIS_CONFIGURATION_PORT", "6379")
	password := os.Getenv("REDIS_CONFIGURATION_PASSWORD")
	if password == "" {
		return fmt.Sprintf("redis://%s:%s/0", host, port)
	}
	return fmt.Sprintf("redis://:%s@%s:%s/0", url.QueryEscape(password), host, port)
}

func loadPartnerSnapshotFromFile(path string) config.Snapshot {
	data, err := os.ReadFile(path)
	if err != nil {
		log.Fatalf("не удалось прочитать %s: %v", path, err)
	}
	var partner config.Partner
	if err := json.Unmarshal(data, &partner); err != nil {
		log.Fatalf("partner.schema.json форма: %v", err)
	}
	return config.NewSnapshot([]config.Partner{partner})
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

	// BACKOFFICE_ROADMAP.md "Production Readiness Review" P0 #4: partner
	// config used to load exactly once from PARTNER_CONFIG_PATH at
	// startup and never again. Now that's an opt-in fallback for local
	// dev without Configuration Redis (PARTNER_CONFIG_PATH set
	// explicitly) — the default path bootstraps from Configuration Redis
	// (the same store config-cache-projector already projects into) and
	// stays live via a config.changes consumer (see configStore/
	// configSource below and kafkaio.HandleConfigChangeRecord).
	var (
		configStore  *config.Store
		configSource *config.RedisSource
	)
	if partnerPath := os.Getenv("PARTNER_CONFIG_PATH"); partnerPath != "" {
		log.Printf("PARTNER_CONFIG_PATH=%s задан явно — партнёрский снапшот грузится один раз из статического файла (local dev без Configuration Redis), config.changes НЕ отслеживается", partnerPath)
		configStore = config.NewStore(loadPartnerSnapshotFromFile(partnerPath))
	} else {
		var err error
		configSource, err = config.NewRedisSource(buildRedisConfigurationURL())
		if err != nil {
			log.Fatalf("config.NewRedisSource: %v", err)
		}
		bootstrapCtx, bootstrapCancel := context.WithTimeout(ctx, 30*time.Second)
		snapshot, err := configSource.LoadAll(bootstrapCtx)
		bootstrapCancel()
		if err != nil {
			log.Fatalf("bootstrap-загрузка партнёров из Configuration Redis: %v", err)
		}
		configStore = config.NewStore(snapshot)
		log.Printf("bootstrap: партнёрский снапшот загружен из Configuration Redis (%d партнёров)", snapshot.Len())
	}

	redisRuntimeURL := buildRedisRuntimeURL()
	msgctxStore, err := msgctx.NewStore(redisRuntimeURL)
	if err != nil {
		log.Fatalf("msgctx.NewStore: %v", err)
	}
	defer msgctxStore.Close()

	registryStore, err := registry.NewStore(redisRuntimeURL)
	if err != nil {
		log.Fatalf("registry.NewStore: %v", err)
	}
	defer registryStore.Close()

	pendingStore, err := pending.NewStore(redisRuntimeURL)
	if err != nil {
		log.Fatalf("pending.NewStore: %v", err)
	}
	defer pendingStore.Close()

	smppClient := notify.NewSmppClient(5 * time.Second)
	defer smppClient.Close()
	restClient := notify.NewRestClient(5 * time.Second)

	brokers := strings.Split(getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-bootstrap.mpp.svc:9092"), ",")
	// configSource == nil means the static-file fallback is in effect
	// (PARTNER_CONFIG_PATH was set explicitly) — no live config source to
	// refresh from, so config.changes isn't worth subscribing to.
	var consumer *kafkaio.Consumer
	if configSource != nil {
		consumer, err = kafkaio.NewConsumer(brokers, "partner-notification-service", kafkaio.TopicConfigChanges)
	} else {
		consumer, err = kafkaio.NewConsumer(brokers, "partner-notification-service")
	}
	if err != nil {
		log.Fatalf("kafkaio.NewConsumer: %v", err)
	}
	defer consumer.Close()
	if configSource != nil {
		defer configSource.Close()
	}

	producer, err := kafkaio.NewProducer(brokers)
	if err != nil {
		log.Fatalf("kafkaio.NewProducer: %v", err)
	}
	defer producer.Close()

	deps := kafkaio.Deps{
		Snapshot:        configStore,
		MsgCtxStore:     msgctxStore,
		RegistryStore:   registryStore,
		PendingStore:    pendingStore,
		SmppClient:      smppClient,
		RestClient:      restClient,
		Producer:        producer,
		NotificationTTL: 24 * time.Hour,
		// RetryBackoffBase/Max — см. schedule.NextRetryDelay: full jitter
		// exponential backoff, попытка 1 -> [0,30s), попытка 2 -> [0,60s),
		// ... капается на 5 минут, а не растёт неограниченно к 24ч TTL.
		RetryBackoffBase: 30 * time.Second,
		RetryBackoffMax:  5 * time.Minute,
		// MaxAttempts — реальная находка нагрузочного тестирования: раньше
		// единственным пределом был 24ч TTL, так что стабильно отказывающий
		// partner-webhook (или любой другой постоянно недоступный endpoint)
		// ретраился вплоть до суток на сообщение — см. PublishArchived. 2
		// попытки (первая из message.lifecycle + один ретрай), затем архив.
		MaxAttempts: 2,
	}

	healthState.SetReady(true)

	errHandler := func(err error) { log.Printf("partner-notification-service: %v", err) }

	for {
		select {
		case <-ctx.Done():
			_ = healthServer.Close()
			return
		default:
		}

		pollCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		consumer.PollOnce(pollCtx, func(record *kgo.Record) {
			if record.Topic == kafkaio.TopicConfigChanges {
				// BACKOFFICE_ROADMAP.md P0 #4 — live refresh path,
				// distinct from the message-delivery orchestration below
				// (same consumer/consumer group, branched by topic so no
				// second Kafka client is needed).
				if err := kafkaio.HandleConfigChangeRecord(ctx, configStore, configSource, record); err != nil {
					errHandler(err)
					return // не коммитим — at-least-once, переобработается
				}
				if err := consumer.CommitRecords(ctx, record); err != nil {
					errHandler(err)
				}
				return
			}
			if err := kafkaio.HandleRecord(ctx, deps, record); err != nil {
				errHandler(err)
				return // не коммитим — at-least-once, переобработается
			}
			if err := consumer.CommitRecords(ctx, record); err != nil {
				errHandler(err)
			}
		}, errHandler)
		cancel()
	}
}
