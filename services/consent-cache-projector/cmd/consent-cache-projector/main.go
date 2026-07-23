// Consent Cache Projector — проекция policy.subscriber_consent в Runtime
// Redis (data_infrastructure_spec.md §1.9c), симметричен Config Cache
// Projector, но hot-path (Policy читает на каждое сообщение).
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"mpp/consent-cache-projector/internal/health"
	"mpp/consent-cache-projector/internal/kafkaio"
	"mpp/consent-cache-projector/internal/projector"

	eventsv1 "mpp/platformcontracts/events/v1"
)

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	healthState := &health.State{}
	healthSrv := &http.Server{Addr: ":9090", Handler: health.Router(healthState)}
	go func() {
		if err := healthSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("health server failed: %v", err)
		}
	}()

	redisClient := projector.NewClient(
		env("REDIS_RUNTIME_HOST", "localhost")+":"+env("REDIS_RUNTIME_PORT", "6379"),
		env("REDIS_RUNTIME_PASSWORD", ""),
	)
	defer redisClient.Close()

	brokers := strings.Split(env("KAFKA_BOOTSTRAP_SERVERS", "kafka-bootstrap.mpp.svc:9092"), ",")
	consumer, err := kafkaio.NewConsumer(brokers, "consent-cache-projector")
	if err != nil {
		log.Fatalf("не удалось создать Kafka consumer: %v", err)
	}
	defer consumer.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go consumer.Run(ctx,
		func(event *eventsv1.ConfigChangeEvent) {
			if err := redisClient.ApplyConsentChange(ctx, event); err != nil {
				log.Printf("apply_consent_change failed for entity_id=%s: %v", event.GetEntityId(), err)
			}
		},
		func(err error) {
			log.Printf("config.changes consume error: %v", err)
		},
	)

	healthState.SetReady(true)
	log.Println("consent-cache-projector готов")

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	log.Println("остановка consent-cache-projector")
	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = healthSrv.Shutdown(shutdownCtx)
}