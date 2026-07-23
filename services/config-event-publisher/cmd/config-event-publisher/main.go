// Config Event Publisher — poll_outbox -> publish_config_change ->
// mark_published (services_specifictaion.md §4.2, отдельный worker из того
// же логического Configuration Service репо, здесь — отдельная директория/
// бинарник per development_plan.md распределения).
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"mpp/config-event-publisher/internal/health"
	"mpp/config-event-publisher/internal/kafkaio"
	"mpp/config-event-publisher/internal/outbox"
)

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func buildPostgresDSN() string {
	host := env("POSTGRES_HOST", "localhost")
	port := env("POSTGRES_PORT", "5432")
	db := env("POSTGRES_DB", "mpp")
	user := env("POSTGRES_USER", "")
	password := env("POSTGRES_PASSWORD", "")
	if user == "" {
		return fmt.Sprintf("postgres://%s:%s/%s", host, port, db)
	}
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s", user, password, host, port, db)
}

func main() {
	healthState := &health.State{}
	healthSrv := &http.Server{Addr: ":9090", Handler: health.Router(healthState)}
	go func() {
		if err := healthSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("health server failed: %v", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	pool, err := pgxpool.New(ctx, buildPostgresDSN())
	cancel()
	if err != nil {
		log.Fatalf("не удалось создать пул подключений к PostgreSQL: %v", err)
	}
	defer pool.Close()
	store := outbox.New(pool)

	brokers := strings.Split(env("KAFKA_BOOTSTRAP_SERVERS", "kafka-bootstrap.mpp.svc:9092"), ",")
	publisher, err := kafkaio.NewPublisher(brokers)
	if err != nil {
		log.Fatalf("не удалось создать Kafka producer: %v", err)
	}
	defer publisher.Close()

	pollInterval := time.Duration(envInt("POLL_INTERVAL_MS", 500)) * time.Millisecond
	batchSize := envInt("POLL_BATCH_SIZE", 100)

	loopCtx, loopCancel := context.WithCancel(context.Background())
	defer loopCancel()
	go runPollLoop(loopCtx, store, publisher, pollInterval, batchSize)

	healthState.SetReady(true)
	log.Println("config-event-publisher готов")

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	log.Println("остановка config-event-publisher")
	loopCancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = healthSrv.Shutdown(shutdownCtx)
}

func runPollLoop(ctx context.Context, store *outbox.Store, publisher *kafkaio.Publisher, interval time.Duration, batchSize int) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			entries, err := store.PollOutbox(ctx, batchSize)
			if err != nil {
				log.Printf("poll_outbox failed: %v", err)
				continue
			}
			for _, e := range entries {
				if err := publisher.Publish(ctx, e); err != nil {
					log.Printf("publish_config_change failed for outbox id=%d: %v", e.ID, err)
					continue
				}
				if err := store.MarkPublished(ctx, e.ID); err != nil {
					log.Printf("mark_published failed for outbox id=%d: %v", e.ID, err)
				}
			}
		}
	}
}
