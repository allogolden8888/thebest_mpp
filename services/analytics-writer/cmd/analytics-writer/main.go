// Analytics Writer (services_specifictaion.md §7.2): incoming.messages +
// stage.completed + message.lifecycle -> ClickHouse batch insert +
// материализованные агрегаты.
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"mpp/analytics-writer/internal/core"
	"mpp/analytics-writer/internal/health"
	"mpp/analytics-writer/internal/kafkaio"
	"mpp/analytics-writer/internal/store"
)

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

type buffer struct {
	mu      sync.Mutex
	records []core.NormalizedRecord
}

func main() {
	healthState := &health.State{}
	healthSrv := &http.Server{Addr: ":9090", Handler: health.Router(healthState)}
	go func() {
		if err := healthSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("health server failed: %v", err)
		}
	}()

	chAddr := env("CLICKHOUSE_HOST", "localhost") + ":" + env("CLICKHOUSE_PORT", "9000")
	db, err := store.New(chAddr, env("CLICKHOUSE_DB", "default"), env("CLICKHOUSE_USER", "default"), env("CLICKHOUSE_PASSWORD", ""))
	if err != nil {
		log.Fatalf("не удалось создать ClickHouse клиент: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := db.EnsureSchema(ctx); err != nil {
		log.Fatalf("EnsureSchema failed: %v", err)
	}
	cancel()

	brokers := strings.Split(env("KAFKA_BOOTSTRAP_SERVERS", "kafka-bootstrap.mpp.svc:9092"), ",")
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup("analytics-writer"),
		kgo.ConsumeTopics("incoming.messages", kafkaio.StageCompletedTopic, "message.lifecycle"),
	)
	if err != nil {
		log.Fatalf("не удалось создать Kafka consumer: %v", err)
	}
	defer client.Close()

	buf := &buffer{}
	batchSize, _ := strconv.Atoi(env("BATCH_SIZE", "500"))

	loopCtx, loopCancel := context.WithCancel(context.Background())
	defer loopCancel()

	go runConsumeLoop(loopCtx, client, buf)
	go runFlushLoop(loopCtx, db, buf, batchSize)

	healthState.SetReady(true)
	log.Println("analytics-writer готов")

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	log.Println("остановка analytics-writer")
	loopCancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = healthSrv.Shutdown(shutdownCtx)
}

func runConsumeLoop(ctx context.Context, client *kgo.Client, buf *buffer) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		fetches := client.PollFetches(ctx)
		if ctx.Err() != nil {
			return
		}
		fetches.EachRecord(func(rec *kgo.Record) {
			var normalized core.NormalizedRecord
			switch rec.Topic {
			case "incoming.messages":
				msg, err := kafkaio.DecodeIncomingMessage(rec.Value)
				if err != nil {
					log.Printf("decode incoming.messages failed: %v", err)
					return
				}
				normalized = core.FromIncomingMessage(msg)
			case kafkaio.StageCompletedTopic:
				event, err := kafkaio.DecodeStageCompleted(rec.Value)
				if err != nil {
					log.Printf("decode stage.completed failed: %v", err)
					return
				}
				normalized = core.FromStageCompleted(event)
			case "message.lifecycle":
				event, err := kafkaio.DecodeLifecycleEvent(rec.Value)
				if err != nil {
					log.Printf("decode message.lifecycle failed: %v", err)
					return
				}
				normalized = core.FromLifecycleEvent(event)
			default:
				return
			}

			buf.mu.Lock()
			buf.records = append(buf.records, normalized)
			buf.mu.Unlock()
		})
	}
}

func runFlushLoop(ctx context.Context, db *store.Store, buf *buffer, batchSize int) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			buf.mu.Lock()
			if len(buf.records) == 0 {
				buf.mu.Unlock()
				continue
			}
			records := buf.records
			buf.records = nil
			buf.mu.Unlock()

			for start := 0; start < len(records); start += batchSize {
				end := min(start+batchSize, len(records))
				if err := db.FlushBatch(ctx, records[start:end]); err != nil {
					log.Printf("flush_batch failed: %v", err)
				}
			}
		}
	}
}