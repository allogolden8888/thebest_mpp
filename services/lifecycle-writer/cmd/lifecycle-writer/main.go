// Lifecycle Writer (services_specifictaion.md §7.1): incoming.messages +
// message.lifecycle + stage.*.dlq -> PostgreSQL (read model, lifecycle
// history, DLQ record). stage.completed сознательно не читается.
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"

	"mpp/lifecycle-writer/internal/core"
	"mpp/lifecycle-writer/internal/health"
	"mpp/lifecycle-writer/internal/kafkaio"
	"mpp/lifecycle-writer/internal/store"
)

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
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

// buffer — batch_buffer: накопление per-таблица между flush_batch тиками.
type buffer struct {
	mu        sync.Mutex
	history   []core.LifecycleHistoryRow
	dlq       []core.DlqRow
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
	db := store.New(pool)

	topics := append([]string{"incoming.messages", "message.lifecycle"}, kafkaio.DlqTopics...)
	brokers := strings.Split(env("KAFKA_BOOTSTRAP_SERVERS", "kafka-bootstrap.mpp.svc:9092"), ",")
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup("lifecycle-writer"),
		kgo.ConsumeTopics(topics...),
	)
	if err != nil {
		log.Fatalf("не удалось создать Kafka consumer: %v", err)
	}
	defer client.Close()

	buf := &buffer{}

	loopCtx, loopCancel := context.WithCancel(context.Background())
	defer loopCancel()

	go runConsumeLoop(loopCtx, client, pool, buf)
	go runFlushLoop(loopCtx, db, buf)

	healthState.SetReady(true)
	log.Println("lifecycle-writer готов")

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	log.Println("остановка lifecycle-writer")
	loopCancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = healthSrv.Shutdown(shutdownCtx)
}

func runConsumeLoop(ctx context.Context, client *kgo.Client, pool *pgxpool.Pool, buf *buffer) {
	db := store.New(pool)
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
			switch rec.Topic {
			case "incoming.messages":
				msg, err := kafkaio.DecodeIncomingMessage(rec.Value)
				if err != nil {
					log.Printf("decode incoming.messages failed: %v", err)
					return
				}
				if err := db.InsertReadModel(ctx, core.FromIncomingMessage(msg)); err != nil {
					log.Printf("InsertReadModel failed: %v", err)
				}
			case "message.lifecycle":
				event, err := kafkaio.DecodeLifecycleEvent(rec.Value)
				if err != nil {
					log.Printf("decode message.lifecycle failed: %v", err)
					return
				}
				update, historyRow := core.FromLifecycleEvent(event)
				if err := db.UpdateReadModel(ctx, update); err != nil {
					log.Printf("UpdateReadModel failed: %v", err)
				}
				buf.mu.Lock()
				buf.history = append(buf.history, historyRow)
				buf.mu.Unlock()
			default:
				rec2, err := kafkaio.DecodeDlqRecord(rec.Value)
				if err != nil {
					log.Printf("decode %s failed: %v", rec.Topic, err)
					return
				}
				dlqRow, err := core.FromDlqRecord(rec2)
				if err != nil {
					log.Printf("FromDlqRecord failed: %v", err)
					return
				}
				buf.mu.Lock()
				buf.dlq = append(buf.dlq, dlqRow)
				buf.mu.Unlock()
			}
		})
	}
}

func runFlushLoop(ctx context.Context, db *store.Store, buf *buffer) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			flush(ctx, db, buf)
		}
	}
}

func flush(ctx context.Context, db *store.Store, buf *buffer) {
	buf.mu.Lock()
	history := buf.history
	dlq := buf.dlq
	buf.history = nil
	buf.dlq = nil
	buf.mu.Unlock()

	if err := db.BatchInsertLifecycleHistory(ctx, history); err != nil {
		log.Printf("flush_batch (lifecycle_history) failed: %v", err)
	}
	if err := db.BatchInsertDlq(ctx, dlq); err != nil {
		log.Printf("flush_batch (dlq_record) failed: %v", err)
	}
}