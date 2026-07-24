package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"mpp/dlr-correlation-writer/internal/health"
	"mpp/dlr-correlation-writer/internal/kafkaio"
	"mpp/dlr-correlation-writer/internal/writer"
)

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// buildDatabaseURL — реальная находка (см. services/dlr-manager/README.md
// "Реальная находка (систематическая...)" для полного разбора): k8s
// инжектит POSTGRES_HOST/PORT/DB/USER/PASSWORD дискретно (envFrom
// secretRef), не единую DATABASE_URL, которую этот сервис читал раньше —
// в реальном кластере он никогда бы не подключился. DATABASE_URL оставлен
// как явный override для локальной разработки/тестов.
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

	pgWriter, err := writer.NewPgWriter(ctx, buildDatabaseURL())
	if err != nil {
		log.Fatalf("writer.NewPgWriter: %v", err)
	}
	defer pgWriter.Close()

	brokers := strings.Split(getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-bootstrap.mpp.svc:9092"), ",")
	consumer, err := kafkaio.NewConsumer(brokers, "dlr-correlation-writer")
	if err != nil {
		log.Fatalf("kafkaio.NewConsumer: %v", err)
	}
	defer consumer.Close()

	batchSize := 500
	if v := os.Getenv("BATCH_MAX_SIZE"); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil && parsed > 0 {
			batchSize = parsed
		}
	}
	flushInterval := 2 * time.Second
	if v := os.Getenv("BATCH_FLUSH_INTERVAL_MS"); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil && parsed > 0 {
			flushInterval = time.Duration(parsed) * time.Millisecond
		}
	}

	healthState.SetReady(true)

	buf := writer.NewBatchBuffer(batchSize)
	latestByPartition := make(map[int32]*kgo.Record)
	lastFlush := time.Now()

	errHandler := func(err error) { log.Printf("dlr-correlation-writer: %v", err) }

	flush := func() {
		if buf.Len() == 0 {
			lastFlush = time.Now()
			return
		}
		// EnsurePartition — см. writer.PgWriter.EnsurePartition: без этого
		// вызова insert начинает падать в реальной эксплуатации, как только
		// текущий час выходит за пределы бутстрап-окна V015 (найдено живым
		// тестом против настоящего PostgreSQL, не гипотетически). Дешёвый
		// idempotent вызов (CREATE TABLE IF NOT EXISTS) — не жаль делать на
		// каждый flush, не только раз в час. И текущий, и следующий час —
		// буфер может пересечь границу часа между первой записью в него и
		// flush'ем. Не покрывает произвольно устаревший submitted_at
		// (backfill/replay) — не в этом срезе, см. README.
		now := time.Now()
		if err := pgWriter.EnsurePartition(ctx, now); err != nil {
			errHandler(err)
			lastFlush = time.Now()
			return
		}
		if err := pgWriter.EnsurePartition(ctx, now.Add(time.Hour)); err != nil {
			errHandler(err)
			lastFlush = time.Now()
			return
		}
		snapshot := buf.Snapshot()
		correlations := make([]writer.CorrelationRecord, len(snapshot))
		for i, rec := range snapshot {
			correlations[i] = rec.Correlation
		}
		if err := pgWriter.Flush(ctx, correlations); err != nil {
			// Не коммитим, не чистим буфер — та же запись переобработается
			// на следующем flush'е (at-least-once, тот же принцип "коммит
			// только после подтверждённой записи", что у всех остальных
			// Kafka-consumer'ов этой сессии).
			errHandler(err)
			lastFlush = time.Now()
			return
		}

		maxOffsets := writer.MaxOffsets(snapshot)
		toCommit := make([]*kgo.Record, 0, len(maxOffsets))
		for partition := range maxOffsets {
			if rec, ok := latestByPartition[partition]; ok {
				toCommit = append(toCommit, rec)
			}
		}
		if err := consumer.CommitRecords(ctx, toCommit...); err != nil {
			// Flush в PostgreSQL уже успешен (идемпотентно, ON CONFLICT DO
			// NOTHING) — повторная обработка того же батча при рестарте
			// безопасна, просто лишний no-op insert, не потеря/задвоение.
			errHandler(err)
		}
		buf.Clear()
		lastFlush = time.Now()
	}

	for {
		select {
		case <-ctx.Done():
			flush() // последний best-effort flush перед выходом
			_ = healthServer.Close()
			return
		default:
		}

		pollCtx, cancel := context.WithTimeout(ctx, flushInterval)
		consumer.PollOnce(pollCtx, func(rec writer.BufferedRecord, raw *kgo.Record) {
			latestByPartition[rec.Partition] = raw
			if shouldFlush := buf.Add(rec); shouldFlush {
				flush()
			}
		}, errHandler)
		cancel()

		if time.Since(lastFlush) >= flushInterval {
			flush()
		}
	}
}
