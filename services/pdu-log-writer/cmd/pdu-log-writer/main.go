// PDU Log Writer (BACKOFFICE_DESIGN_SPEC.md Экраны 38-40): operator.pdu.log
// -> ClickHouse batch insert. Отдельный от analytics-writer сервис — та же
// причина, что уже задокументирована в k8s/generate_manifests.py: схема
// per-PDU лога (operator_id/direction/pdu_type/sequence_number/
// smsc_message_id) не пересекается со stage_events (stage_name/outcome/
// lifecycle_status), а объём на порядок выше (по PDU, не по сообщению) —
// смешивать оба потока в одном буфере/batch means либо две независимые
// ветки batching-логики внутри одного сервиса, либо общий буфер с двумя
// разными формами строк. Структура и buffer/flush-патторн (drain-до-commit,
// restore-при-ошибке, backpressure через maxBufferedRecords) — 1:1 портированы
// из analytics-writer/cmd/analytics-writer/main.go, тот же CODE_REVIEW.md
// класс находок (commit офсетов только после подтверждённой записи).
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

	_ "github.com/KimMachineGun/automemlimit"

	"mpp/pdu-log-writer/internal/core"
	"mpp/pdu-log-writer/internal/health"
	"mpp/pdu-log-writer/internal/kafkaio"
	"mpp/pdu-log-writer/internal/store"
)

const maxBufferedRecords = 200_000

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// buffer — накопление между flush-тиками. records — связанные *kgo.Record,
// коммитятся только после подтверждённой записи (см. package doc).
type buffer struct {
	mu      sync.Mutex
	rows    []core.PduLogRecord
	records []*kgo.Record
}

func (b *buffer) len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.rows)
}

func (b *buffer) append(row core.PduLogRecord, rec *kgo.Record) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.rows = append(b.rows, row)
	b.records = append(b.records, rec)
}

func (b *buffer) drain() ([]core.PduLogRecord, []*kgo.Record) {
	b.mu.Lock()
	defer b.mu.Unlock()
	rows, records := b.rows, b.records
	b.rows, b.records = nil, nil
	return rows, records
}

func (b *buffer) restore(rows []core.PduLogRecord, records []*kgo.Record) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.rows = append(rows, b.rows...)
	b.records = append(records, b.records...)
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
		kgo.ConsumerGroup("pdu-log-writer"),
		kgo.ConsumeTopics(kafkaio.PduLogTopic),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		log.Fatalf("не удалось создать Kafka consumer: %v", err)
	}
	defer client.Close()

	buf := &buffer{}
	batchSize, _ := strconv.Atoi(env("BATCH_SIZE", "1000"))

	loopCtx, loopCancel := context.WithCancel(context.Background())
	defer loopCancel()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); runConsumeLoop(loopCtx, client, buf) }()
	go func() { defer wg.Done(); runFlushLoop(loopCtx, db, client, buf, batchSize) }()

	healthState.SetReady(true)
	log.Println("pdu-log-writer готов")

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	log.Println("остановка pdu-log-writer")
	loopCancel()
	wg.Wait()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	flush(shutdownCtx, db, client, buf, batchSize)
	_ = healthSrv.Shutdown(shutdownCtx)
}

func runConsumeLoop(ctx context.Context, client *kgo.Client, buf *buffer) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if buf.len() >= maxBufferedRecords {
			select {
			case <-ctx.Done():
				return
			case <-time.After(200 * time.Millisecond):
			}
			continue
		}

		fetches := client.PollFetches(ctx)
		if ctx.Err() != nil {
			return
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			log.Printf("fetch error (topic=%s partition=%d): %v", topic, partition, err)
		})
		fetches.EachRecord(func(rec *kgo.Record) {
			event, err := kafkaio.DecodePduLog(rec.Value)
			if err != nil {
				log.Printf("decode operator.pdu.log failed: %v", err)
				return
			}
			buf.append(core.FromOperatorPduLog(event), rec)
		})
	}
}

func runFlushLoop(ctx context.Context, db *store.Store, client *kgo.Client, buf *buffer, batchSize int) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			flush(ctx, db, client, buf, batchSize)
		}
	}
}

// flush — не очищает буфер до подтверждённой успешной записи: при ошибке
// весь снятый набор возвращается через restore() для повтора на следующем
// тике, офсеты не коммитятся (тот же CODE_REVIEW.md класс исправления,
// что analytics-writer).
func flush(ctx context.Context, db *store.Store, client *kgo.Client, buf *buffer, batchSize int) {
	rows, records := buf.drain()
	if len(rows) == 0 {
		return
	}

	for start := 0; start < len(rows); start += batchSize {
		end := min(start+batchSize, len(rows))
		if err := db.FlushBatch(ctx, rows[start:end]); err != nil {
			log.Printf("flush_batch failed, будет повторено: %v", err)
			buf.restore(rows[start:], records[start:])
			return
		}
	}

	if len(records) == 0 {
		return
	}
	if err := client.CommitRecords(ctx, records...); err != nil {
		log.Printf("commit offsets failed (данные уже записаны, будет передоставлено при рестарте до следующего успешного commit): %v", err)
	}
}
