// Analytics Writer (services_specifictaion.md §7.2): incoming.messages +
// stage.completed + message.lifecycle -> ClickHouse batch insert +
// материализованные агрегаты.
//
// CODE_REVIEW.md HIGH findings — что изменилось в этом файле и почему
// (тот же класс бага и то же исправление, что в lifecycle-writer,
// сестринский Writer-сервис этой сессии):
//   - Раньше buf.records очищался ПОД ЛОКОМ до подтверждения успешной
//     записи (flush читал+обнулял буфер, потом писал в ClickHouse) — при
//     сбое записи данные терялись безвозвратно, лог был единственным
//     следом. Плюс kgo.NewClient не передавал kgo.DisableAutoCommit() —
//     franz-go автокоммитил позицию раз в 5с независимо от успеха записи.
//   - Плюс graceful shutdown не дренировал буфер.
// Исправлено той же моделью: consume-цикл только декодирует и
// буферизует, commit смещений — полностью ручной и происходит только
// ПОСЛЕ подтверждённого успеха FlushBatch; при сбое весь снятый набор
// (включая связанные *kgo.Record) возвращается в буфер для повтора —
// безопасно, т.к. ClickHouse-таблица теперь ReplacingMergeTree с
// event_id (см. internal/store/store.go) — повторная вставка уже
// вставленных строк не искажает агрегаты после merge.
//
// maxBufferedRecords — CODE_REVIEW.md Low finding: буфер, который
// никогда не отбрасывает данные при сбое (как теперь), растёт
// неограниченно при затяжном простое ClickHouse — OOM risk. Простой
// backpressure: consume-цикл приостанавливает PollFetches, пока буфер
// не опустится ниже порога, вместо того чтобы копить неограниченно;
// непрочитанные записи остаются в Kafka (не в памяти этого процесса).
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

	"mpp/analytics-writer/internal/core"
	"mpp/analytics-writer/internal/health"
	"mpp/analytics-writer/internal/kafkaio"
	"mpp/analytics-writer/internal/store"
)

const maxBufferedRecords = 100_000

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// buffer — batch_buffer: накопление между flush_batch тиками. records —
// связанные *kgo.Record, коммитятся только после подтверждённой записи
// (см. package doc).
type buffer struct {
	mu      sync.Mutex
	rows    []core.NormalizedRecord
	records []*kgo.Record
}

func (b *buffer) len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.rows)
}

func (b *buffer) append(row core.NormalizedRecord, rec *kgo.Record) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.rows = append(b.rows, row)
	b.records = append(b.records, rec)
}

func (b *buffer) drain() ([]core.NormalizedRecord, []*kgo.Record) {
	b.mu.Lock()
	defer b.mu.Unlock()
	rows, records := b.rows, b.records
	b.rows, b.records = nil, nil
	return rows, records
}

func (b *buffer) restore(rows []core.NormalizedRecord, records []*kgo.Record) {
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
		kgo.ConsumerGroup("analytics-writer"),
		kgo.ConsumeTopics("incoming.messages", kafkaio.StageCompletedTopic, "message.lifecycle"),
		// CODE_REVIEW.md HIGH finding: коммит смещений теперь полностью
		// ручной (client.CommitRecords в flush()), только после
		// подтверждённой записи в ClickHouse.
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		log.Fatalf("не удалось создать Kafka consumer: %v", err)
	}
	defer client.Close()

	buf := &buffer{}
	batchSize, _ := strconv.Atoi(env("BATCH_SIZE", "500"))

	loopCtx, loopCancel := context.WithCancel(context.Background())
	defer loopCancel()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); runConsumeLoop(loopCtx, client, buf) }()
	go func() { defer wg.Done(); runFlushLoop(loopCtx, db, client, buf, batchSize) }()

	healthState.SetReady(true)
	log.Println("analytics-writer готов")

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	log.Println("остановка analytics-writer")
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

		// Backpressure — см. package doc про maxBufferedRecords: не
		// затапливаем память, пока ClickHouse/flush не успевают.
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
				// rec.Timestamp — fallback на случай незаполненного
				// completed_at в payload (см. core.FromStageCompletedAt).
				normalized = core.FromStageCompletedAt(event, rec.Timestamp)
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

			buf.append(normalized, rec)
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

// flush — flush_batch. Не очищает буфер до подтверждённой успешной
// записи: при ошибке весь снятый набор возвращается в буфер через
// restore() для повтора на следующем тике, офсеты не коммитятся.
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
		// Данные уже записаны в ClickHouse и идемпотентны
		// (ReplacingMergeTree + event_id) — не восстанавливаем буфер,
		// иначе повторяли бы уже успешную запись на каждом тике до
		// следующего успешного commit.
		log.Printf("commit offsets failed (данные уже записаны, будет передоставлено при рестарте до следующего успешного commit): %v", err)
	}
}
