// Lifecycle Writer (services_specifictaion.md §7.1): incoming.messages +
// message.lifecycle + stage.*.dlq -> PostgreSQL (read model, lifecycle
// history, DLQ record). stage.completed сознательно не читается.
//
// CODE_REVIEW.md HIGH findings — что изменилось в этом файле и почему:
//   - Раньше buf.history/buf.dlq очищались ПОД ЛОКОМ до подтверждения
//     успешной записи (flush читал+обнулял буфер, потом писал в Postgres)
//     — при сбое записи данные терялись безвозвратно, лог был единственным
//     следом. Плюс InsertReadModel/UpdateReadModel писались синхронно по
//     одной строке в consume-цикле — та же "запись без подтверждения"
//     проблема, только без буфера вообще.
//   - Плюс kgo.NewClient не передавал kgo.DisableAutoCommit() — franz-go
//     автокоммитил позицию раз в 5с НЕЗАВИСИМО от того, успела ли
//     запись в Postgres пройти. Транзиентный сбой Postgres на несколько
//     секунд означал permanent data loss для всего, что было
//     забуферизовано/обработано в этом окне — при рестарте consumer
//     продолжал бы с уже закоммиченной (за пределами реально записанных
//     данных) позиции.
//   - Плюс graceful shutdown не дренировал буфер — SIGTERM просто
//     отменял контекст, обычный rolling deploy терял всё, что было
//     накоплено с последнего тика.
//
// Исправлено единой моделью: consume-цикл только декодирует и
// буферизует (никаких синхронных записей в БД в hot path — заодно
// закрывает MEDIUM-находку про несовпадение с §6.1, который описывает
// батчинг всех таблиц вместе), commit смещений — ПОЛНОСТЬЮ ручной
// (kgo.DisableAutoCommit()) и происходит только ПОСЛЕ того, как flush
// подтвердил успешную запись всех накопленных данных. При сбое записи
// весь снятый с буфера набор (включая связанные *kgo.Record) возвращается
// обратно в буфер для повтора на следующем тике — ничего не теряется, а
// SQL идемпотентен (ON CONFLICT DO NOTHING для INSERT,
// lifecycle_version-guard для UPDATE), так что повторная попытка частично
// уже применённого набора безопасна. При остановке — WaitGroup вокруг
// обоих goroutine + финальный flush перед выходом.
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
	"sync"
	"syscall"
	"time"

	_ "github.com/KimMachineGun/automemlimit"
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
	poolMaxConns := env("POSTGRES_POOL_MAX_CONNS", "8")
	if user == "" {
		return fmt.Sprintf("postgres://%s:%s/%s?pool_max_conns=%s", host, port, db, poolMaxConns)
	}
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?pool_max_conns=%s", user, password, host, port, db, poolMaxConns)
}

// buffer — batch_buffer: накопление между flush_batch тиками. Все поля
// защищены mu; records несёт *kgo.Record для КАЖДОЙ порции данных выше
// (в т.ч. для строк, приведших только к synchronous-в-старом-смысле
// insert/update) — коммитится только после подтверждённой записи всей
// порции, см. flush().
type buffer struct {
	mu      sync.Mutex
	inserts []core.ReadModelRow
	updates []core.ReadModelUpdate
	history []core.LifecycleHistoryRow
	dlq     []core.DlqRow
	records []*kgo.Record
}

func (b *buffer) empty() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.inserts) == 0 && len(b.updates) == 0 && len(b.history) == 0 && len(b.dlq) == 0
}

// drain — атомарно забирает всё накопленное и обнуляет буфер (НЕ
// подтверждение успеха — вызывающий обязан restore() при сбое записи).
func (b *buffer) drain() (inserts []core.ReadModelRow, updates []core.ReadModelUpdate, history []core.LifecycleHistoryRow, dlq []core.DlqRow, records []*kgo.Record) {
	b.mu.Lock()
	defer b.mu.Unlock()
	inserts, updates, history, dlq, records = b.inserts, b.updates, b.history, b.dlq, b.records
	b.inserts, b.updates, b.history, b.dlq, b.records = nil, nil, nil, nil, nil
	return
}

// restore — возвращает снятую drain() порцию обратно в начало буфера
// (перед тем, что успело накопиться после drain) — используется, когда
// запись в Postgres провалилась, чтобы следующий тик повторил ровно то
// же самое (плюс всё новое, накопленное за это время).
func (b *buffer) restore(inserts []core.ReadModelRow, updates []core.ReadModelUpdate, history []core.LifecycleHistoryRow, dlq []core.DlqRow, records []*kgo.Record) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.inserts = append(inserts, b.inserts...)
	b.updates = append(updates, b.updates...)
	b.history = append(history, b.history...)
	b.dlq = append(dlq, b.dlq...)
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
		// CODE_REVIEW.md HIGH finding: коммит смещений теперь полностью
		// ручной (client.CommitRecords в flush()), только после
		// подтверждённой записи — не на 5-секундном таймере, независимо
		// от успеха.
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		log.Fatalf("не удалось создать Kafka consumer: %v", err)
	}
	defer client.Close()

	buf := &buffer{}

	loopCtx, loopCancel := context.WithCancel(context.Background())
	defer loopCancel()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); runConsumeLoop(loopCtx, client, buf) }()
	go func() { defer wg.Done(); runFlushLoop(loopCtx, db, client, buf) }()

	healthState.SetReady(true)
	log.Println("lifecycle-writer готов")

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	log.Println("остановка lifecycle-writer")
	loopCancel()
	wg.Wait()
	// Финальный drain — не даём последней порции буфера (накопленной
	// между последним тиком и остановкой) пропасть на обычном rolling
	// deploy.
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	flush(shutdownCtx, db, client, buf)
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
			switch rec.Topic {
			case "incoming.messages":
				msg, err := kafkaio.DecodeIncomingMessage(rec.Value)
				if err != nil {
					log.Printf("decode incoming.messages failed: %v", err)
					return
				}
				buf.mu.Lock()
				buf.inserts = append(buf.inserts, core.FromIncomingMessage(msg))
				buf.records = append(buf.records, rec)
				buf.mu.Unlock()
			case "message.lifecycle":
				event, err := kafkaio.DecodeLifecycleEvent(rec.Value)
				if err != nil {
					log.Printf("decode message.lifecycle failed: %v", err)
					return
				}
				update, historyRow := core.FromLifecycleEvent(event)
				buf.mu.Lock()
				buf.updates = append(buf.updates, update)
				buf.history = append(buf.history, historyRow)
				buf.records = append(buf.records, rec)
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
				buf.records = append(buf.records, rec)
				buf.mu.Unlock()
			}
		})
	}
}

func runFlushLoop(ctx context.Context, db *store.Store, client *kgo.Client, buf *buffer) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	// retentionInterval/retainHours — тот же принцип, что
	// dlr-correlation-writer/cmd/dlr-correlation-writer/main.go: DROP TABLE
	// не такой дешёвый, как CREATE IF NOT EXISTS, поэтому реже, чем на
	// каждый flush. 72ч по умолчанию — та же граница, что уже
	// задокументирована в migrations/V015 (не решено окончательно,
	// development_plan.md 5.6).
	retentionInterval := time.Hour
	if v := env("RETENTION_CHECK_INTERVAL", ""); v != "" {
		if parsed, err := time.ParseDuration(v); err == nil {
			retentionInterval = parsed
		}
	}
	retainHours := 72
	if v := env("LIFECYCLE_HISTORY_RETAIN_HOURS", ""); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil && parsed > 0 {
			retainHours = parsed
		}
	}
	retentionTicker := time.NewTicker(retentionInterval)
	defer retentionTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			flush(ctx, db, client, buf)
		case <-retentionTicker.C:
			dropped, err := db.DropOldPartitions(ctx, retainHours)
			if err != nil {
				log.Printf("drop_old_lifecycle_history_partitions failed: %v", err)
				continue
			}
			if dropped > 0 {
				log.Printf("drop_old_lifecycle_history_partitions: удалено %d устаревших партиций", dropped)
			}
		}
	}
}

// flush — flush_batch. Не очищает буфер до подтверждённой успешной
// записи (см. package doc): при любой ошибке весь снятый набор
// возвращается в буфер через restore() для повтора на следующем тике, и
// офсеты НЕ коммитятся — падение процесса до следующей успешной попытки
// безопасно передоставит эти же записи (SQL идемпотентен).
func flush(ctx context.Context, db *store.Store, client *kgo.Client, buf *buffer) {
	if buf.empty() {
		return
	}

	// EnsurePartition — см. store.Store.EnsurePartition: без этого вызова
	// BatchInsertLifecycleHistory рано или поздно начинает падать на
	// каждом tick'е, как только текущий час выходит за пределы
	// бутстрап-окна V015 — и поскольку commit офсетов ниже происходит
	// только после успеха ВСЕХ шагов flush, эта постоянная ошибка
	// блокирует продвижение consumer'а целиком, для всех трёх топиков, не
	// только для history. Дешёвый идемпотентный вызов — не жаль делать на
	// каждый tick, не только раз в час. И текущий, и следующий час —
	// буфер может пересечь границу часа между первой записью в него и
	// flush'ем.
	now := time.Now()

	inserts, updates, history, dlq, records := buf.drain()

	// Партиции создаются под ФАКТИЧЕСКИЕ occurred_at батча, а не только под
	// текущий час. Прежняя версия делала EnsurePartition(now) и
	// EnsurePartition(now+1h) ДО drain'а, то есть даже не смотрела, за какие
	// часы пришли записи. При replay после простоя occurred_at лежит в
	// прошлом, партиции нет, вставка падает с SQLSTATE 23514 — и так вечно,
	// блокируя коммит офсетов по всем трём топикам разом.
	// Замерено: 8 684 одинаковые ошибки подряд, 131% CPU на простое, lag
	// incoming.messages 69 946. См. store.PlanHistoryPartitions.
	plan, err := db.EnsurePartitionsForBatch(ctx, history, now)
	if err != nil {
		log.Printf("ensure_partitions_for_batch failed, flush отложен: %v", err)
		buf.restore(inserts, updates, history, dlq, records)
		return
	}
	if len(plan.Rejected) > 0 {
		// Вне окна партиций: партиция под них всё равно будет удалена
		// retention'ом. Повторять вечно значило бы создать poison pill того
		// же класса, который здесь и лечится, поэтому отбрасываем — но
		// ГРОМКО, потерю данных нельзя проводить молча.
		log.Printf("ВНИМАНИЕ: %d записей lifecycle_history вне окна партиций (older than %v / newer than %v) ОТБРОШЕНО",
			len(plan.Rejected), store.DefaultPartitionMaxPast, store.DefaultPartitionMaxFuture)
	}
	history = plan.Accepted

	if err := db.BatchInsertReadModel(ctx, inserts); err != nil {
		log.Printf("flush_batch (message_read_model insert) failed, будет повторено: %v", err)
		buf.restore(inserts, updates, history, dlq, records)
		return
	}
	missing, err := db.BatchUpdateReadModel(ctx, updates)
	if err != nil {
		log.Printf("flush_batch (message_read_model update) failed, будет повторено: %v", err)
		buf.restore(nil, updates, history, dlq, records)
		return
	}
	if len(missing) > 0 {
		// Реальная гонка incoming.messages/message.lifecycle (см. javadoc
		// store.Store.BatchUpdateReadModel) — строка read model для этих
		// message_id ещё не создана INSERT'ом. НЕ отбрасываем эти
		// обновления (иначе сообщение виснет на RECEIVED навсегда) —
		// кладём обратно в буфер, INSERT почти наверняка доедет к
		// следующему tick'у (секунда). Тот же принцип "коммитим офсеты
		// только когда всё применилось", что и у остальных веток ниже.
		log.Printf("flush_batch: %d update(s) опережают ещё не применённый insert (гонка incoming.messages/message.lifecycle), будет повторено", len(missing))
		buf.restore(nil, missing, history, dlq, records)
		return
	}
	if err := db.BatchInsertLifecycleHistory(ctx, history); err != nil {
		log.Printf("flush_batch (lifecycle_history) failed, будет повторено: %v", err)
		buf.restore(nil, nil, history, dlq, records)
		return
	}
	if err := db.BatchInsertDlq(ctx, dlq); err != nil {
		log.Printf("flush_batch (dlq_record) failed, будет повторено: %v", err)
		buf.restore(nil, nil, nil, dlq, records)
		return
	}

	if len(records) == 0 {
		return
	}
	if err := client.CommitRecords(ctx, records...); err != nil {
		// Запись в Postgres уже подтверждена и идемпотентна — при
		// перезапуске до успешного коммита эти же записи передоставятся
		// и безопасно no-op'нут (ON CONFLICT DO NOTHING /
		// lifecycle_version-guard), поэтому здесь НЕ восстанавливаем
		// буфер — иначе они попытались бы записаться в Postgres ещё раз
		// без необходимости на каждом следующем тике до тех пор, пока
		// commit не пройдёт.
		log.Printf("commit offsets failed (данные уже записаны, будет передоставлено при рестарте до следующего успешного commit): %v", err)
	}
}
