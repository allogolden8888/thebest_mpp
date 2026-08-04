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
	"sync"
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
	maxAttempts := envInt("POLL_MAX_ATTEMPTS", outbox.DefaultMaxAttempts)

	// CODE_REVIEW.md: "No sync.WaitGroup around the background goroutine
	// before Close() on shutdown" — SIGTERM раньше отменял контекст и main()
	// сразу возвращался в defer'ы Close(), не дожидаясь, пока runPollLoop
	// реально доработает начатый Publish/MarkPublished и выйдет из цикла —
	// это могло гонять "use of closed connection" на in-flight работе при
	// rolling deploy. Теперь main() блокируется на wg.Wait() перед Close().
	loopCtx, loopCancel := context.WithCancel(context.Background())
	defer loopCancel()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		runPollLoop(loopCtx, store, publisher, pollInterval, batchSize, maxAttempts)
	}()

	// CODE_REVIEW.md: "/readyz never reflects real downstream health after
	// startup" — раньше это был статический флаг, выставленный один раз.
	// Теперь /readyz реально пингует Postgres и Kafka с 2с таймаутом на
	// каждый запрос (см. internal/health).
	healthState.SetDependencyChecks(map[string]func(context.Context) error{
		"postgres": pool.Ping,
		"kafka":    publisher.Ping,
	})
	healthState.SetReady(true)
	log.Println("config-event-publisher готов")

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	log.Println("остановка config-event-publisher")
	loopCancel()
	wg.Wait()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = healthSrv.Shutdown(shutdownCtx)
}

// runPollLoop — CODE_REVIEW.md findings #2/#3/#4 (poison-message
// head-of-line blocking with no bound/DLQ; no SELECT ... FOR UPDATE SKIP
// LOCKED for multi-replica safety; no backoff on poll-loop errors) —
// PollOutboxWithLimits теперь claim-ит строки атомарно (safe для >1
// реплики) и бракует строку после maxAttempts неудачных попыток вместо
// того, чтобы вечно вытеснять реальные pending-строки. store.PollOutbox
// errors (напр. Postgres недоступен) теперь ведут к экспоненциальному
// backoff вместо hammering каждые pollInterval.
func runPollLoop(ctx context.Context, store *outbox.Store, publisher *kafkaio.Publisher, interval time.Duration, batchSize, maxAttempts int) {
	const maxBackoff = 30 * time.Second

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	backoff := interval
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			entries, err := store.PollOutboxWithLimits(ctx, batchSize, maxAttempts, outbox.DefaultClaimTTL)
			if err != nil {
				log.Printf("poll_outbox failed: %v — backoff %s", err, backoff)
				select {
				case <-ctx.Done():
					return
				case <-time.After(backoff):
				}
				backoff *= 2
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
				continue
			}
			backoff = interval

			for _, e := range entries {
				if status, unresolved, resolveErr := kafkaio.ResolveStatus(e); resolveErr == nil && unresolved {
					log.Printf("WARNING: outbox id=%d entity_type=%s entity_id=%s: status не определяем из config_versions/payload_json, публикуем как %q — CODE_REVIEW.md: subscriber_consent revocation не может пропагироваться через этот пайплайн, пока configuration-service не начнёт сигнализировать archived (см. README)", e.ID, e.EntityType, e.EntityID, status)
				}

				if err := publisher.Publish(ctx, e); err != nil {
					exhausted, markErr := store.MarkPublishFailed(ctx, e.ID, err, maxAttempts)
					if markErr != nil {
						log.Printf("publish_config_change failed for outbox id=%d (%v), AND mark_publish_failed failed too: %v", e.ID, err, markErr)
						continue
					}
					if exhausted {
						log.Printf("CRITICAL: outbox id=%d entity_type=%s entity_id=%s исчерпал %d попыток публикации, последняя ошибка: %v — больше не будет выбираться poll_outbox, требуется ручное вмешательство", e.ID, e.EntityType, e.EntityID, maxAttempts, err)
					} else {
						log.Printf("publish_config_change failed for outbox id=%d: %v (будет повторено)", e.ID, err)
					}
					continue
				}
				if err := store.MarkPublished(ctx, e.ID); err != nil {
					log.Printf("mark_published failed for outbox id=%d: %v", e.ID, err)
				}
			}
		}
	}
}
