// Consent Cache Projector — проекция policy.subscriber_consent в Runtime
// Redis (data_infrastructure_spec.md §1.9c), симметричен Config Cache
// Projector, но hot-path (Policy читает на каждое сообщение).
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

func envBool(key string, fallback bool) bool {
	if v := os.Getenv(key); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
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
	poolMaxConns := env("POSTGRES_POOL_MAX_CONNS", "8")
	if user == "" {
		return fmt.Sprintf("postgres://%s:%s/%s?pool_max_conns=%s", host, port, db, poolMaxConns)
	}
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?pool_max_conns=%s", user, password, host, port, db, poolMaxConns)
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

	// CODE_REVIEW.md finding #4 (self-disclosed in README as not
	// implemented): "Full Postgres resync on Redis loss not implemented".
	// consent:*_blacklist:* — единственный НЕ эфемерный ключ-класс в
	// Runtime Redis (data_infrastructure_spec.md §2.1: "Исключение —
	// consent:*"), поэтому при полной потере этой части Redis
	// восстановление из Kafka replay невозможно, а тихое ожидание
	// естественного трафика оставляет блэклист неполной на неопределённый
	// срок. Теперь при каждом старте (RESYNC_ON_START=true по умолчанию —
	// идемпотентно и безопасно на обычном рестарте, не только после
	// инцидента) сервис делает полный ресинк из policy.subscriber_consent
	// ДО того, как объявить себя ready — readiness probe не пустит трафик
	// на под, пока consent-данные не гарантированно консистентны с
	// PostgreSQL (source of truth, data_infrastructure_spec.md §1.9c).
	// Ошибка ресинка не фатальна (Postgres может быть временно недоступен
	// при обычном rolling restart) — логируется как CRITICAL, сервис
	// продолжает работу на текущем состоянии Redis (соответствует
	// поведению до этого фикса, но теперь явно, не молча).
	if envBool("RESYNC_ON_START", true) {
		pgCtx, pgCancel := context.WithTimeout(context.Background(), 5*time.Second)
		pool, err := pgxpool.New(pgCtx, buildPostgresDSN())
		pgCancel()
		if err != nil {
			log.Printf("CRITICAL: resync_from_postgres: не удалось создать пул подключений к PostgreSQL (%v) — Redis НЕ ресинхронизирован, продолжаем на текущем состоянии", err)
		} else {
			resyncCtx, resyncCancel := context.WithTimeout(context.Background(), 60*time.Second)
			written, deleted, err := redisClient.ResyncFromPostgres(resyncCtx, pool)
			resyncCancel()
			pool.Close()
			if err != nil {
				log.Printf("CRITICAL: resync_from_postgres failed (%v) — Redis НЕ гарантированно консистентен с PostgreSQL, продолжаем на текущем состоянии", err)
			} else {
				log.Printf("resync_from_postgres: keys_written=%d keys_deleted=%d", written, deleted)
			}
		}
	}

	brokers := strings.Split(env("KAFKA_BOOTSTRAP_SERVERS", "kafka-bootstrap.mpp.svc:9092"), ",")
	consumer, err := kafkaio.NewConsumer(brokers, "consent-cache-projector")
	if err != nil {
		log.Fatalf("не удалось создать Kafka consumer: %v", err)
	}
	defer consumer.Close()

	// CODE_REVIEW.md: "No sync.WaitGroup around the background goroutine
	// before Close() on shutdown" — SIGTERM раньше отменял контекст и
	// main() сразу возвращался в defer Close() Kafka/Redis, не дожидаясь,
	// пока Consumer.Run реально доработает начатый ApplyConsentChange и
	// выйдет из цикла — rolling deploy мог гонять "use of closed
	// connection" на in-flight работе. Теперь main() блокируется на
	// wg.Wait() перед Close().
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		consumer.Run(ctx,
			func(event *eventsv1.ConfigChangeEvent) error {
				if err := redisClient.ApplyConsentChange(ctx, event); err != nil {
					log.Printf("apply_consent_change failed for entity_id=%s: %v", event.GetEntityId(), err)
					return err
				}
				return nil
			},
			func(err error) {
				log.Printf("config.changes consume error: %v", err)
			},
		)
	}()

	// CODE_REVIEW.md: "/readyz never reflects real downstream health after
	// startup" — раньше это был статический флаг, выставленный один раз.
	// Теперь /readyz реально пингует Redis и Kafka с 2с таймаутом на
	// каждый запрос (см. internal/health).
	healthState.SetDependencyChecks(map[string]func(context.Context) error{
		"redis": redisClient.Ping,
		"kafka": consumer.Ping,
	})
	healthState.SetReady(true)
	log.Println("consent-cache-projector готов")

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	log.Println("остановка consent-cache-projector")
	cancel()
	wg.Wait()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = healthSrv.Shutdown(shutdownCtx)
}
