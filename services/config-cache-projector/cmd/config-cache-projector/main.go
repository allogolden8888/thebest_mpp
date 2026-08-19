// Config Cache Projector — on_config_change -> write_projection в
// Configuration Redis (services_specifictaion.md §4.2 / §16 bootstrap-only
// паттерн).
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "github.com/KimMachineGun/automemlimit"

	"mpp/config-cache-projector/internal/health"
	"mpp/config-cache-projector/internal/kafkaio"
	"mpp/config-cache-projector/internal/projector"

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
		env("REDIS_CONFIGURATION_HOST", "localhost")+":"+env("REDIS_CONFIGURATION_PORT", "6379"),
		env("REDIS_CONFIGURATION_PASSWORD", ""),
	)
	defer redisClient.Close()

	brokers := strings.Split(env("KAFKA_BOOTSTRAP_SERVERS", "kafka-bootstrap.mpp.svc:9092"), ",")
	consumer, err := kafkaio.NewConsumer(brokers, "config-cache-projector")
	if err != nil {
		log.Fatalf("не удалось создать Kafka consumer: %v", err)
	}
	defer consumer.Close()

	// CODE_REVIEW.md: "No sync.WaitGroup around the background goroutine
	// before Close() on shutdown" — SIGTERM раньше отменял контекст и
	// main() сразу возвращался в defer Close() Kafka/Redis, не дожидаясь,
	// пока Consumer.Run реально доработает начатый WriteProjection и выйдет
	// из цикла — rolling deploy мог гонять "use of closed connection" на
	// in-flight работе. Теперь main() блокируется на wg.Wait() перед Close().
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		consumer.Run(ctx,
			func(event *eventsv1.ConfigChangeEvent) error {
				if err := redisClient.WriteProjection(ctx, event); err != nil {
					log.Printf("write_projection failed for entity_id=%s: %v", event.GetEntityId(), err)
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
	log.Println("config-cache-projector готов")

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	log.Println("остановка config-cache-projector")
	cancel()
	wg.Wait()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = healthSrv.Shutdown(shutdownCtx)
}
