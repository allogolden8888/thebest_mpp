// Replay Service — load_dlq_record -> check_ttl -> check_idempotency ->
// check_billing_side_effect -> check_delivery_ambiguity -> republish ->
// write_audit (service_internal_methods.md §7.4, services_specifictaion.md
// §4.9). Вызывается Backoffice API (handle_replay_request), см.
// platform-contracts/grpc/internal_control.proto ReplayService.RequestReplay.
package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"syscall"
	"os/signal"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"

	"mpp/replay-service/internal/controlsnapshot"
	"mpp/replay-service/internal/grpcserver"
	"mpp/replay-service/internal/health"
	"mpp/replay-service/internal/kafkaio"
	"mpp/replay-service/internal/store"

	commonv1 "mpp/platformcontracts/common/v1"
	eventsv1 "mpp/platformcontracts/events/v1"
	grpcv1 "mpp/platformcontracts/grpc/v1"
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
	st := store.New(pool)

	brokers := strings.Split(env("KAFKA_BOOTSTRAP_SERVERS", "kafka-bootstrap.mpp.svc:9092"), ",")
	publisher, err := kafkaio.NewPublisher(brokers)
	if err != nil {
		log.Fatalf("не удалось создать Kafka producer: %v", err)
	}
	defer publisher.Close()

	// check_execution_control (CODE_REVIEW.md HIGH finding #4) —
	// снапшот execution.control как локальный full-mirror (см. package doc
	// в internal/kafkaio/controlconsumer.go). Пустой снапшот на старте не
	// блокирует ни один replay (fail-open до первого прогона консьюмера,
	// как и у остальных потребителей execution.control этой сессии) — не
	// делает readiness зависимым от прогрева снапшота, потому что
	// RequestReplay — редкая, ручная операция, а не постоянный hot-path
	// цикл, где узкое окно fail-open на старте пода несёт тот же риск.
	controlConsumer, err := kafkaio.NewControlConsumer(brokers)
	if err != nil {
		log.Fatalf("не удалось создать Kafka consumer для execution.control: %v", err)
	}
	defer controlConsumer.Close()
	snapshot := controlsnapshot.New()
	controlCtx, controlCancel := context.WithCancel(context.Background())
	defer controlCancel()
	go controlConsumer.Run(controlCtx, func(rec *eventsv1.ExecutionControlRecord, scope commonv1.ExecutionControlScope, scopeID string, tombstone bool) {
		if tombstone {
			snapshot.Delete(scope, scopeID)
			return
		}
		snapshot.Apply(scope, scopeID, rec.GetState())
	}, func(err error) {
		log.Printf("execution.control consume error: %v", err)
	})

	srv := grpcserver.New(st, st, st, st, snapshot, publisher)

	grpcServer := grpc.NewServer()
	grpcv1.RegisterReplayServiceServer(grpcServer, srv)

	lis, err := net.Listen("tcp", ":9000")
	if err != nil {
		log.Fatalf("не удалось забиндить gRPC-порт 9000: %v", err)
	}
	go func() {
		log.Println("gRPC ReplayService слушает :9000")
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("gRPC server failed: %v", err)
		}
	}()

	healthState.SetReady(true)
	log.Println("replay-service готов")

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	log.Println("остановка replay-service")
	grpcServer.GracefulStop()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = healthSrv.Shutdown(shutdownCtx)
}