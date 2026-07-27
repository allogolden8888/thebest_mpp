// Backoffice API (services_specifictaion.md §8.3): административный CRUD,
// execution control, force scheduler command, DLQ/reconciliation browse,
// replay, отчётность — service_internal_methods.md §7.3 целиком.
package main

import (
	"context"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	grpcv1 "mpp/platformcontracts/grpc/v1"

	"mpp/backoffice-api/internal/auth"
	"mpp/backoffice-api/internal/health"
	"mpp/backoffice-api/internal/httpapi"
	"mpp/backoffice-api/internal/kafkaio"
	"mpp/backoffice-api/internal/store"
	"mpp/backoffice-api/internal/telemetry"
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

func loadJWTPublicKey() (*rsa.PublicKey, error) {
	pemData := os.Getenv("JWT_PUBLIC_KEY_PEM")
	if pemData == "" {
		return nil, fmt.Errorf("JWT_PUBLIC_KEY_PEM не задан")
	}
	block, _ := pem.Decode([]byte(pemData))
	if block == nil {
		return nil, fmt.Errorf("не удалось разобрать PEM из JWT_PUBLIC_KEY_PEM")
	}
	pub, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("x509.ParsePKIXPublicKey: %w", err)
	}
	rsaPub, ok := pub.(*rsa.PublicKey)
	if !ok {
		return nil, fmt.Errorf("JWT_PUBLIC_KEY_PEM не является RSA-ключом")
	}
	return rsaPub, nil
}

func dialGRPC(addr string) (*grpc.ClientConn, error) {
	return grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
}

type noopExporter struct{}

func (noopExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error { return nil }
func (noopExporter) Shutdown(ctx context.Context) error                                   { return nil }

func main() {
	healthState := &health.State{}
	healthSrv := &http.Server{Addr: ":9090", Handler: health.Router(healthState)}
	go func() {
		if err := healthSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("health server failed: %v", err)
		}
	}()

	pubKey, err := loadJWTPublicKey()
	if err != nil {
		log.Fatalf("не удалось загрузить JWT public key: %v", err)
	}
	validator := auth.NewValidator(pubKey)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	pool, err := pgxpool.New(ctx, buildPostgresDSN())
	cancel()
	if err != nil {
		log.Fatalf("не удалось создать пул подключений к PostgreSQL: %v", err)
	}
	defer pool.Close()
	pg := store.NewPostgres(pool)

	chAddr := env("CLICKHOUSE_HOST", "localhost") + ":" + env("CLICKHOUSE_PORT", "9000")
	ch, err := store.NewClickHouse(chAddr, env("CLICKHOUSE_DB", "default"), env("CLICKHOUSE_USER", "default"), env("CLICKHOUSE_PASSWORD", ""))
	if err != nil {
		log.Fatalf("не удалось создать ClickHouse клиент: %v", err)
	}

	brokers := strings.Split(env("KAFKA_BOOTSTRAP_SERVERS", "kafka-bootstrap.mpp.svc:9092"), ",")
	publisher, err := kafkaio.NewPublisher(brokers)
	if err != nil {
		log.Fatalf("не удалось создать Kafka producer: %v", err)
	}
	defer publisher.Close()

	configConn, err := dialGRPC(env("CONFIGURATION_SERVICE_ADDR", "configuration-service.mpp.svc:9000"))
	if err != nil {
		log.Fatalf("не удалось подключиться к Configuration Service: %v", err)
	}
	defer configConn.Close()

	execControlConn, err := dialGRPC(env("EXECUTION_CONTROL_SERVICE_ADDR", "execution-control-service.mpp.svc:9000"))
	if err != nil {
		log.Fatalf("не удалось подключиться к Execution Control Service: %v", err)
	}
	defer execControlConn.Close()

	replayConn, err := dialGRPC(env("REPLAY_SERVICE_ADDR", "replay-service.mpp.svc:9000"))
	if err != nil {
		log.Fatalf("не удалось подключиться к Replay Service: %v", err)
	}
	defer replayConn.Close()

	tp := telemetry.NewProvider(sdktrace.NewBatchSpanProcessor(noopExporter{}))
	defer func() { _ = telemetry.Shutdown(context.Background(), tp) }()

	router := httpapi.NewRouter(httpapi.Deps{
		Validator:        validator,
		Postgres:         pg,
		ClickHouse:       ch,
		Publisher:        publisher,
		ConfigClient:     grpcv1.NewConfigServiceClient(configConn),
		ExecutionControl: grpcv1.NewExecutionControlServiceClient(execControlConn),
		Replay:           grpcv1.NewReplayServiceClient(replayConn),
		TracerProvider:   tp,
	})

	httpSrv := &http.Server{Addr: ":8080", Handler: router}
	go func() {
		log.Println("HTTP Backoffice API слушает :8080")
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http server failed: %v", err)
		}
	}()

	healthState.SetReady(true)
	log.Println("backoffice-api готов")

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	log.Println("остановка backoffice-api")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = httpSrv.Shutdown(shutdownCtx)
}
