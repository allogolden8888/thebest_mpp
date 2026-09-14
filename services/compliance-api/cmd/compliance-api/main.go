// Compliance API (Фаза 6 плана, /Users/Alisher/.claude/plans/luminous-hugging-charm.md):
// point-lookup consent/blacklist-статуса по MSISDN (Runtime Redis, тот же
// формат ключей, что consent-cache-projector реально поддерживает живым) +
// ручные block/unblock-записи через configuration-service (Фаза 0 наличие
// iam-service обязательно — RequirePermission("compliance:write")).
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

	_ "github.com/KimMachineGun/automemlimit"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"

	grpcv1 "mpp/platformcontracts/grpc/v1"

	"mpp/compliance-api/internal/auth"
	"mpp/compliance-api/internal/health"
	"mpp/compliance-api/internal/httpapi"
	"mpp/compliance-api/internal/redisio"
	"mpp/compliance-api/internal/telemetry"
)

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
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

// dialGRPC — insecure.NewCredentials() намеренно, тот же случай, что
// backoffice-api/cmd/backoffice-api/main.go (Istio mTLS на уровне mesh,
// не приложения — см. комментарий там за полным обоснованием).
func dialGRPC(addr string) (*grpc.ClientConn, error) {
	return grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
}

func grpcConnCheck(conn *grpc.ClientConn) func(context.Context) error {
	return func(ctx context.Context) error {
		switch state := conn.GetState(); state {
		case connectivity.TransientFailure, connectivity.Shutdown:
			return fmt.Errorf("gRPC-соединение %s: %s", conn.Target(), state)
		default:
			return nil
		}
	}
}

type noopExporter struct{}

func (noopExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	return nil
}
func (noopExporter) Shutdown(ctx context.Context) error { return nil }

// buildSpanExporter — BACKOFFICE_ROADMAP.md P1 "Observability": раньше все
// спаны (internal/telemetry, реальный SDK) шли в noopExporter и сразу
// отбрасывались — ни один трейс никогда никуда не уходил. При заданном
// OTEL_EXPORTER_OTLP_ENDPOINT реально экспортирует их по OTLP/gRPC в
// otel-collector (infra/docker/docker-compose.yml). Без переменной (bare
// `go test`, окружения без коллектора) — прежний noop, старт не блокируется
// и не падает: otlptracegrpc.New не делает блокирующий dial, а ошибки
// самого экспорта BatchSpanProcessor только логирует.
func buildSpanExporter(ctx context.Context) sdktrace.SpanExporter {
	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if endpoint == "" {
		return noopExporter{}
	}
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "https://"), "http://")
	exp, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(endpoint),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		log.Printf("otel: не удалось создать OTLP exporter (%s), откат на noop: %v", endpoint, err)
		return noopExporter{}
	}
	return exp
}

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

	redisClient := redisio.NewClient(env("REDIS_RUNTIME_HOST", "localhost")+":"+env("REDIS_RUNTIME_PORT", "6379"), os.Getenv("REDIS_RUNTIME_PASSWORD"))
	defer redisClient.Close()

	configConn, err := dialGRPC(env("CONFIGURATION_SERVICE_ADDR", "configuration-service.mpp.svc:9000"))
	if err != nil {
		log.Fatalf("не удалось подключиться к Configuration Service: %v", err)
	}
	defer configConn.Close()

	iamConn, err := dialGRPC(env("IAM_SERVICE_ADDR", "iam-service.mpp.svc:9000"))
	if err != nil {
		log.Fatalf("не удалось подключиться к IAM Service: %v", err)
	}
	defer iamConn.Close()

	tp := telemetry.NewProvider(sdktrace.NewSimpleSpanProcessor(buildSpanExporter(context.Background())))
	defer func() { _ = telemetry.Shutdown(context.Background(), tp) }()

	router := httpapi.NewRouter(httpapi.Deps{
		Validator:      validator,
		Redis:          redisClient,
		ConfigClient:   grpcv1.NewConfigServiceClient(configConn),
		IamClient:      grpcv1.NewIamServiceClient(iamConn),
		TracerProvider: tp,
	})

	healthState.SetDependencyChecks(map[string]func(context.Context) error{
		"redis":                 redisClient.Ping,
		"configuration-service": grpcConnCheck(configConn),
		"iam-service":           grpcConnCheck(iamConn),
	})
	healthState.SetReady(true)

	srv := &http.Server{
		Addr:         ":8080",
		Handler:      router,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server failed: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
	_ = healthSrv.Shutdown(ctx)
}
