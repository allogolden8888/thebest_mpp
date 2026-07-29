// Partner API (services_specifictaion.md §8.2): статус сообщения, поиск,
// отчёты — service_internal_methods.md §7.2 (handle_status_query,
// handle_search_query, handle_report_query). HTTP-only, без собственного
// gRPC-сервера (не проксирует ни в один внутренний сервис, в отличие от
// Backoffice API).
package main

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"mpp/partner-api/internal/auth"
	"mpp/partner-api/internal/health"
	"mpp/partner-api/internal/httpapi"
	"mpp/partner-api/internal/store"
	"mpp/partner-api/internal/telemetry"

	"crypto/rsa"

	sdktracepkg "go.opentelemetry.io/otel/sdk/trace"
)

// noopExporter — SpanExporter-заглушка: спаны реально создаются и
// завершаются (internal/telemetry.Middleware), но никуда не отправляются —
// в этой сессии нет ни одного развёрнутого OTLP-коллектора для экспорта.
type noopExporter struct{}

func (noopExporter) ExportSpans(ctx context.Context, spans []sdktracepkg.ReadOnlySpan) error {
	return nil
}

func (noopExporter) Shutdown(ctx context.Context) error {
	return nil
}

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

// loadJWTPublicKey — JWT_PUBLIC_KEY_PEM: PEM-encoded RSA public key
// (Keycloak realm public key, PKIX/SPKI формат). Обязателен — без него
// сервис не может проверить ни один токен.
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
	validator := auth.NewValidator(pubKey, env("PARTNER_API_JWT_AUDIENCE", "partner-api"), env("PARTNER_API_JWT_ISSUER", "https://keycloak.mpp.svc/realms/mpp"))

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

	// В проде SpanProcessor должен экспортировать в OTLP-коллектор
	// (не специфицирован ни в одном документе этой сессии) — здесь
	// используется no-op batcher без реального exporter'а, span'ы создаются
	// и завершаются реально (см. internal/telemetry), но никуда не
	// отправляются за пределы процесса.
	tp := telemetry.NewProvider(sdktrace.NewBatchSpanProcessor(noopExporter{}))
	defer func() { _ = telemetry.Shutdown(context.Background(), tp) }()

	router := httpapi.NewRouter(validator, pg, ch, tp)

	// CODE_REVIEW.md Medium finding: без ReadTimeout/WriteTimeout/IdleTimeout
	// externally-reachable сервер уязвим к slow-client исчерпанию соединений
	// — более прямо эксплуатируемо здесь, чем на внутренних сервисах.
	httpSrv := &http.Server{
		Addr:         ":8080",
		Handler:      router,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}
	go func() {
		log.Println("HTTP Partner API слушает :8080")
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http server failed: %v", err)
		}
	}()

	healthState.SetDependencyChecks(map[string]func(context.Context) error{
		"postgres":   pg.Ping,
		"clickhouse": ch.Ping,
	})
	healthState.SetReady(true)
	log.Println("partner-api готов")

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	log.Println("остановка partner-api")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = httpSrv.Shutdown(shutdownCtx)
}
