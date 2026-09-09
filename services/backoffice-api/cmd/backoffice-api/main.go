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

	_ "github.com/KimMachineGun/automemlimit"
	"github.com/jackc/pgx/v5/pgxpool"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
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
	poolMaxConns := env("POSTGRES_POOL_MAX_CONNS", "8")
	if user == "" {
		return fmt.Sprintf("postgres://%s:%s/%s?pool_max_conns=%s", host, port, db, poolMaxConns)
	}
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?pool_max_conns=%s", user, password, host, port, db, poolMaxConns)
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

// loadJWTPrivateKey — luminous-hugging-charm.md, BACKOFFICE_DESIGN_SPEC.md
// Экран 33 "Admin users". Новый, отдельный от JWT_PUBLIC_KEY_PEM keypair
// (см. internal/auth/issuer.go package doc) — backoffice-api впервые сам
// ПОДПИСЫВАЕТ токены (POST /v1/auth/login), не только валидирует чужие.
func loadJWTPrivateKey() (*rsa.PrivateKey, error) {
	pemData := os.Getenv("JWT_PRIVATE_KEY_PEM")
	if pemData == "" {
		return nil, fmt.Errorf("JWT_PRIVATE_KEY_PEM не задан")
	}
	block, _ := pem.Decode([]byte(pemData))
	if block == nil {
		return nil, fmt.Errorf("не удалось разобрать PEM из JWT_PRIVATE_KEY_PEM")
	}
	key, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("x509.ParsePKCS1PrivateKey: %w", err)
	}
	return key, nil
}

// dialGRPC — insecure.NewCredentials() здесь НАМЕРЕННО, не пропущенный mTLS
// (CODE_REVIEW.md отметило это как HIGH — расследовано, тот же false
// positive, что уже разобран в services/partner-notification-service/internal/notify/grpc_client.go:47-62,
// см. этот файл за полным обоснованием). Все три адреса ниже — ConfigurationService/
// ExecutionControlService/ReplayService — резолвятся как `*.mpp.svc:9000`,
// то есть in-namespace: namespace `mpp` целиком помечен
// `istio-injection: enabled` (k8s/generate_manifests.py), и
// `infra/istio/peer-authentication-strict.yaml` держит режим STRICT — Envoy
// sidecar каждого пода прозрачно поднимает mTLS между собой, приложение
// видит только localhost-плейнтекст до своего sidecar. Добавление TLS
// здесь поверх mesh было бы double-mTLS без документированного источника
// certs/CA на уровне приложения.
// grpcConnCheck — /readyz dependency check для gRPC-соединения: считает
// зависимость недоступной только в определённо-нерабочих состояниях
// (TRANSIENT_FAILURE/SHUTDOWN); IDLE/CONNECTING не блокируют readiness —
// gRPC ленивое соединение может легитимно простаивать в IDLE между
// вызовами. Connect(false) лишь читает текущее состояние, не форсирует
// новую попытку подключения на каждый health-check тик.
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

// httpPingCheck — /readyz dependency check для plain-HTTP зависимости
// (ops-visibility-service, единственная такая в этом сервисе — см.
// internal/httpapi/ops.go package doc, почему она не gRPC). В отличие от
// grpcConnCheck (читает уже известное состояние соединения) — здесь
// реальный GET на каждый readyz-тик, у plain http.Client нет аналога
// gRPC-шного "текущее состояние канала" без сетевого запроса.
func httpPingCheck(client *http.Client, url string) func(context.Context) error {
	return func(ctx context.Context) error {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return err
		}
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("ops-visibility-service healthz: статус %d", resp.StatusCode)
		}
		return nil
	}
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

	privKey, err := loadJWTPrivateKey()
	if err != nil {
		log.Fatalf("не удалось загрузить JWT private key: %v", err)
	}
	tokenIssuer := auth.NewTokenIssuer(privKey)

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

	// iamConn — luminous-hugging-charm.md Фаза 0: auth.RequirePermission
	// (internal/auth/permission.go) вызывает IamService.CheckPermission на
	// каждый мутирующий запрос вместо разбора realm_access.roles на месте
	// (см. package doc internal/auth/jwt.go). Тот же dialGRPC/grpcConnCheck
	// паттерн, что и у трёх соединений выше.
	iamConn, err := dialGRPC(env("IAM_SERVICE_ADDR", "iam-service.mpp.svc:9000"))
	if err != nil {
		log.Fatalf("не удалось подключиться к IAM Service: %v", err)
	}
	defer iamConn.Close()

	// credentialIssuerConn — luminous-hugging-charm.md Ф1: "Rotate
	// credential" (backoffice-ui, PARTNER config экран) проксируется в
	// новый credential-issuer-service (см. internal/httpapi/credentials.go
	// package doc). Тот же dialGRPC/grpcConnCheck паттерн, что и у
	// остальных четырёх соединений выше.
	credentialIssuerConn, err := dialGRPC(env("CREDENTIAL_ISSUER_SERVICE_ADDR", "credential-issuer-service.mpp.svc:9000"))
	if err != nil {
		log.Fatalf("не удалось подключиться к Credential Issuer Service: %v", err)
	}
	defer credentialIssuerConn.Close()

	// incidentConn — luminous-hugging-charm.md Ф7: backoffice-ui "Incidents"
	// проксируется в новый incident-service (internal/httpapi/incidents.go
	// package doc). Тот же dialGRPC/grpcConnCheck паттерн, что и у
	// остальных соединений выше.
	incidentConn, err := dialGRPC(env("INCIDENT_SERVICE_ADDR", "incident-service.mpp.svc:9000"))
	if err != nil {
		log.Fatalf("не удалось подключиться к Incident Service: %v", err)
	}
	defer incidentConn.Close()

	// opsVisibilityURL/opsHTTPClient — luminous-hugging-charm.md Ф8.
	// ops-visibility-service не поднимает gRPC вообще (см.
	// internal/httpapi/ops.go package doc) — плоский http.Client, не
	// сгенерированный stub, как у остальных зависимостей выше.
	opsVisibilityURL := "http://" + env("OPS_VISIBILITY_SERVICE_ADDR", "ops-visibility-service.mpp.svc:9090")
	opsHTTPClient := &http.Client{Timeout: 10 * time.Second}

	// complianceAPIURL/complianceHealthURL — BACKOFFICE_ROADMAP.md §4
	// "Blacklist" (internal/httpapi/compliance.go). compliance-api тоже не
	// gRPC — тот же класс зависимости, что ops-visibility-service выше, но
	// с раздельными business/health портами (compliance-api/cmd/main.go:
	// :8080 business, :9090 health — в отличие от ops-visibility-service, у
	// которого оба на одном порту), поэтому два отдельных env var.
	complianceAPIURL := "http://" + env("COMPLIANCE_API_ADDR", "compliance-api.mpp.svc:8080")
	complianceHealthURL := "http://" + env("COMPLIANCE_API_HEALTH_ADDR", "compliance-api.mpp.svc:9090")

	// redisRuntime — BACKOFFICE_ROADMAP.md §2 "Операторы" (internal/httpapi/
	// operators.go, internal/store/redis.go). Единственное прямое Redis-
	// подключение backoffice-api — тот же env var naming, что уже
	// используют operator-smpp-session-manager/compliance-api для того же
	// Runtime Redis instance.
	redisRuntimeAddr := env("REDIS_RUNTIME_HOST", "localhost") + ":" + env("REDIS_RUNTIME_PORT", "6379")
	redisRuntime := store.NewRedis(redisRuntimeAddr, env("REDIS_RUNTIME_PASSWORD", ""))
	defer redisRuntime.Close()

	tp := telemetry.NewProvider(sdktrace.NewBatchSpanProcessor(noopExporter{}))
	defer func() { _ = telemetry.Shutdown(context.Background(), tp) }()

	router := httpapi.NewRouter(httpapi.Deps{
		Validator:              validator,
		Postgres:               pg,
		ClickHouse:             ch,
		Publisher:              publisher,
		ConfigClient:           grpcv1.NewConfigServiceClient(configConn),
		ExecutionControl:       grpcv1.NewExecutionControlServiceClient(execControlConn),
		Replay:                 grpcv1.NewReplayServiceClient(replayConn),
		IamClient:              grpcv1.NewIamServiceClient(iamConn),
		CredentialIssuerClient: grpcv1.NewCredentialIssuerServiceClient(credentialIssuerConn),
		IncidentClient:         grpcv1.NewIncidentServiceClient(incidentConn),
		HTTPClient:             opsHTTPClient,
		OpsVisibilityURL:       opsVisibilityURL,
		ComplianceAPIURL:       complianceAPIURL,
		RedisRuntime:           redisRuntime,
		TokenIssuer:            tokenIssuer,
		TracerProvider:         tp,
	})

	// CODE_REVIEW.md Low finding: без ReadTimeout/WriteTimeout/IdleTimeout
	// сервер уязвим к slow-client (Slowloris-класс) исчерпанию соединений.
	httpSrv := &http.Server{
		Addr:         ":8080",
		Handler:      router,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}
	go func() {
		log.Println("HTTP Backoffice API слушает :8080")
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http server failed: %v", err)
		}
	}()

	healthState.SetDependencyChecks(map[string]func(context.Context) error{
		"postgres":                  pg.Ping,
		"clickhouse":                ch.Ping,
		"kafka":                     publisher.Ping,
		"configuration-service":     grpcConnCheck(configConn),
		"execution-control-service": grpcConnCheck(execControlConn),
		"replay-service":            grpcConnCheck(replayConn),
		"iam-service":               grpcConnCheck(iamConn),
		"credential-issuer-service": grpcConnCheck(credentialIssuerConn),
		"incident-service":          grpcConnCheck(incidentConn),
		"ops-visibility-service":    httpPingCheck(opsHTTPClient, opsVisibilityURL+"/healthz"),
		"compliance-api":            httpPingCheck(opsHTTPClient, complianceHealthURL+"/healthz"),
		"redis-runtime":             redisRuntime.Ping,
	})
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
