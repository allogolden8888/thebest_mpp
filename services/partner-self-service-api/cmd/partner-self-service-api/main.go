// Partner Self-Service API (Фаза 3 плана,
// /Users/Alisher/.claude/plans/luminous-hugging-charm.md): applications/
// senders read-modify-write, выпуск/ротация credentials, webhook-конфиг —
// всё ограничено partner_id из JWT-claim вызывающего (internal/auth/jwt.go
// зеркалит partner-api: RS256, обязательные aud/iss). IamService НЕ
// используется — см. doc-комментарий в internal/auth/jwt.go за полным
// обоснованием (нет RPC под iam.partner_portal_role_assignments).
//
// internal/httpapi/chat.go (BACKOFFICE_DESIGN_SPEC.md Экран 27 "Chat") —
// тонкий gRPC-прокси в chat-service, тот же класс зависимости, что
// ConfigService/CredentialIssuerService ниже — не прямое подключение к
// Postgres (см. services/chat-service/README.md за разбором, почему
// отдельный сервис).
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
	"syscall"
	"time"

	_ "github.com/KimMachineGun/automemlimit"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"

	grpcv1 "mpp/platformcontracts/grpc/v1"

	"mpp/partner-self-service-api/internal/auth"
	"mpp/partner-self-service-api/internal/health"
	"mpp/partner-self-service-api/internal/httpapi"
	"mpp/partner-self-service-api/internal/telemetry"
)

// loadJWTPrivateKey — BACKOFFICE_ROADMAP.md Production Readiness Review
// P0#5, internal/auth/issuer.go package doc: новый, отдельный от
// JWT_PUBLIC_KEY_PEM keypair — partner-self-service-api впервые сам
// ПОДПИСЫВАЕТ токены (POST /v1/self-service/auth/login), не только
// валидирует чужие. Тот же паттерн разбора PKCS1, что backoffice-api
// (cmd/backoffice-api/main.go).
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

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// loadJWTPublicKey — JWT_PUBLIC_KEY_PEM: PEM-encoded RSA public key
// (Keycloak realm public key, PKIX/SPKI формат) — тот же паттерн, что
// partner-api/compliance-api.
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
// backoffice-api/compliance-api (Istio mTLS на уровне mesh, не приложения).
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
	jwtAudience := env("PARTNER_SELF_SERVICE_API_JWT_AUDIENCE", "partner-self-service-api")
	jwtIssuer := env("PARTNER_SELF_SERVICE_API_JWT_ISSUER", "https://keycloak.mpp.svc/realms/mpp")
	validator := auth.NewValidator(pubKey, jwtAudience, jwtIssuer)

	// privKey/tokenIssuer — BACKOFFICE_ROADMAP.md Production Readiness
	// Review P0#5 (internal/auth/issuer.go package doc). Тот же
	// audience/issuer, что validator выше — Issue() проставляет их в каждый
	// выпущенный токен, иначе Validator.ParseBearer (jwt.go, требует
	// jwt.WithAudience/jwt.WithIssuer) отклонял бы токены, которые этот же
	// сервис только что сам выпустил.
	privKey, err := loadJWTPrivateKey()
	if err != nil {
		log.Fatalf("не удалось загрузить JWT private key: %v", err)
	}
	tokenIssuer := auth.NewTokenIssuer(privKey, jwtAudience, jwtIssuer)

	configConn, err := dialGRPC(env("CONFIGURATION_SERVICE_ADDR", "configuration-service.mpp.svc:9000"))
	if err != nil {
		log.Fatalf("не удалось подключиться к Configuration Service: %v", err)
	}
	defer configConn.Close()

	credentialConn, err := dialGRPC(env("CREDENTIAL_ISSUER_SERVICE_ADDR", "credential-issuer-service.mpp.svc:9000"))
	if err != nil {
		log.Fatalf("не удалось подключиться к Credential Issuer Service: %v", err)
	}
	defer credentialConn.Close()

	// chatConn — BACKOFFICE_DESIGN_SPEC.md Экран 27 "Chat"
	// (internal/httpapi/chat.go), проксируется в chat-service — тот же
	// класс зависимости, что configConn/credentialConn выше.
	chatConn, err := dialGRPC(env("CHAT_SERVICE_ADDR", "chat-service.mpp.svc:9000"))
	if err != nil {
		log.Fatalf("не удалось подключиться к Chat Service: %v", err)
	}
	defer chatConn.Close()

	// iamConn — BACKOFFICE_ROADMAP.md Production Readiness Review P0#5
	// (auth.go's handleLogin, internal/auth/resolve.go's ResolveLiveAccess).
	// Новая зависимость этого сервиса — раньше partner-self-service-api не
	// говорил с iam-service вообще (см. package doc jwt.go). Тот же
	// dialGRPC/grpcConnCheck паттерн, что остальные соединения выше.
	iamConn, err := dialGRPC(env("IAM_SERVICE_ADDR", "iam-service.mpp.svc:9000"))
	if err != nil {
		log.Fatalf("не удалось подключиться к IAM Service: %v", err)
	}
	defer iamConn.Close()

	tp := telemetry.NewProvider(sdktrace.NewBatchSpanProcessor(noopExporter{}))
	defer func() { _ = telemetry.Shutdown(context.Background(), tp) }()

	router := httpapi.NewRouter(httpapi.Deps{
		Validator:           validator,
		ConfigClient:        grpcv1.NewConfigServiceClient(configConn),
		CredentialClient:    grpcv1.NewCredentialIssuerServiceClient(credentialConn),
		ChatClient:          grpcv1.NewChatServiceClient(chatConn),
		TemplatesServiceURL: env("TEMPLATE_MANAGEMENT_SERVICE_URL", "http://template-management-service.mpp.svc:8080"),
		IamClient:           grpcv1.NewIamServiceClient(iamConn),
		TokenIssuer:         tokenIssuer,
		TracerProvider:      tp,
	})

	healthState.SetDependencyChecks(map[string]func(context.Context) error{
		"configuration-service":     grpcConnCheck(configConn),
		"credential-issuer-service": grpcConnCheck(credentialConn),
		"chat-service":              grpcConnCheck(chatConn),
		"iam-service":               grpcConnCheck(iamConn),
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
		log.Println("HTTP Partner Self-Service API слушает :8080")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server failed: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	log.Println("остановка partner-self-service-api")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
	_ = healthSrv.Shutdown(ctx)
}
