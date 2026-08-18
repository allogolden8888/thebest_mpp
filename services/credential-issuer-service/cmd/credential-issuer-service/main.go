// Credential Issuer Service — Фаза 1 плана закрытия API-пробелов: живой
// выпуск/ротация partner credentials (заменяет статическую deploy-time
// связку credential_ref -> env var). Первый сервис в этой сессии с
// реальным Vault-клиентом (internal/vault) — до этого Vault был только
// читаемой стороной (ExternalSecret), не писался ни одним приложением.
package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"

	"mpp/credential-issuer-service/internal/grpcserver"
	"mpp/credential-issuer-service/internal/health"
	"mpp/credential-issuer-service/internal/store"
	vaultpkg "mpp/credential-issuer-service/internal/vault"

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

// buildVaultTokenSource — VAULT_TOKEN (если задан) означает
// local/dev/break-glass: статический токен напрямую, без Kubernetes
// auth login (тот же escape hatch, что `vault` CLI сам поддерживает).
// Production-путь (VAULT_TOKEN не задан) — реальный Kubernetes auth login,
// роль "credential-issuer-service" (infra/terraform/vault-secrets.tf),
// JWT — стандартный projected ServiceAccount token path.
func buildVaultTokenSource(addr string, httpClient *http.Client) vaultpkg.TokenSource {
	if staticToken := os.Getenv("VAULT_TOKEN"); staticToken != "" {
		log.Println("VAULT_TOKEN задан — используется статический токен (local/dev/break-glass), не Kubernetes auth login")
		return vaultpkg.StaticTokenSource{StaticToken: staticToken}
	}
	return &vaultpkg.KubernetesAuthTokenSource{
		Addr:       addr,
		Role:       env("VAULT_K8S_AUTH_ROLE", "credential-issuer-service"),
		JWTPath:    env("VAULT_K8S_JWT_PATH", "/var/run/secrets/kubernetes.io/serviceaccount/token"),
		HTTPClient: httpClient,
	}
}

func main() {
	healthState := &health.State{}

	// Health-сервер стартует немедленно — readyz=503 до готовности, та же
	// конвенция, что у остальных Go-сервисов этой сессии.
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
	pg := store.NewPostgres(pool)

	vaultAddr := env("VAULT_ADDR", "http://vault.vault-system.svc:8200")
	vaultHTTPClient := &http.Client{Timeout: 10 * time.Second}
	vaultClient := vaultpkg.NewClient(vaultAddr, env("VAULT_MOUNT", "mpp"), buildVaultTokenSource(vaultAddr, vaultHTTPClient), vaultHTTPClient)

	grpcServer := grpc.NewServer()
	grpcv1.RegisterCredentialIssuerServiceServer(grpcServer, grpcserver.New(pg, vaultClient, nil))

	lis, err := net.Listen("tcp", ":9000")
	if err != nil {
		log.Fatalf("не удалось забиндить gRPC-порт 9000: %v", err)
	}
	go func() {
		log.Println("gRPC CredentialIssuerService слушает :9000")
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("gRPC server failed: %v", err)
		}
	}()

	healthState.SetDependencyChecks(map[string]func(context.Context) error{
		"postgres": pg.Ping,
		"vault":    vaultClient.Ping,
	})
	healthState.SetReady(true)
	log.Println("credential-issuer-service готов")

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	log.Println("остановка credential-issuer-service")
	grpcServer.GracefulStop()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = healthSrv.Shutdown(shutdownCtx)
}
