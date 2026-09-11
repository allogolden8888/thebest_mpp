// Operator HTTP Gateway (services_specifictaion.md §2.3a / k8s external_port
// "webhook" 8080): register_route, enforce_tps, submit_sm через HTTP,
// приём DLR webhook.
package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	_ "github.com/KimMachineGun/automemlimit"
	"google.golang.org/grpc"

	"mpp/operator-http-gateway/internal/core"
	"mpp/operator-http-gateway/internal/grpcserver"
	"mpp/operator-http-gateway/internal/health"
	"mpp/operator-http-gateway/internal/httpio"
	"mpp/operator-http-gateway/internal/kafkaio"
	"mpp/operator-http-gateway/internal/opconfig"
	"mpp/operator-http-gateway/internal/registry"
	vaultpkg "mpp/operator-http-gateway/internal/vault"
	"mpp/operator-http-gateway/internal/webhook"
	"mpp/operator-http-gateway/internal/webhookauth"

	grpcv1 "mpp/platformcontracts/grpc/v1"
)

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// buildVaultTokenSource — byte-for-byte mirror of
// credential-issuer-service/cmd/credential-issuer-service/main.go's
// function of the same name: VAULT_TOKEN set means local/dev/break-glass
// (static token, no Kubernetes auth login — the same escape hatch `vault`
// CLI itself supports). Production path (VAULT_TOKEN unset) — real
// Kubernetes auth login, role "operator-webhook-credential-readers"
// (infra/terraform/vault-secrets.tf), standard projected ServiceAccount
// JWT path.
func buildVaultTokenSource(addr string, httpClient *http.Client) vaultpkg.TokenSource {
	if staticToken := os.Getenv("VAULT_TOKEN"); staticToken != "" {
		log.Println("VAULT_TOKEN задан — используется статический токен (local/dev/break-glass), не Kubernetes auth login")
		return vaultpkg.StaticTokenSource{StaticToken: staticToken}
	}
	return &vaultpkg.KubernetesAuthTokenSource{
		Addr:       addr,
		Role:       env("VAULT_K8S_AUTH_ROLE", "operator-webhook-credential-readers"),
		JWTPath:    env("VAULT_K8S_JWT_PATH", "/var/run/secrets/kubernetes.io/serviceaccount/token"),
		HTTPClient: httpClient,
	}
}

// redisEndpointResolver — grpcserver.EndpointResolver поверх registry.Client.
type redisEndpointResolver struct {
	registry *registry.Client
}

func (r *redisEndpointResolver) ResolveEndpoint(operatorID, routeID string) (string, error) {
	fields, err := r.registry.Lookup(context.Background(), operatorID, routeID)
	if err != nil {
		return "", err
	}
	endpoint, ok := fields["endpoint"]
	if !ok || endpoint == "" {
		return "", fmt.Errorf("route %s:%s не найден в registry", operatorID, routeID)
	}
	return endpoint, nil
}

func main() {
	healthState := &health.State{}
	healthSrv := &http.Server{Addr: ":9090", Handler: health.Router(healthState)}
	go func() {
		if err := healthSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("health server failed: %v", err)
		}
	}()

	operatorID := env("OPERATOR_ID", "beeline_uz")
	routeID := env("ROUTE_ID", "route-1")

	redisClient := registry.NewClient(
		env("REDIS_RUNTIME_HOST", "localhost")+":"+env("REDIS_RUNTIME_PORT", "6379"),
		env("REDIS_RUNTIME_PASSWORD", ""),
		env("HOSTNAME", "operator-http-gateway-0"),
		30*time.Second,
	)
	defer redisClient.Close()

	endpointURL := env("OPERATOR_SUBMIT_ENDPOINT", "https://operator.example/submit")
	if err := redisClient.Register(context.Background(), operatorID, routeID, time.Now().UnixNano(), endpointURL); err != nil {
		log.Printf("register_route failed: %v", err)
	}

	// opconfig.Reader — Configuration Redis (config-cache-projector's
	// entity_type=operator projection), тот же bootstrap/cache-miss
	// паттерн, что уже использует billing-service (TariffCache). Отдельный
	// Redis-клиент от redisClient выше (Runtime Redis, register_route/
	// heartbeat) — разные Redis-инстансы (data_infrastructure_spec.md §2.1
	// vs §2.2), уже отдельная secret-запись в SECRET_DEPENDENCIES.
	opConfigReader := opconfig.NewReader(
		env("REDIS_CONFIGURATION_HOST", "localhost")+":"+env("REDIS_CONFIGURATION_PORT", "6379"),
		env("REDIS_CONFIGURATION_PASSWORD", ""),
	)
	defer opConfigReader.Close()

	// Vault — BACKOFFICE_ROADMAP.md P0#1: заменяет прежний общий
	// WEBHOOK_AUTH_TOKEN (один секрет на ВСЕХ операторов сразу).
	// buildVaultTokenSource выбирает между VAULT_TOKEN (local/dev/
	// break-glass) и реальным Kubernetes auth login — byte-for-byte та же
	// схема, что credential-issuer-service.
	vaultAddr := env("VAULT_ADDR", "http://vault.vault-system.svc:8200")
	vaultHTTPClient := &http.Client{Timeout: 10 * time.Second}
	vaultClient := vaultpkg.NewClient(vaultAddr, env("VAULT_MOUNT", "mpp"), buildVaultTokenSource(vaultAddr, vaultHTTPClient), vaultHTTPClient)

	webhookAuthLookup := webhookauth.New(opConfigReader, vaultClient)

	brokers := strings.Split(env("KAFKA_BOOTSTRAP_SERVERS", "kafka-bootstrap.mpp.svc:9092"), ",")
	publisher, err := kafkaio.NewPublisher(brokers)
	if err != nil {
		log.Fatalf("не удалось создать Kafka producer: %v", err)
	}
	defer publisher.Close()

	tpsLimit, _ := strconv.ParseFloat(env("TPS_LIMIT", "200"), 64)
	tpsBucket := core.NewTokenBucket(tpsLimit, tpsLimit, time.Now())
	httpClient := httpio.NewClient(10 * time.Second)

	grpcServer := grpc.NewServer()
	grpcv1.RegisterOperatorSubmitServiceServer(grpcServer, grpcserver.New(httpClient, tpsBucket, publisher, &redisEndpointResolver{registry: redisClient}))

	lis, err := net.Listen("tcp", ":9000")
	if err != nil {
		log.Fatalf("не удалось забиндить gRPC-порт 9000: %v", err)
	}
	go func() {
		log.Println("gRPC OperatorSubmitService слушает :9000")
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("gRPC server failed: %v", err)
		}
	}()

	// handle_dlr_webhook + normalize_and_publish_dlr
	//
	// BACKOFFICE_ROADMAP.md P0#1: раньше ОДИН общий WEBHOOK_AUTH_TOKEN env
	// var аутентифицировал DLR от ЛЮБОГО оператора на голом "/webhook/dlr"
	// (CODE_REVIEW.md HIGH finding до этого — insecure-дефолт
	// "demo-webhook-token" — было закрыто раньше, но общий секрет на всех
	// операторов сразу остался: утечка/ротация credential'а одного
	// оператора требовала бы регенерации секрета для ВСЕХ, и не было
	// способа отозвать доступ только одному). Маршрут теперь несёт
	// operator_id (тот же identifier, что operator.schema.json
	// operator_id/OPERATOR_ID env var — не изобретён заново), и
	// OperatorTokenAuthenticator резолвит per-operator токен через
	// webhookAuthLookup (Configuration Redis credential_ref -> Vault
	// secret value, internal/webhookauth) — операторы больше не делят один
	// секрет.
	authenticator := webhook.OperatorTokenAuthenticator{Lookup: webhookAuthLookup}
	webhookHandler := webhook.Handler(authenticator, func(dlr webhook.RawDlr) {
		event := kafkaio.BuildDlrEvent(dlr.OperatorID, dlr, time.Now())
		if err := publisher.PublishDlr(context.Background(), event); err != nil {
			log.Printf("normalize_and_publish_dlr failed: %v", err)
		}
	})
	webhookMux := http.NewServeMux()
	webhookMux.HandleFunc("/webhook/dlr/{operator_id}", webhookHandler)
	// CODE_REVIEW.md CRITICAL finding: этот сервер обязан быть доступен из
	// интернета (реальные операторы шлют DLR сюда) и раньше не имел ни
	// ReadTimeout/WriteTimeout/IdleTimeout, ни MaxHeaderBytes — тривиальный
	// неаутентифицированный DoS (медленно льющееся тело/заголовки без
	// таймаута на прерывание чтения). webhook.Handler отдельно ограничивает
	// размер тела через http.MaxBytesReader (см. webhook.go).
	webhookSrv := &http.Server{
		Addr:           ":8080",
		Handler:        webhookMux,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		IdleTimeout:    60 * time.Second,
		MaxHeaderBytes: 16 * 1024,
	}
	go func() {
		log.Println("webhook HTTP-сервер слушает :8080")
		if err := webhookSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("webhook server failed: %v", err)
		}
	}()

	heartbeatTicker := time.NewTicker(30 * time.Second)
	defer heartbeatTicker.Stop()
	go func() {
		for range heartbeatTicker.C {
			if err := redisClient.Heartbeat(context.Background(), operatorID, routeID); err != nil {
				log.Printf("heartbeat_tick failed: %v", err)
			}
		}
	}()

	healthState.SetDependencyChecks(map[string]func(context.Context) error{
		"redis_runtime":       redisClient.Ping,
		"redis_configuration": opConfigReader.Ping,
		"vault":               vaultClient.Ping,
	})
	healthState.SetReady(true)
	log.Println("operator-http-gateway готов")

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	log.Println("остановка operator-http-gateway")
	grpcServer.GracefulStop()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = webhookSrv.Shutdown(shutdownCtx)
	_ = healthSrv.Shutdown(shutdownCtx)
	_ = redisClient.Unregister(context.Background(), operatorID, routeID)
}