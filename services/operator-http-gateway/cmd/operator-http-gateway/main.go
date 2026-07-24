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

	"google.golang.org/grpc"

	"mpp/operator-http-gateway/internal/core"
	"mpp/operator-http-gateway/internal/grpcserver"
	"mpp/operator-http-gateway/internal/health"
	"mpp/operator-http-gateway/internal/httpio"
	"mpp/operator-http-gateway/internal/kafkaio"
	"mpp/operator-http-gateway/internal/registry"
	"mpp/operator-http-gateway/internal/webhook"

	grpcv1 "mpp/platformcontracts/grpc/v1"
)

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
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
	authenticator := webhook.StaticTokenAuthenticator{Token: env("WEBHOOK_AUTH_TOKEN", "demo-webhook-token")}
	webhookHandler := webhook.Handler(authenticator, func(dlr webhook.RawDlr) {
		event := kafkaio.BuildDlrEvent(operatorID, dlr, time.Now())
		if err := publisher.PublishDlr(context.Background(), event); err != nil {
			log.Printf("normalize_and_publish_dlr failed: %v", err)
		}
	})
	webhookMux := http.NewServeMux()
	webhookMux.HandleFunc("/webhook/dlr", webhookHandler)
	webhookSrv := &http.Server{Addr: ":8080", Handler: webhookMux}
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