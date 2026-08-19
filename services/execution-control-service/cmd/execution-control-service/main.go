// Execution Control Service — вычисление ACTIVE/DEGRADED/PAUSED, гистерезис,
// dwell time, композиция scope, admission/dispatch rate, controlled ramp-up,
// partner billing freeze, manual override (services_specifictaion.md §4.1).
// Гистерезис перенесён 1:1 из state_machines/execution_control_hysteresis.py
// (development_plan.md 3.1) — см. internal/hysteresis.
package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	_ "github.com/KimMachineGun/automemlimit"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"

	"mpp/execution-control-service/internal/grpcserver"
	"mpp/execution-control-service/internal/health"
	"mpp/execution-control-service/internal/hysteresis"
	"mpp/execution-control-service/internal/kafkaio"
	"mpp/execution-control-service/internal/registry"
	"mpp/execution-control-service/internal/signals"
	"mpp/execution-control-service/internal/store"

	grpcv1 "mpp/platformcontracts/grpc/v1"
)

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// defaultThresholds — стартовые пороги для GLOBAL scope. Конкретные значения
// per-scope (partner/stage overrides) в этом первом срезе не читаются из
// PostgreSQL "сохранённые правила" (service_io_contracts.md §3.1 — "PostgreSQL
// SQL (read) — сохранённые правила/overrides") — используется единый
// STANDARD-профиль из state_machines/execution_control_hysteresis.py для
// всех scope, пока не появится отдельная схема хранения per-scope порогов.
var defaultThresholds = hysteresis.Thresholds{
	EnterDegraded:                0.5,
	ExitDegraded:                 0.3,
	EnterPaused:                  0.8,
	ExitPaused:                   0.5,
	EnterConfirmationWindowTicks: 3,
	ExitConfirmationWindowTicks:  5,
	MinStateDurationTicks:        5,
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

// runGlobalControlLoop — control loop для scope=GLOBAL: раз в тик снимает
// error_rate через Prometheus (collect_signals), прогоняет через
// evaluate_hysteresis (registry.Evaluate) и публикует ExecutionControlRecord
// в execution.control (publish_control_record). Ошибки Prometheus/Kafka
// логируются и не останавливают цикл — оба внешних сервиса не
// гарантированно живы (см. README.md "Что не проверено").
func runGlobalControlLoop(ctx context.Context, reg *registry.Registry, prom *signals.Client, pub *kafkaio.Publisher) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	key := registry.ScopeKey{Scope: hysteresis.ScopeGlobal}

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			sig, err := prom.Query(ctx, "global_error_rate", `sum(rate(mpp_stage_errors_total[1m])) / sum(rate(mpp_stage_processed_total[1m]))`)
			if err != nil {
				log.Printf("collect_signals: prometheus query failed: %v", err)
				continue
			}
			if math.IsNaN(sig.Value) || math.IsInf(sig.Value, 0) {
				// CODE_REVIEW.md MEDIUM finding: 0/0 (низкий/нулевой трафик)
				// легитимно даёт NaN на этот запрос; без явной проверки
				// сравнения в гистерезисе с NaN всегда false, поэтому scope,
				// застрявший в PAUSED в тихий период, никогда не подтвердит
				// восстановление, и это никак не логируется. Пропускаем тик
				// вместо того, чтобы подавать гистерезису мусорное значение.
				log.Printf("collect_signals: global_error_rate вернул нечисловое значение (%v), пропускаем тик", sig.Value)
				continue
			}

			eval := reg.Evaluate(key, sig.Value, now)
			rec := kafkaio.BuildControlRecord(key, eval, now)
			if err := pub.Publish(ctx, key, rec); err != nil {
				log.Printf("publish_control_record: kafka produce failed: %v", err)
			}
		}
	}
}

func main() {
	healthState := &health.State{}

	// Health-сервер стартует немедленно — readyz=503 до готовности к работе
	// (k8s readinessProbe не пускает трафик на под, пока не готов) —
	// та же конвенция, что services/destination-resolution-service/src/health.rs.
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
	audit := store.NewAuditStore(pool)

	reg := registry.New(defaultThresholds)

	brokers := strings.Split(env("KAFKA_BOOTSTRAP_SERVERS", "kafka-bootstrap.mpp.svc:9092"), ",")
	publisher, err := kafkaio.NewPublisher(brokers)
	if err != nil {
		log.Fatalf("не удалось создать Kafka producer: %v", err)
	}
	defer publisher.Close()

	// publisher передаётся в gRPC-сервер, чтобы ApplyOverride/ClearOverride
	// публиковали ExecutionControlRecord для ЛЮБОГО scope, а не только
	// GLOBAL через runGlobalControlLoop (CODE_REVIEW.md CRITICAL finding —
	// freeze/unfreeze для PARTNER_STAGE от Billing Reconciliation раньше
	// никогда не доходил до Kafka).
	grpcServer := grpc.NewServer()
	grpcv1.RegisterExecutionControlServiceServer(grpcServer, grpcserver.New(reg, audit, publisher))

	lis, err := net.Listen("tcp", ":9000")
	if err != nil {
		log.Fatalf("не удалось забиндить gRPC-порт 9000: %v", err)
	}
	go func() {
		log.Println("gRPC ExecutionControlService слушает :9000")
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("gRPC server failed: %v", err)
		}
	}()

	promClient := signals.NewClient(env("PROMETHEUS_URL", "http://prometheus-operated.mpp.svc:9090"))

	loopCtx, loopCancel := context.WithCancel(context.Background())
	defer loopCancel()
	go runGlobalControlLoop(loopCtx, reg, promClient, publisher)

	healthState.SetReady(true)
	log.Println("execution-control-service готов")

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	log.Println("остановка execution-control-service")
	grpcServer.GracefulStop()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = healthSrv.Shutdown(shutdownCtx)
}
