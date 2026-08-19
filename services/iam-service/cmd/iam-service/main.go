// IAM Service — Фаза 0 плана закрытия API-пробелов (Identity/RBAC/Audit,
// фундамент): роли/права/назначения для backoffice-персонала, синхронная
// CheckPermission для RequireRole -> RequirePermission в backoffice-api,
// заготовка под партнёрских пользователей портала (Фаза 3). Прямая замена
// заглушки migrations/V017__backoffice_stub.sql — см. migrations/V025__iam.sql.
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

	_ "github.com/KimMachineGun/automemlimit"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"

	"mpp/iam-service/internal/grpcserver"
	"mpp/iam-service/internal/health"
	"mpp/iam-service/internal/store"

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
	poolMaxConns := env("POSTGRES_POOL_MAX_CONNS", "8")
	if user == "" {
		return fmt.Sprintf("postgres://%s:%s/%s?pool_max_conns=%s", host, port, db, poolMaxConns)
	}
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?pool_max_conns=%s", user, password, host, port, db, poolMaxConns)
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

	grpcServer := grpc.NewServer()
	grpcv1.RegisterIamServiceServer(grpcServer, grpcserver.New(pg))

	lis, err := net.Listen("tcp", ":9000")
	if err != nil {
		log.Fatalf("не удалось забиндить gRPC-порт 9000: %v", err)
	}
	go func() {
		log.Println("gRPC IamService слушает :9000")
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("gRPC server failed: %v", err)
		}
	}()

	healthState.SetDependencyChecks(map[string]func(context.Context) error{
		"postgres": pg.Ping,
	})
	healthState.SetReady(true)
	log.Println("iam-service готов")

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	log.Println("остановка iam-service")
	grpcServer.GracefulStop()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = healthSrv.Shutdown(shutdownCtx)
}
