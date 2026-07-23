// Scheduler — Critical Sweep (services_specifictaion.md §3.1, пересмотрено):
// раз в ~1с опрашивает шардированный Redis sorted set deadlines:{bucket} на
// просроченные записи, публикует retry/timeout/DLQ, принимает ручные команды
// FORCE_TIMEOUT/FORCE_RETRY из scheduler.critical.commands.
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"

	"mpp/scheduler-critical-sweep/internal/controlsnapshot"
	"mpp/scheduler-critical-sweep/internal/health"
	"mpp/scheduler-critical-sweep/internal/kafkaio"
	"mpp/scheduler-critical-sweep/internal/redisio"
	"mpp/scheduler-critical-sweep/internal/sweep"

	eventsv1 "mpp/platformcontracts/events/v1"
)

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

// defaultRetryPolicy — единый профиль для всех стадий в этом срезе.
// pipeline.schema.json не описывает retry-параметры per-stage (только граф
// переходов по Outcome) — значения ниже рабочее предположение, см. README.
var defaultRetryPolicy = sweep.RetryPolicy{
	MaxAttempts: 3,
	Backoff: func(attempt int32, now time.Time) time.Time {
		return now.Add(time.Duration(attempt) * 10 * time.Second)
	},
}

func main() {
	healthState := &health.State{}
	healthSrv := &http.Server{Addr: ":9090", Handler: health.Router(healthState)}
	go func() {
		if err := healthSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("health server failed: %v", err)
		}
	}()

	redisClient := redisio.NewClient(
		env("REDIS_RUNTIME_HOST", "localhost")+":"+env("REDIS_RUNTIME_PORT", "6379"),
		env("REDIS_RUNTIME_PASSWORD", ""),
		envInt("DEADLINE_BUCKETS", 16),
	)
	defer redisClient.Close()

	brokers := strings.Split(env("KAFKA_BOOTSTRAP_SERVERS", "kafka-bootstrap.mpp.svc:9092"), ",")
	publisher, err := kafkaio.NewPublisher(brokers)
	if err != nil {
		log.Fatalf("не удалось создать Kafka producer: %v", err)
	}
	defer publisher.Close()

	snapshot := controlsnapshot.New()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go runManualCommandConsumer(ctx, brokers)
	go runControlSnapshotConsumer(ctx, brokers, snapshot)
	go runSweepLoop(ctx, redisClient, publisher, snapshot)

	healthState.SetReady(true)
	log.Println("scheduler-critical-sweep готов")

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	log.Println("остановка scheduler-critical-sweep")
	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = healthSrv.Shutdown(shutdownCtx)
}

// runManualCommandConsumer — on_manual_command: FORCE_TIMEOUT/FORCE_RETRY,
// минуя обычный sweep. В этом срезе только логирует прочитанную команду —
// прямая обработка конкретного stage_execution_id в обход sweep требует
// того же ResolveMessageID+LoadExecutionState пути, что и обычный тик;
// не реализовано отдельно в этой первой версии (см. README).
func runManualCommandConsumer(ctx context.Context, brokers []string) {
	consumer, err := kafkaio.NewManualCommandConsumer(brokers, "scheduler-critical-sweep")
	if err != nil {
		log.Printf("не удалось создать consumer scheduler.critical.commands: %v", err)
		return
	}
	defer consumer.Close()

	consumer.Run(ctx,
		func(cmd *eventsv1.SchedulerCriticalCommand) {
			log.Printf("on_manual_command: stage_execution_id=%s task_type=%v requested_by=%s",
				cmd.GetStageExecutionId(), cmd.GetTaskType(), cmd.GetRequestedBy())
		},
		func(err error) {
			log.Printf("scheduler.critical.commands consume error: %v", err)
		},
	)
}

// runControlSnapshotConsumer — поддержание локального immutable snapshot
// execution.control, читаемого check_execution_control на каждом тике.
func runControlSnapshotConsumer(ctx context.Context, brokers []string, snapshot *controlsnapshot.Snapshot) {
	consumer, err := kafkaio.NewControlSnapshotConsumer(brokers, "scheduler-critical-sweep")
	if err != nil {
		log.Printf("не удалось создать consumer execution.control: %v", err)
		return
	}
	defer consumer.Close()

	consumer.Run(ctx,
		func(rec *eventsv1.ExecutionControlRecord) {
			snapshot.Apply(rec.GetScope(), rec.GetScopeId(), rec.GetState())
		},
		func(err error) {
			log.Printf("execution.control consume error: %v", err)
		},
	)
}

// runSweepLoop — tick_sweep раз в ~1с (services_specifictaion.md §3.1).
func runSweepLoop(ctx context.Context, redisClient *redisio.Client, publisher *kafkaio.Publisher, snapshot *controlsnapshot.Snapshot) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			processTick(ctx, now, redisClient, publisher, snapshot)
		}
	}
}

func processTick(ctx context.Context, now time.Time, redisClient *redisio.Client, publisher *kafkaio.Publisher, snapshot *controlsnapshot.Snapshot) {
	entries, err := redisClient.TickSweep(ctx, now)
	if err != nil {
		log.Printf("tick_sweep failed: %v", err)
		return
	}

	for _, entry := range entries {
		messageID, err := redisClient.ResolveMessageID(ctx, entry.StageExecutionID)
		if err != nil {
			log.Printf("ResolveMessageID(%s) failed: %v", entry.StageExecutionID, err)
			continue
		}
		state, err := redisClient.LoadExecutionState(ctx, messageID)
		if err != nil {
			log.Printf("LoadExecutionState(%s) failed: %v", messageID, err)
			continue
		}

		switch sweep.Decide(state, defaultRetryPolicy, snapshot) {
		case sweep.ActionHold:
			// Execution control сейчас PAUSED для этой стадии — оставляем
			// дедлайн как есть, следующий тик перепроверит.
			continue

		case sweep.ActionRetry:
			newDeadline := sweep.NextDeadline(state.Attempt+1, defaultRetryPolicy, now)
			cmd := kafkaio.BuildRetryCommand(state, uuid.NewString(), newDeadline, "")
			if err := publisher.PublishRetry(ctx, cmd); err != nil {
				log.Printf("publish_retry failed: %v", err)
				continue
			}

		case sweep.ActionTimeout:
			ev := kafkaio.BuildTimeoutEvent(state, uuid.NewString(), now)
			if err := publisher.PublishTimeout(ctx, ev); err != nil {
				log.Printf("publish_timeout_result failed: %v", err)
				continue
			}

		case sweep.ActionDlq:
			original := kafkaio.BuildRetryCommand(state, uuid.NewString(), now, "")
			rec := kafkaio.BuildDlqRecord(state, original, "retry attempts exhausted", now)
			if err := publisher.PublishDlq(ctx, rec); err != nil {
				log.Printf("publish_dlq failed: %v", err)
				continue
			}
		}

		if err := redisClient.ClearDeadline(ctx, entry); err != nil {
			log.Printf("clear_deadline failed: %v", err)
		}
	}
}
