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
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"

	"mpp/scheduler-critical-sweep/internal/controlsnapshot"
	"mpp/scheduler-critical-sweep/internal/health"
	"mpp/scheduler-critical-sweep/internal/kafkaio"
	"mpp/scheduler-critical-sweep/internal/redisio"
	"mpp/scheduler-critical-sweep/internal/sweep"

	commonv1 "mpp/platformcontracts/common/v1"
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

const (
	// heldBackoff — CODE_REVIEW.md finding #9: сколько не перепроверять
	// запись, для которой check_execution_control вернул Hold (стадия на
	// паузе). Не слишком долго (быстро подхватить снятие паузы), не
	// слишком коротко (не молотить Redis весь инцидент раз в ~1с).
	heldBackoff = 5 * time.Second

	// unresolvedBackoff — CODE_REVIEW.md finding #8: сколько не
	// перепроверять запись, для которой ResolveMessageID/LoadExecutionState
	// не смогли найти состояние (сегодня — ожидаемый путь для КАЖДОЙ
	// записи, пока Pipeline Engine не пишет stage_exec_index, см. README).
	// Дольше heldBackoff — это диагностируемая проблема конфигурации/
	// координации, не штатное ожидание снятия паузы.
	unresolvedBackoff = 30 * time.Second
)

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

	var wg sync.WaitGroup
	controlWarm := make(chan struct{})

	// CODE_REVIEW.md finding #11: WaitGroup вокруг всех трёх фоновых
	// циклов — без него cancel() не гарантирует, что публикация/клэйм,
	// начатые до отмены, успеют завершиться до того, как ниже сработают
	// defer redisClient.Close()/publisher.Close().
	wg.Add(1)
	go func() {
		defer wg.Done()
		runManualCommandConsumer(ctx, brokers)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		runControlSnapshotConsumer(ctx, brokers, snapshot, controlWarm)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		runSweepLoop(ctx, redisClient, publisher, snapshot)
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	// CODE_REVIEW.md finding #6: раньше SetReady(true) вызывался сразу
	// после запуска горутин, до того, как control snapshot успевал сделать
	// хоть один цикл PollFetches — комбинация с fail-open IsPaused()
	// (controlsnapshot) означала, что под мог пройти readiness и сразу
	// слепо ретраить, пока GLOBAL PAUSE ещё не долетел до локального
	// снапшота (особенно опасно на rolling restart во время активного
	// инцидента). Теперь readiness ждёт первого успешного прохода
	// консьюмера execution.control — но не блокирует обработку SIGTERM,
	// если стоп пришёл раньше прогрева.
	select {
	case <-controlWarm:
		healthState.SetReady(true)
		log.Println("scheduler-critical-sweep готов (execution.control snapshot прогрет)")
		<-stop
	case <-stop:
		log.Println("остановка до прогрева execution.control snapshot")
	}

	log.Println("остановка scheduler-critical-sweep")
	cancel()
	wg.Wait()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = healthSrv.Shutdown(shutdownCtx)
}

// runManualCommandConsumer — on_manual_command: FORCE_TIMEOUT/FORCE_RETRY,
// минуя обычный sweep. В этом срезе только логирует прочитанную команду —
// прямая обработка конкретного stage_execution_id в обход sweep требует
// того же ResolveMessageID+LoadExecutionState пути, что и обычный тик;
// не реализовано отдельно в этой первой версии (см. README).
//
// Собственный consumer group id ("scheduler-critical-sweep-manual-commands",
// CODE_REVIEW.md finding #5) — отдельный от execution.control, у которого
// (после finding #4) вообще нет группы. Partitioned-группа здесь корректна:
// scheduler.critical.commands — обычный командный топик, а не compacted
// local snapshot, делить партиции между репликами можно и нужно.
func runManualCommandConsumer(ctx context.Context, brokers []string) {
	backoff := time.Second
	const maxBackoff = 30 * time.Second

	for {
		if ctx.Err() != nil {
			return
		}

		consumer, err := kafkaio.NewManualCommandConsumer(brokers, "scheduler-critical-sweep-manual-commands")
		if err != nil {
			log.Printf("не удалось создать consumer scheduler.critical.commands: %v — retry через %s", err, backoff)
			if !sleepOrDone(ctx, backoff) {
				return
			}
			backoff = nextBackoff(backoff, maxBackoff)
			continue
		}
		backoff = time.Second

		consumer.Run(ctx,
			func(cmd *eventsv1.SchedulerCriticalCommand) {
				log.Printf("on_manual_command: stage_execution_id=%s task_type=%v requested_by=%s",
					cmd.GetStageExecutionId(), cmd.GetTaskType(), cmd.GetRequestedBy())
			},
			func(err error) {
				log.Printf("scheduler.critical.commands consume error: %v", err)
			},
		)
		consumer.Close()

		if ctx.Err() != nil {
			return
		}
		log.Printf("scheduler.critical.commands consumer завершился неожиданно, пересоздаём через %s", backoff)
		if !sleepOrDone(ctx, backoff) {
			return
		}
	}
}

// runControlSnapshotConsumer — поддержание ПОЛНОГО локального immutable
// snapshot execution.control, читаемого check_execution_control на каждом
// тике (CODE_REVIEW.md finding #4/#6). warm закрывается ровно один раз,
// после первого успешного прохода PollFetches — main() ждёт этого перед
// SetReady(true).
//
// Retry/backoff и на ошибке создания клиента, и на неожиданном завершении
// Run() (обрыв соединения с брокером) — раньше ошибка при старте просто
// логировалась, и горутина завершалась НАВСЕГДА, оставляя snapshot пустым
// на весь жизненный цикл пода (finding #6).
func runControlSnapshotConsumer(ctx context.Context, brokers []string, snapshot *controlsnapshot.Snapshot, warm chan<- struct{}) {
	var markWarmOnce sync.Once
	markWarm := func() { markWarmOnce.Do(func() { close(warm) }) }

	backoff := time.Second
	const maxBackoff = 30 * time.Second

	for {
		if ctx.Err() != nil {
			return
		}

		consumer, err := kafkaio.NewControlSnapshotConsumer(brokers)
		if err != nil {
			log.Printf("не удалось создать consumer execution.control: %v — retry через %s", err, backoff)
			if !sleepOrDone(ctx, backoff) {
				return
			}
			backoff = nextBackoff(backoff, maxBackoff)
			continue
		}
		backoff = time.Second

		consumer.Run(ctx,
			func(scope commonv1.ExecutionControlScope, scopeID string, state commonv1.ExecutionControlState) {
				snapshot.Apply(scope, scopeID, state)
			},
			func(scope commonv1.ExecutionControlScope, scopeID string) {
				snapshot.Delete(scope, scopeID)
			},
			markWarm,
			func(err error) {
				log.Printf("execution.control consume error: %v", err)
			},
		)
		consumer.Close()

		if ctx.Err() != nil {
			return
		}
		log.Printf("execution.control consumer завершился неожиданно, пересоздаём через %s", backoff)
		if !sleepOrDone(ctx, backoff) {
			return
		}
	}
}

// sleepOrDone — ждёт d или отмену контекста, что раньше; возвращает false,
// если контекст отменился первым (вызывающая сторона должна прекратить
// retry-цикл, не спать).
func sleepOrDone(ctx context.Context, d time.Duration) bool {
	select {
	case <-time.After(d):
		return true
	case <-ctx.Done():
		return false
	}
}

func nextBackoff(current, cap time.Duration) time.Duration {
	next := current * 2
	if next > cap {
		return cap
	}
	return next
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

// deadlineStore — то подмножество *redisio.Client, которое нужно
// processTick/processEntry. Введён вместе с commandPublisher, чтобы этот
// самый рискованный код сервиса (публикация retry/timeout/DLQ, порядок
// claim->publish->restore) можно было юнит-тестировать с фейками, без
// реального Redis/Kafka — CODE_REVIEW.md "Test quality": раньше processTick
// принимал конкретные *redisio.Client/*kafkaio.Publisher и не мог быть
// протестирован иначе как интеграционно.
type deadlineStore interface {
	TickSweep(ctx context.Context, now time.Time) ([]sweep.ExpiredEntry, error)
	ResolveMessageID(ctx context.Context, stageExecutionID string) (string, error)
	LoadExecutionState(ctx context.Context, messageID string) (sweep.ExecutionState, error)
	ClaimDeadline(ctx context.Context, entry sweep.ExpiredEntry, now time.Time) (bool, error)
	RestoreDeadline(ctx context.Context, entry sweep.ExpiredEntry) error
	DeferDeadline(ctx context.Context, entry sweep.ExpiredEntry, until time.Time) error
}

// commandPublisher — то подмножество *kafkaio.Publisher, которое нужно
// processEntry.
type commandPublisher interface {
	PublishRetry(ctx context.Context, cmd *commonv1.StageExecuteCommand) error
	PublishTimeout(ctx context.Context, ev *commonv1.StageCompletedEvent) error
	PublishDlq(ctx context.Context, rec *eventsv1.DlqRecord) error
}

func processTick(ctx context.Context, now time.Time, store deadlineStore, publisher commandPublisher, snapshot sweep.ControlSnapshot) {
	entries, err := store.TickSweep(ctx, now)
	if err != nil {
		log.Printf("tick_sweep failed: %v", err)
		return
	}

	for _, entry := range entries {
		processEntry(ctx, now, entry, store, publisher, snapshot)
	}
}

// processEntry — одна просроченная запись: resolve -> load -> decide ->
// (claim -> publish, restore при неудаче) | defer (Hold/нерезолвящиеся).
//
// Порядок claim ПЕРЕД publish (не после, как раньше) — CODE_REVIEW.md
// finding #3: ClaimDeadline атомарно удаляет запись из ZSET, так что из
// нескольких конкурентных реплик ровно одна успешно её заберёт (см.
// redisio.Client.ClaimDeadline). Если сама публикация после успешного
// клэйма не удаётся, запись явно возвращается в ZSET (RestoreDeadline,
// finding #7) — не теряется молча и не дублируется.
func processEntry(ctx context.Context, now time.Time, entry sweep.ExpiredEntry, store deadlineStore, publisher commandPublisher, snapshot sweep.ControlSnapshot) {
	messageID, err := store.ResolveMessageID(ctx, entry.StageExecutionID)
	if err != nil {
		log.Printf("ResolveMessageID(%s) failed: %v — откладываем на %s (finding #8, не тайт-луп)", entry.StageExecutionID, err, unresolvedBackoff)
		if derr := store.DeferDeadline(ctx, entry, now.Add(unresolvedBackoff)); derr != nil {
			log.Printf("DeferDeadline(%s) failed: %v", entry.StageExecutionID, derr)
		}
		return
	}

	state, err := store.LoadExecutionState(ctx, messageID)
	if err != nil {
		log.Printf("LoadExecutionState(%s) failed: %v — откладываем на %s (finding #8, не тайт-луп)", messageID, err, unresolvedBackoff)
		if derr := store.DeferDeadline(ctx, entry, now.Add(unresolvedBackoff)); derr != nil {
			log.Printf("DeferDeadline(%s) failed: %v", entry.StageExecutionID, derr)
		}
		return
	}

	action := sweep.Decide(state, defaultRetryPolicy, snapshot)
	if action == sweep.ActionHold {
		// Execution control сейчас PAUSED для этой стадии — откладываем
		// перепроверку (finding #9), не пересчитываем на каждом тике.
		if derr := store.DeferDeadline(ctx, entry, now.Add(heldBackoff)); derr != nil {
			log.Printf("DeferDeadline(%s) failed: %v", entry.StageExecutionID, derr)
		}
		return
	}

	claimed, err := store.ClaimDeadline(ctx, entry, now)
	if err != nil {
		log.Printf("claim_deadline(%s) failed: %v", entry.StageExecutionID, err)
		return
	}
	if !claimed {
		// Другая реплика уже забрала эту запись первой — штатный исход под
		// конкурентным sweep (finding #3), не ошибка.
		return
	}

	var publishErr error
	switch action {
	case sweep.ActionRetry:
		newDeadline := sweep.NextDeadline(state.Attempt+1, defaultRetryPolicy, now)
		cmd, hasExtension := kafkaio.BuildRetryCommand(state, uuid.NewString(), state.Attempt+1, newDeadline, "")
		if hasExtension {
			publishErr = publisher.PublishRetry(ctx, cmd)
		} else {
			// Finding #1: без stage_extension republish гарантированно
			// REJECTED на стороне стадии-потребителя (проверено против
			// destination-resolution-service). Честно уходим в DLQ с явной
			// причиной вместо того, чтобы жечь retry-бюджет на три
			// заведомо неверных попытки подряд.
			original, _ := kafkaio.BuildRetryCommand(state, uuid.NewString(), state.Attempt, state.Deadline, "")
			rec := kafkaio.BuildDlqRecord(state, original,
				"insufficient runtime state to rebuild stage_extension for retry — see README 'Открытый вопрос'", now)
			publishErr = publisher.PublishDlq(ctx, rec)
		}

	case sweep.ActionTimeout:
		ev := kafkaio.BuildTimeoutEvent(state, uuid.NewString(), now)
		publishErr = publisher.PublishTimeout(ctx, ev)

	case sweep.ActionDlq:
		// Finding #2: original_command строится с attempt=state.Attempt —
		// реально исчерпанная попытка, не state.Attempt+1 (которой не
		// существовало).
		original, _ := kafkaio.BuildRetryCommand(state, uuid.NewString(), state.Attempt, state.Deadline, "")
		rec := kafkaio.BuildDlqRecord(state, original, "retry attempts exhausted", now)
		publishErr = publisher.PublishDlq(ctx, rec)
	}

	if publishErr != nil {
		log.Printf("публикация не удалась для %s (action=%v): %v — возвращаем запись в планирование", entry.StageExecutionID, action, publishErr)
		if rerr := store.RestoreDeadline(ctx, entry); rerr != nil {
			log.Printf("RestoreDeadline(%s) failed: %v — запись потеряна из планирования до ручного вмешательства", entry.StageExecutionID, rerr)
		}
	}
}
