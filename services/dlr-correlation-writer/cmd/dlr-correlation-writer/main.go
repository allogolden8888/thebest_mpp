package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	_ "github.com/KimMachineGun/automemlimit"
	"github.com/twmb/franz-go/pkg/kgo"

	"mpp/dlr-correlation-writer/internal/health"
	"mpp/dlr-correlation-writer/internal/kafkaio"
	"mpp/dlr-correlation-writer/internal/writer"
)

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// buildDatabaseURL — реальная находка (см. services/dlr-manager/README.md
// "Реальная находка (систематическая...)" для полного разбора): k8s
// инжектит POSTGRES_HOST/PORT/DB/USER/PASSWORD дискретно (envFrom
// secretRef), не единую DATABASE_URL, которую этот сервис читал раньше —
// в реальном кластере он никогда бы не подключился. DATABASE_URL оставлен
// как явный override для локальной разработки/тестов.
func buildDatabaseURL() string {
	if v := os.Getenv("DATABASE_URL"); v != "" {
		return v
	}
	host := getenv("POSTGRES_HOST", "postgresql.mpp.svc")
	port := getenv("POSTGRES_PORT", "5432")
	db := getenv("POSTGRES_DB", "mpp")
	user := getenv("POSTGRES_USER", "mpp")
	password := os.Getenv("POSTGRES_PASSWORD")
	poolMaxConns := getenv("POSTGRES_POOL_MAX_CONNS", "8")
	if password == "" {
		return fmt.Sprintf("postgres://%s@%s:%s/%s?sslmode=disable&pool_max_conns=%s", url.QueryEscape(user), host, port, db, poolMaxConns)
	}
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable&pool_max_conns=%s", url.QueryEscape(user), url.QueryEscape(password), host, port, db, poolMaxConns)
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	healthState := &health.State{}
	healthServer := &http.Server{Addr: ":9090", Handler: health.Router(healthState)}
	go func() {
		if err := healthServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("health server: %v", err)
		}
	}()

	pgWriter, err := writer.NewPgWriter(ctx, buildDatabaseURL())
	if err != nil {
		log.Fatalf("writer.NewPgWriter: %v", err)
	}
	defer pgWriter.Close()

	brokers := strings.Split(getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-bootstrap.mpp.svc:9092"), ",")
	consumer, err := kafkaio.NewConsumer(brokers, "dlr-correlation-writer")
	if err != nil {
		log.Fatalf("kafkaio.NewConsumer: %v", err)
	}
	defer consumer.Close()

	batchSize := 500
	if v := os.Getenv("BATCH_MAX_SIZE"); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil && parsed > 0 {
			batchSize = parsed
		}
	}
	flushInterval := 2 * time.Second
	if v := os.Getenv("BATCH_FLUSH_INTERVAL_MS"); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil && parsed > 0 {
			flushInterval = time.Duration(parsed) * time.Millisecond
		}
	}

	healthState.SetReady(true)

	buf := writer.NewBatchBuffer(batchSize)
	latestByPartition := make(map[int32]*kgo.Record)
	// suspendedPartitions — CODE_REVIEW.md PART 2, dlr-correlation-writer #1:
	// как только партиция даёт хотя бы одну недекодируемую запись, дальнейшие
	// записи ЭТОЙ партиции игнорируются (не буферизуются, latestByPartition
	// для неё больше не продвигается) до перезапуска процесса — иначе более
	// свежая успешная запись той же партиции молча закоммитила бы offset мимо
	// непрочитанной. См. kafkaio.Consumer.PollOnce.
	suspendedPartitions := make(map[int32]bool)
	lastFlush := time.Now()

	// retentionInterval/retainHours — см. writer.PgWriter.DropOldPartitions.
	// 48ч по умолчанию — та же консервативная граница, что уже
	// задокументирована в migrations/V015 и dlr-manager's DLR_CORRELATION_WINDOW,
	// требует того же уточнения по реальным SLA операторов (development_plan.md
	// 5.6, не решено здесь).
	retentionInterval := time.Hour
	if v := os.Getenv("RETENTION_CHECK_INTERVAL"); v != "" {
		if parsed, err := time.ParseDuration(v); err == nil {
			retentionInterval = parsed
		}
	}
	retainHours := 48
	if v := os.Getenv("DLR_CORRELATION_RETAIN_HOURS"); v != "" {
		// Верхняя граница — не косметика. time.Duration это int64 наносекунд,
		// поэтому time.Duration(retainHours)*time.Hour переполняется примерно
		// на 2 562 047 часах и становится ОТРИЦАТЕЛЬНЫМ. Отрицательный maxPast
		// в SetPartitionWindow означает, что ни один реальный submitted_at не
		// попадёт в окно: PlanPartitions отвергнет всё, запись встанет
		// полностью. Ровно этот класс отказа уже стоил сервису 18 дней
		// простоя (партиции не создавались, вставки падали с SQLSTATE 23514),
		// и обнаружился он только когда мы пошли искать, почему нет DLR.
		// Опечатка в env (лишние нули) не должна давать тот же результат
		// молча, поэтому значение зажимается, а не отбрасывается: зажатое
		// окно оставляет сервис рабочим, отброшенное — тоже, но человек,
		// поставивший число, не узнает, что оно не применилось.
		const maxRetainHours = 24 * 365 // год — заведомо больше любого разумного окна корреляции
		if parsed, err := strconv.Atoi(v); err == nil && parsed > 0 {
			if parsed > maxRetainHours {
				log.Printf("dlr-correlation-writer: DLR_CORRELATION_RETAIN_HOURS=%d выходит за верхнюю границу, зажато до %d", parsed, maxRetainHours)
				parsed = maxRetainHours
			}
			retainHours = parsed
		}
	}
	// Окно создания партиций жёстко связано с окном retention: писать в
	// час, который DropOldPartitions дропнет следующим же проходом,
	// бессмысленно, а расхождение этих двух чисел — источник ровно того
	// класса дефектов, который здесь и чинится.
	pgWriter.SetPartitionWindow(time.Duration(retainHours)*time.Hour, writer.DefaultPartitionMaxFuture)
	lastRetentionRun := time.Now()

	errHandler := func(err error) { log.Printf("dlr-correlation-writer: %v", err) }

	flush := func() {
		if buf.Len() == 0 {
			lastFlush = time.Now()
			return
		}
		snapshot := buf.Snapshot()
		correlations := make([]writer.CorrelationRecord, len(snapshot))
		for i, rec := range snapshot {
			correlations[i] = rec.Correlation
		}
		// EnsurePartitionsForBatch — см. writer.PlanPartitions. Партиции
		// обеспечиваются под ФАКТИЧЕСКИЕ submitted_at записей батча, а не
		// только под текущий/следующий час, как было раньше: при любом
		// отставании от топика (или replay) submitted_at попадал в прошлые
		// часы, партиций под которые нет, batch insert падал с SQLSTATE
		// 23514, offset не двигался и сервис вставал в вечный retry —
		// ровно это и наблюдалось 18 суток подряд в логах.
		plan, err := pgWriter.EnsurePartitionsForBatch(ctx, correlations, time.Now())
		if err != nil {
			errHandler(err)
			lastFlush = time.Now()
			return
		}
		if len(plan.Rejected) > 0 {
			// Осознанно отбрасываем, а не пытаемся вставить: submitted_at
			// вне окна корреляции коррелировать всё равно уже некому
			// (партиция такого часа либо уже дропнута retention'ом, либо
			// таймстамп битый), а попытка вставки заблокировала бы весь
			// батч навсегда. Не молча — логируем количество и образец.
			sample := plan.Rejected[0]
			log.Printf("dlr-correlation-writer: %d записей отброшено — submitted_at вне окна партиционирования (-%dч..+%dч); образец: operator_id=%s message_id=%s submitted_at=%s",
				len(plan.Rejected), retainHours, int(writer.DefaultPartitionMaxFuture.Hours()),
				sample.OperatorID, sample.MessageID, sample.SubmittedAt.UTC().Format(time.RFC3339))
		}
		if err := pgWriter.Flush(ctx, plan.Accepted); err != nil {
			// Не коммитим, не чистим буфер — та же запись переобработается
			// на следующем flush'е (at-least-once, тот же принцип "коммит
			// только после подтверждённой записи", что у всех остальных
			// Kafka-consumer'ов этой сессии).
			errHandler(err)
			lastFlush = time.Now()
			return
		}

		maxOffsets := writer.MaxOffsets(snapshot)
		toCommit := make([]*kgo.Record, 0, len(maxOffsets))
		for partition := range maxOffsets {
			if rec, ok := latestByPartition[partition]; ok {
				toCommit = append(toCommit, rec)
			}
		}
		if err := consumer.CommitRecords(ctx, toCommit...); err != nil {
			// Flush в PostgreSQL уже успешен (идемпотентно, ON CONFLICT DO
			// NOTHING) — повторная обработка того же батча при рестарте
			// безопасна, просто лишний no-op insert, не потеря/задвоение.
			errHandler(err)
		}
		buf.Clear()
		lastFlush = time.Now()
	}

	for {
		select {
		case <-ctx.Done():
			flush() // последний best-effort flush перед выходом
			_ = healthServer.Close()
			return
		default:
		}

		pollCtx, cancel := context.WithTimeout(ctx, flushInterval)
		consumer.PollOnce(pollCtx, func(rec writer.BufferedRecord, raw *kgo.Record) {
			if suspendedPartitions[rec.Partition] {
				return
			}
			latestByPartition[rec.Partition] = raw
			if shouldFlush := buf.Add(rec); shouldFlush {
				flush()
			}
		}, func(partition int32) {
			suspendedPartitions[partition] = true
		}, errHandler)
		cancel()

		if time.Since(lastFlush) >= flushInterval {
			flush()
		}

		if time.Since(lastRetentionRun) >= retentionInterval {
			if dropped, err := pgWriter.DropOldPartitions(ctx, retainHours); err != nil {
				errHandler(err)
			} else if dropped > 0 {
				log.Printf("dlr-correlation-writer: retention удалила %d партиций старше %dч", dropped, retainHours)
			}
			lastRetentionRun = time.Now()
		}
	}
}
