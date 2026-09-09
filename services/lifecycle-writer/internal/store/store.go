// Package store — batch_buffer + flush_batch (service_internal_methods.md
// §6.1): batch UPSERT/INSERT (COPY) в PostgreSQL.
package store

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"mpp/lifecycle-writer/internal/core"
)

type Store struct {
	pool *pgxpool.Pool

	// ensuredHours — какие часы уже созданы этим процессом. Без кэша
	// EnsurePartitionsForBatch делал бы round trip на каждый час каждого
	// батча; с ним установившийся поток (один-два часа) не делает ни
	// одного после первого раза.
	mu           sync.Mutex
	ensuredHours map[time.Time]struct{}
	maxPast      time.Duration
	maxFuture    time.Duration
}

// Окно партиций по умолчанию. maxPast щедрый намеренно: replay после
// длительного простоя — штатный сценарий этого сервиса, и именно на нём
// прежняя логика ломалась.
const (
	DefaultPartitionMaxPast   = 48 * time.Hour
	DefaultPartitionMaxFuture = 24 * time.Hour
)

func New(pool *pgxpool.Pool) *Store {
	return &Store{
		pool:         pool,
		ensuredHours: make(map[time.Time]struct{}),
		maxPast:      DefaultPartitionMaxPast,
		maxFuture:    DefaultPartitionMaxFuture,
	}
}

// SetPartitionWindow — окно связывается с retention (см. main.go): писать в
// час, который DropOldPartitions удалит следующим проходом, бессмысленно.
func (s *Store) SetPartitionWindow(maxPast, maxFuture time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maxPast, s.maxFuture = maxPast, maxFuture
}

// PartitionPlan — какие часы создать и какие строки не принимать.
type PartitionPlan struct {
	Hours    []time.Time
	Accepted []core.LifecycleHistoryRow
	Rejected []core.LifecycleHistoryRow
}

// PlanHistoryPartitions — ИЗМЕРЕННЫЙ БАГ, не гипотеза.
//
// Прежняя версия создавала партиции только под now и now+1h, тогда как
// ключ партиционирования — occurred_at САМОГО СОБЫТИЯ, а не время flush'а.
// При replay после простоя occurred_at лежит в прошлых часах, партиции для
// них нет, и BatchInsertLifecycleHistory падает с SQLSTATE 23514. Поскольку
// офсеты коммитятся только после успеха ВСЕХ шагов flush, одна такая
// ошибка блокирует продвижение consumer'а по всем трём топикам разом и
// повторяется вечно.
//
// Замерено на живом стенде: 8 684 одинаковые ошибки подряд, по одной в
// секунду; lag incoming.messages 69 946; офсет message.lifecycle не
// зафиксирован ни разу; сервис жёг 131% CPU НА ПРОСТОЕ, повторяя одну и ту
// же неуспешную вставку. Прогон, во время которого это происходило, был
// признан недействительным как замер производительности платформы.
//
// Ровно эта же правка уже сделана в dlr-correlation-writer
// (writer.PlanPartitions) после того, как тот по той же причине простоял
// 18 суток. Здесь она перенесена тем же способом.
//
// Строки вне окна отбрасываются, а не переносятся: партиция под них будет
// удалена retention'ом, и бесконечный повтор превратил бы их в poison pill
// того же класса, который здесь и лечится. Отбрасывание логируется
// вызывающим.
func PlanHistoryPartitions(rows []core.LifecycleHistoryRow, now time.Time, maxPast, maxFuture time.Duration) PartitionPlan {
	nowHour := now.UTC().Truncate(time.Hour)
	earliest := now.UTC().Add(-maxPast).Truncate(time.Hour)
	latest := now.UTC().Add(maxFuture).Truncate(time.Hour)

	plan := PartitionPlan{Accepted: make([]core.LifecycleHistoryRow, 0, len(rows))}
	seen := make(map[time.Time]struct{}, len(rows))
	addHour := func(hour time.Time) {
		if _, ok := seen[hour]; ok {
			return
		}
		seen[hour] = struct{}{}
		plan.Hours = append(plan.Hours, hour)
	}
	// Текущий и следующий час нужны даже при пустом батче: буфер может
	// пересечь границу часа между первой записью и flush'ем.
	addHour(nowHour)
	addHour(nowHour.Add(time.Hour))

	for _, r := range rows {
		hour := r.OccurredAt.UTC().Truncate(time.Hour)
		if hour.Before(earliest) || hour.After(latest) {
			plan.Rejected = append(plan.Rejected, r)
			continue
		}
		addHour(hour)
		plan.Accepted = append(plan.Accepted, r)
	}
	sort.Slice(plan.Hours, func(i, j int) bool { return plan.Hours[i].Before(plan.Hours[j]) })
	return plan
}

// EnsurePartitionsForBatch — создаёт недостающие партиции под ФАКТИЧЕСКИЕ
// occurred_at батча перед вставкой. Число round trip'ов ограничено сверху
// шириной окна (48+24 = максимум 73 часа), на практике — нулём благодаря
// кэшу ensuredHours.
func (s *Store) EnsurePartitionsForBatch(ctx context.Context, rows []core.LifecycleHistoryRow, now time.Time) (PartitionPlan, error) {
	s.mu.Lock()
	maxPast, maxFuture := s.maxPast, s.maxFuture
	s.mu.Unlock()

	plan := PlanHistoryPartitions(rows, now, maxPast, maxFuture)
	for _, hour := range plan.Hours {
		s.mu.Lock()
		_, done := s.ensuredHours[hour]
		s.mu.Unlock()
		if done {
			continue
		}
		if err := s.EnsurePartition(ctx, hour); err != nil {
			return plan, err
		}
		s.mu.Lock()
		s.ensuredHours[hour] = struct{}{}
		s.mu.Unlock()
	}
	return plan, nil
}

// EnsurePartition — реальный, воспроизведённый вживую баг (не гипотеза):
// `messaging.message_lifecycle_history` (V005) партиционирована по часам,
// `V015__partition_maintenance.sql` создаёт партиции только на момент
// применения миграций и явно документирует, что
// `messaging.create_lifecycle_history_partition` дальше вызывается любым
// внешним планировщиком (k8s CronJob/pg_cron) раз в час — но такой
// планировщик нигде в репозитории не заведён (grep по
// create_lifecycle_history_partition/CronJob — пусто). На практике это
// значит: как только текущий час выходит за пределы бутстрап-окна,
// BatchInsertLifecycleHistory начинает падать с "no partition of relation
// found for row" НА КАЖДОМ flush — а поскольку flush коммитит офсеты
// ТОЛЬКО после того, как отработают все четыре batch-шага (insert/update/
// history/dlq), эта постоянная ошибка блокирует commit офсетов для ВСЕХ
// трёх топиков разом (incoming.messages/message.lifecycle/DLQ), не только
// для history. Consumer перестаёт продвигаться вообще — именно это и
// выглядит как "current_status застревает": сервис живой, буфер растёт,
// но ничего не коммитится, и при любом рестарте начинается replay с
// давно устаревшего офсета.
//
// Тот же принцип самообслуживания, что уже применён в
// dlr-correlation-writer/internal/writer/pg_writer.go::EnsurePartition
// (этот сервис — единственный писатель в таблицу, он и берёт на себя
// то, для чего не завели внешний планировщик) — вызывается перед каждым
// flush в cmd/lifecycle-writer/main.go, дешёвый идемпотентный вызов
// (CREATE TABLE IF NOT EXISTS внутри функции).
func (s *Store) EnsurePartition(ctx context.Context, hourStart time.Time) error {
	_, err := s.pool.Exec(ctx, "SELECT messaging.create_lifecycle_history_partition($1)", hourStart.Truncate(time.Hour))
	if err != nil {
		return fmt.Errorf("messaging.create_lifecycle_history_partition: %w", err)
	}
	return nil
}

// DropOldPartitions — та же логика, что dlr-correlation-writer's
// DropOldPartitions: messaging.drop_old_lifecycle_history_partitions
// (V015) была определена, но нигде не вызывалась — партиции росли бы
// неограниченно. Вызывается реже, чем EnsurePartition (см. main.go —
// раз в час, не на каждый flush: DROP TABLE, не дешёвый idempotent
// CREATE IF NOT EXISTS).
func (s *Store) DropOldPartitions(ctx context.Context, retainHours int) (int, error) {
	var dropped int
	err := s.pool.QueryRow(ctx, "SELECT messaging.drop_old_lifecycle_history_partitions($1)", retainHours).Scan(&dropped)
	if err != nil {
		return 0, fmt.Errorf("messaging.drop_old_lifecycle_history_partitions: %w", err)
	}
	return dropped, nil
}

// InsertReadModel — первая строка read model (INSERT, ON CONFLICT DO NOTHING
// — IncomingMessage не должен переопределять уже существующую строку, если
// consumer перечитывает после рестарта). Оставлен для read-only/одиночных
// вызовов (например тестов) — реальный consume-путь использует
// BatchInsertReadModel, см. ниже.
func (s *Store) InsertReadModel(ctx context.Context, row core.ReadModelRow) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO messaging.message_read_model
			(message_id, partner_id, application_id, trace_id, pipeline_id, pipeline_version, current_status, terminal, created_at, updated_at, sandbox)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $9, $10)
		ON CONFLICT (message_id) DO NOTHING
	`, row.MessageID, row.PartnerID, row.ApplicationID, row.TraceID, row.PipelineID, row.PipelineVersion,
		row.CurrentStatus, row.Terminal, row.Timestamp, row.Sandbox)
	if err != nil {
		return fmt.Errorf("insert message_read_model: %w", err)
	}
	return nil
}

// BatchInsertReadModel — batch-версия InsertReadModel (CODE_REVIEW.md
// MEDIUM finding: read model писался синхронно по одной строке за раз,
// не батчем вместе с history/dlq, как описывает service_internal_methods.md
// §6.1 — риск отставания consumer'а под нагрузкой).
func (s *Store) BatchInsertReadModel(ctx context.Context, rows []core.ReadModelRow) error {
	if len(rows) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for _, row := range rows {
		batch.Queue(`
			INSERT INTO messaging.message_read_model
				(message_id, partner_id, application_id, trace_id, pipeline_id, pipeline_version, current_status, terminal, created_at, updated_at, sandbox)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $9, $10)
			ON CONFLICT (message_id) DO NOTHING
		`, row.MessageID, row.PartnerID, row.ApplicationID, row.TraceID, row.PipelineID, row.PipelineVersion,
			row.CurrentStatus, row.Terminal, row.Timestamp, row.Sandbox)
	}
	br := s.pool.SendBatch(ctx, batch)
	defer br.Close()
	for range rows {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("batch insert message_read_model: %w", err)
		}
	}
	return nil
}

// UpdateReadModel — обновление current_status/terminal по message.lifecycle.
// Оставлен для read-only/одиночных вызовов — реальный consume-путь
// использует BatchUpdateReadModel, см. ниже.
//
// lifecycle_version — CODE_REVIEW.md MEDIUM finding: раньше обновление
// было безусловным, без защиты от переупорядоченной/повторной доставки —
// партиционный rebalance, редоставивший старый SUBMITTED уже ПОСЛЕ того,
// как был применён более новый DELIVERED, откатывал партнёр-facing read
// model назад. `AND lifecycle_version < $5` — обновление применяется,
// только если оно новее уже применённого (миграция V019).
func (s *Store) UpdateReadModel(ctx context.Context, update core.ReadModelUpdate) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE messaging.message_read_model
		SET current_status = $2, terminal = $3, updated_at = $4, lifecycle_version = $5
		WHERE message_id = $1 AND lifecycle_version < $5
	`, update.MessageID, update.CurrentStatus, update.Terminal, update.UpdatedAt, update.LifecycleVersion)
	if err != nil {
		return fmt.Errorf("update message_read_model: %w", err)
	}
	return nil
}

// BatchUpdateReadModel — batch-версия UpdateReadModel, та же
// lifecycle_version-защита от out-of-order/дубликатов.
//
// **Реальная гонка, найденная при разборе "current_status застревает",
// не гипотеза**: incoming.messages и message.lifecycle — РАЗНЫЕ топики
// одного consumer'а (main.go), PollFetches ничего не гарантирует про
// порядок между ними. Если самое первое message.lifecycle-событие для
// message_id (обычно SUBMITTED) обрабатывается раньше, чем
// incoming.messages создаст строку read model для этого же message_id
// (совсем не гипотетически — под нагрузкой/при replay огромного backlog'а
// после рестарта, см. EnsurePartition выше, это НЕ редкий случай), то
// `WHERE message_id = $1 AND lifecycle_version < $5` находит НОЛЬ строк —
// не ошибка, просто 0 affected rows, молча. Раньше это событие терялось
// НАВСЕГДА: строка потом создаётся через INSERT со status='RECEIVED', и
// раз обновление уже "было" и пропало, ничто больше не пере-присылает тот
// же SUBMITTED — сообщение виснет на RECEIVED навечно, даже если реально
// давно DELIVERED.
//
// Исправление — не наивный UPSERT (у ReadModelUpdate нет
// partner_id/application_id/trace_id/created_at, INSERT ими не
// заполнить), а различение ДВУХ разных причин "0 affected rows":
// (а) строки для message_id ещё не существует — гонка, обновление нужно
// повторить, когда INSERT доедет (см. main.go: возвращённые здесь missing
// кладутся обратно в буфер на следующий tick); (б) строка существует, но
// lifecycle_version уже не новее — легитимный дубликат/устаревшая
// редоставка, ретраить НЕ нужно (иначе copilo растил бы буфер вечно).
// Отсюда — сначала проверяем, какие message_id вообще существуют, и
// применяем UPDATE только к существующим; остальные возвращаем вызывающей
// стороне как missing.
func (s *Store) BatchUpdateReadModel(ctx context.Context, updates []core.ReadModelUpdate) (missing []core.ReadModelUpdate, err error) {
	if len(updates) == 0 {
		return nil, nil
	}

	ids := make([]string, len(updates))
	for i, u := range updates {
		ids[i] = u.MessageID
	}
	rows, err := s.pool.Query(ctx, `SELECT message_id FROM messaging.message_read_model WHERE message_id = ANY($1)`, ids)
	if err != nil {
		return nil, fmt.Errorf("select existing message_read_model rows: %w", err)
	}
	existing := make(map[string]bool, len(updates))
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan message_id: %w", err)
		}
		existing[id] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate existing message_read_model rows: %w", err)
	}

	applicable := make([]core.ReadModelUpdate, 0, len(updates))
	for _, u := range updates {
		if existing[u.MessageID] {
			applicable = append(applicable, u)
		} else {
			missing = append(missing, u)
		}
	}
	if len(applicable) == 0 {
		return missing, nil
	}

	batch := &pgx.Batch{}
	for _, update := range applicable {
		batch.Queue(`
			UPDATE messaging.message_read_model
			SET current_status = $2, terminal = $3, updated_at = $4, lifecycle_version = $5
			WHERE message_id = $1 AND lifecycle_version < $5
		`, update.MessageID, update.CurrentStatus, update.Terminal, update.UpdatedAt, update.LifecycleVersion)
	}
	br := s.pool.SendBatch(ctx, batch)
	defer br.Close()
	for range applicable {
		if _, err := br.Exec(); err != nil {
			return nil, fmt.Errorf("batch update message_read_model: %w", err)
		}
	}
	return missing, nil
}

// BatchInsertLifecycleHistory — COPY-стиль batch insert через pgx.Batch.
func (s *Store) BatchInsertLifecycleHistory(ctx context.Context, rows []core.LifecycleHistoryRow) error {
	if len(rows) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for _, row := range rows {
		batch.Queue(`
			INSERT INTO messaging.message_lifecycle_history
				(message_id, lifecycle_version, status, event_id, occurred_at, source)
			VALUES ($1, $2, $3, $4, $5, $6)
			ON CONFLICT DO NOTHING
		`, row.MessageID, row.LifecycleVersion, row.Status, row.EventID, row.OccurredAt, row.Source)
	}
	br := s.pool.SendBatch(ctx, batch)
	defer br.Close()
	for range rows {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("batch insert message_lifecycle_history: %w", err)
		}
	}
	return nil
}

// BatchInsertDlq — batch insert dlq_record.
func (s *Store) BatchInsertDlq(ctx context.Context, rows []core.DlqRow) error {
	if len(rows) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for _, row := range rows {
		batch.Queue(`
			INSERT INTO messaging.dlq_record
				(stage_execution_id, message_id, stage_name, attempt, original_command, reason_code, error_detail, created_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
			ON CONFLICT (stage_execution_id) DO NOTHING
		`, row.StageExecutionID, row.MessageID, row.StageName, row.Attempt, row.OriginalCommand,
			row.ReasonCode, row.ErrorDetail, row.CreatedAt)
	}
	br := s.pool.SendBatch(ctx, batch)
	defer br.Close()
	for range rows {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("batch insert dlq_record: %w", err)
		}
	}
	return nil
}