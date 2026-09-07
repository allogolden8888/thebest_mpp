// Package correlation — lookup_correlation (service_internal_methods.md
// §4.2): PostgreSQL read из dlr.dlr_correlation (написана
// dlr-correlation-writer, migrations/V009__dlr_correlation.sql), плюс
// быстрый путь в Runtime Redis перед ней (см. Store.Lookup).
package correlation

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

type Record struct {
	OperatorID       string
	SmscMessageID    string
	SegmentID        int32
	MessageID        string
	StageExecutionID string
	SubmittedAt      time.Time
	ExpiresAt        time.Time
}

type Store struct {
	pool *pgxpool.Pool

	// fast — быстрый путь корреляции в Runtime Redis; nil, если не
	// включён (EnableFastPath не вызывался) — тогда Lookup работает ровно
	// как раньше, только по PostgreSQL.
	fast *redis.Client
	// onFastPathError — необязательный логгер сбоев быстрого пути. Сбой
	// быстрого пути НИКОГДА не проваливает Lookup: он деградирует к
	// PostgreSQL. Но молча глотать его нельзя — иначе выключившийся
	// Redis выглядел бы как «корреляция просто не находится».
	onFastPathError func(error)
}

func NewStore(ctx context.Context, databaseURL string) (*Store, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("pgxpool.New: %w", err)
	}
	return &Store{pool: pool}, nil
}

// EnableFastPath — подключает быстрый путь в Runtime Redis. Отдельный
// клиент (не переиспользуется pending.Store) сознательно: pending.Store —
// другая ответственность и другой жизненный цикл, а лишний пул соединений
// к тому же Redis стоит пренебрежимо мало.
func (s *Store) EnableFastPath(redisURL string, onError func(error)) error {
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		return fmt.Errorf("redis.ParseURL: %w", err)
	}
	s.fast = redis.NewClient(opts)
	s.onFastPathError = onError
	return nil
}

func (s *Store) Close() {
	s.pool.Close()
	if s.fast != nil {
		_ = s.fast.Close()
	}
}

// FastPathKey — ключ быстрого пути. Формат обязан совпадать байт в байт с
// delivery-service/SubmitIdempotencyStore.correlationKey (Java) — там же
// подробное обоснование, почему запись живёт именно в delivery-service и
// почему её TTL 15 минут, а не 48 часов.
func FastPathKey(operatorID, smscMessageID string, segmentID int32) string {
	return "dlrcorr:" + operatorID + ":" + smscMessageID + ":" + strconv.Itoa(int(segmentID))
}

// parseFastPathValue — разбор значения "{submitted_at_ms}|{message_id}|{stage_execution_id}"
// (SubmitIdempotencyStore.correlationValue). Чистая функция, тестируется без Redis.
func parseFastPathValue(value string) (submittedAt time.Time, messageID, stageExecutionID string, ok bool) {
	parts := strings.SplitN(value, "|", 3)
	if len(parts) != 3 {
		return time.Time{}, "", "", false
	}
	ms, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || parts[1] == "" || parts[2] == "" {
		return time.Time{}, "", "", false
	}
	return time.UnixMilli(ms).UTC(), parts[1], parts[2], true
}

// lookupFast — быстрый путь: один GET.
//
// СЕМАНТИКА ОДИН В ОДИН С PostgreSQL-ЗАПРОСОМ, включая защиту от
// переиспользования smsc_message_id, ради которой в SQL появилась граница
// `submitted_at <= received_at` (HIGH находка кодревью, см. Lookup):
// в Redis по ключу лежит ТОЛЬКО САМАЯ СВЕЖАЯ submission для этой тройки
// (operator_id, smsc_message_id, segment_id). Если её submitted_at не
// позже received_at этой DLR — она И ЕСТЬ ближайшая предшествующая, то
// есть ровно то, что вернул бы SQL. Если позже (smsc_message_id уже
// переиспользован более новой submission, а DLR относится к старой) —
// быстрый путь ОБЯЗАН промахнуться и отдать решение PostgreSQL, где
// лежит вся история и где `ORDER BY submitted_at DESC` с этой границей
// найдёт правильную, более раннюю строку. Поэтому проверка ниже —
// не «оптимизация», а условие корректности.
func (s *Store) lookupFast(ctx context.Context, operatorID, smscMessageID string, segmentID int32, receivedAt time.Time) *Record {
	if s.fast == nil {
		return nil
	}
	value, err := s.fast.Get(ctx, FastPathKey(operatorID, smscMessageID, segmentID)).Result()
	if errors.Is(err, redis.Nil) {
		return nil // штатный промах: записи ещё/уже нет
	}
	if err != nil {
		if s.onFastPathError != nil {
			s.onFastPathError(fmt.Errorf("быстрый путь корреляции недоступен, деградация к PostgreSQL: %w", err))
		}
		return nil
	}
	submittedAt, messageID, stageExecutionID, ok := parseFastPathValue(value)
	if !ok {
		if s.onFastPathError != nil {
			s.onFastPathError(fmt.Errorf("быстрый путь корреляции: неразбираемое значение %q", value))
		}
		return nil
	}
	if submittedAt.After(receivedAt) {
		return nil
	}
	return &Record{
		OperatorID:       operatorID,
		SmscMessageID:    smscMessageID,
		SegmentID:        segmentID,
		MessageID:        messageID,
		StageExecutionID: stageExecutionID,
		SubmittedAt:      submittedAt,
	}
}

// Lookup — самый свежий (по submitted_at), но НЕ ПОЗЖЕ receivedAt,
// correlation-ряд для этой (operator_id, smsc_message_id, segment_id).
//
// HIGH находка кодревью (PART 2, dlr-manager #1) — исправлено здесь:
// раньше запрос не был привязан ко времени самой DLR (`ORDER BY
// submitted_at DESC LIMIT 1` без границы), из-за чего оператор,
// переиспользующий smsc_message_id (нормальная практика при wraparound
// счётчика на стороне SMSC), приводил к неверной корреляции — DLR,
// пришедшая вскоре после submission A (submitted_at=T1), после того как
// тот же smsc_message_id был повторно занят submission'ом B
// (submitted_at=T2 > T1), находила бы B вместо A, потому что B "более
// свежий" в таблице целиком, а не более свежий ОТНОСИТЕЛЬНО момента DLR.
// `submitted_at <= received_at` — DLR физически не может отчитываться о
// submission, которого ещё не было с её же точки зрения; среди подходящих
// строк `ORDER BY submitted_at DESC LIMIT 1` теперь корректно берёт САМУЮ
// БЛИЖАЙШУЮ ПРЕДШЕСТВУЮЩУЮ попытку, не самую свежую вообще.
//
// `nil, nil` — не найдено, не ошибка (обычный, ожидаемый исход при первой
// попытке до того, как DLR обогнал корреляцию).
//
// БЫСТРЫЙ ПУТЬ. Durable-запись в dlr.dlr_correlation асинхронна:
// dlr-correlation-writer копит батч и флашит его раз в
// BATCH_FLUSH_INTERVAL_MS (2с). Измерено на живой системе: SMSC стоит в
// одной сети и отвечает мгновенно — operator.submit.accepted и
// operator.dlr по одному smsc_message_id попадают в Kafka с ОДНИМ И ТЕМ
// ЖЕ CreateTime, — поэтому к моменту этого Lookup строки в PostgreSQL
// ещё нет практически никогда, Decide возвращает KindScheduleRetry, и
// DLR уходит в dlr:pending:* + scheduler.background.commands. На прогоне
// 300 TPS: 27000 отправленных сообщений -> +8298 записей в
// delivery.status, +99658 ключей в Runtime Redis. Поэтому сначала
// спрашивается быстрый путь (одна GET-команда в Runtime Redis, пишется
// delivery-service тем же round trip'ом, что и dlvsubmit:*), и только
// при промахе — PostgreSQL. Путь retry НЕ удалён: он остаётся сеткой
// безопасности для DLR, пришедших позже TTL быстрого пути.
func (s *Store) Lookup(ctx context.Context, operatorID, smscMessageID string, segmentID int32, receivedAt time.Time) (*Record, error) {
	if rec := s.lookupFast(ctx, operatorID, smscMessageID, segmentID, receivedAt); rec != nil {
		return rec, nil
	}

	row := s.pool.QueryRow(ctx, `
		SELECT operator_id, smsc_message_id, segment_id, message_id, stage_execution_id, submitted_at, expires_at
		FROM dlr.dlr_correlation
		WHERE operator_id = $1 AND smsc_message_id = $2 AND segment_id = $3 AND submitted_at <= $4
		ORDER BY submitted_at DESC
		LIMIT 1
	`, operatorID, smscMessageID, segmentID, receivedAt)

	var rec Record
	err := row.Scan(&rec.OperatorID, &rec.SmscMessageID, &rec.SegmentID, &rec.MessageID, &rec.StageExecutionID, &rec.SubmittedAt, &rec.ExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("lookup dlr.dlr_correlation: %w", err)
	}
	return &rec, nil
}
