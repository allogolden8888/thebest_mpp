// Package outbox — poll_outbox + mark_published (service_internal_methods.md
// §3.3), against config.config_outbox (migrations/V003__config_outbox.sql),
// LEFT JOIN config.config_versions для version/status (NULL для
// policy_template/subscriber_consent, см. V003 комментарий).
//
// CODE_REVIEW.md findings #2/#3 fixed here (см. services/config-event-publisher/README.md
// "CODE_REVIEW.md — что исправлено" за полное описание):
//   - PollOutbox теперь атомарно "claim"-ит строки через
//     SELECT ... FOR UPDATE SKIP LOCKED, так что несколько реплик
//     (typical for HA) не публикуют одну и ту же запись дважды.
//   - Claim истекает через claimTTL (по умолчанию 30с) — если реплика
//     упала между claim и MarkPublished/MarkPublishFailed, запись не
//     потеряна навсегда, а снова становится доступна для poll.
//   - attempts ограничивает poison-message head-of-line blocking: запись,
//     которая не может быть опубликована после maxAttempts попыток,
//     перестаёт выбираться PollOutbox (не блокирует остальные unpublished
//     строки), но и не публикуется молча как "success" — она остаётся
//     published=false с last_error, видимая через прямой SQL-запрос
//     оператором (мягкий DLQ без отдельной таблицы/топика).
package outbox

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// DefaultMaxAttempts — после скольких неудачных попыток публикации запись
// перестаёт выбираться PollOutbox (CODE_REVIEW.md finding #2: "no bound,
// no DLQ" на poison-message head-of-line blocking).
const DefaultMaxAttempts = 10

// DefaultClaimTTL — сколько времени запись считается "занятой" одной
// репликой после claim, прежде чем снова станет доступна для poll (на
// случай, если реплика упала между claim и Mark*).
const DefaultClaimTTL = 30 * time.Second

type Entry struct {
	ID         int64
	EntityType string
	EntityID   string
	Payload    []byte
	Version    int64
	// Status — COALESCE(cv.status, "") сырое значение из config_versions;
	// пустая строка означает "config_version_id NULL для этой строки, cv.status
	// недоступен через JOIN" — entity-type-специфичное разрешение (payload_json
	// для policy_template, недоступно для subscriber_consent) происходит в
	// kafkaio.BuildConfigChangeEvent, не здесь (см. CODE_REVIEW.md finding #1).
	Status    string
	Attempts  int32
	CreatedAt time.Time
}

type Store struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// PollOutbox — poll_outbox: таймер/long-poll на PostgreSQL -> PendingEntries[].
// Использует partial index config_outbox_unpublished_idx (WHERE published=false).
func (s *Store) PollOutbox(ctx context.Context, limit int) ([]Entry, error) {
	return s.PollOutboxWithLimits(ctx, limit, DefaultMaxAttempts, DefaultClaimTTL)
}

// PollOutboxWithLimits — тот же poll_outbox, с явными maxAttempts/claimTTL
// (вынесено отдельно ради тестируемости граничных случаев без ожидания
// реального DefaultClaimTTL).
//
// CODE_REVIEW.md finding #3: SELECT ... FOR UPDATE SKIP LOCKED внутри
// транзакции — если запустить >1 реплики (typical for HA), они не будут
// клеймить одни и те же строки. Claim выставляется в той же транзакции
// сразу после SELECT, снаружи транзакции остаётся видимым остальным
// репликам как обычный committed claimed_at.
func (s *Store) PollOutboxWithLimits(ctx context.Context, limit, maxAttempts int, claimTTL time.Duration) ([]Entry, error) {
	var entries []Entry

	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		claimTTLSeconds := int64(claimTTL.Seconds())

		rows, err := tx.Query(ctx, `
			SELECT id
			FROM config.config_outbox
			WHERE published = false
			  AND attempts < $2
			  AND (claimed_at IS NULL OR claimed_at < now() - make_interval(secs => $3))
			ORDER BY created_at ASC
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		`, limit, maxAttempts, claimTTLSeconds)
		if err != nil {
			return fmt.Errorf("claim candidates query: %w", err)
		}
		var ids []int64
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return fmt.Errorf("scan candidate id: %w", err)
			}
			ids = append(ids, id)
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("claim candidates rows: %w", err)
		}
		rows.Close()

		if len(ids) == 0 {
			return nil
		}

		if _, err := tx.Exec(ctx, `
			UPDATE config.config_outbox SET claimed_at = now() WHERE id = ANY($1)
		`, ids); err != nil {
			return fmt.Errorf("claim update: %w", err)
		}

		detailRows, err := tx.Query(ctx, `
			SELECT o.id, o.entity_type, o.entity_id, o.payload, o.created_at,
			       o.attempts, COALESCE(cv.version, 0), cv.status
			FROM config.config_outbox o
			LEFT JOIN config.config_versions cv ON cv.id = o.config_version_id
			WHERE o.id = ANY($1)
			ORDER BY o.created_at ASC
		`, ids)
		if err != nil {
			return fmt.Errorf("claimed detail query: %w", err)
		}
		defer detailRows.Close()

		for detailRows.Next() {
			var e Entry
			// cv.status — NULL, когда LEFT JOIN не находит строку
			// config_versions (policy_template/subscriber_consent) —
			// сканирование NULL в *string локальную переменную, не
			// напрямую в string (тот же паттерн, что уже встречался в
			// backoffice-api DlqBrowse / replay-service LoadDlqRecord).
			var status *string
			if err := detailRows.Scan(&e.ID, &e.EntityType, &e.EntityID, &e.Payload, &e.CreatedAt, &e.Attempts, &e.Version, &status); err != nil {
				return fmt.Errorf("scan claimed outbox row: %w", err)
			}
			if status != nil {
				e.Status = *status
			}
			entries = append(entries, e)
		}
		return detailRows.Err()
	})
	if err != nil {
		return nil, err
	}
	return entries, nil
}

// MarkPublished — mark_published: UPDATE после успешной публикации в Kafka.
func (s *Store) MarkPublished(ctx context.Context, id int64) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE config.config_outbox
		SET published = true, published_at = now(), claimed_at = NULL
		WHERE id = $1
	`, id)
	if err != nil {
		return fmt.Errorf("mark_published: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("mark_published: outbox row %d не найдена", id)
	}
	return nil
}

// MarkPublishFailed — CODE_REVIEW.md finding #2: вызывается вместо
// молчаливого "continue" на ошибку публикации. Увеличивает attempts,
// сохраняет last_error, и снимает claim — запись немедленно снова
// доступна для poll (другой попыткой того же тика или следующей репликой),
// пока attempts не достигнет maxAttempts, после чего PollOutbox перестаёт
// её выбирать (mягкий DLQ, см. package doc).
//
// exhausted=true сообщает вызывающей стороне, что это был вклад,
// исчерпавший maxAttempts — стоит громко залогировать/заалертить, это уже
// не транзиентная ошибка.
func (s *Store) MarkPublishFailed(ctx context.Context, id int64, cause error, maxAttempts int) (exhausted bool, err error) {
	var attempts int32
	err = s.pool.QueryRow(ctx, `
		UPDATE config.config_outbox
		SET attempts = attempts + 1, last_error = $2, claimed_at = NULL
		WHERE id = $1
		RETURNING attempts
	`, id, cause.Error()).Scan(&attempts)
	if err != nil {
		return false, fmt.Errorf("mark_publish_failed: %w", err)
	}
	return int(attempts) >= maxAttempts, nil
}
