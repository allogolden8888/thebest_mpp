// Package outbox — poll_outbox + mark_published (service_internal_methods.md
// §3.3), against config.config_outbox (migrations/V003__config_outbox.sql),
// LEFT JOIN config.config_versions для version/status (NULL для
// policy_template/subscriber_consent, см. V003 комментарий).
package outbox

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Entry struct {
	ID         int64
	EntityType string
	EntityID   string
	Payload    []byte
	Version    int64
	Status     string
	CreatedAt  time.Time
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
	rows, err := s.pool.Query(ctx, `
		SELECT o.id, o.entity_type, o.entity_id, o.payload, o.created_at,
		       COALESCE(cv.version, 0), COALESCE(cv.status, 'active')
		FROM config.config_outbox o
		LEFT JOIN config.config_versions cv ON cv.id = o.config_version_id
		WHERE o.published = false
		ORDER BY o.created_at ASC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("poll_outbox query: %w", err)
	}
	defer rows.Close()

	var entries []Entry
	for rows.Next() {
		var e Entry
		if err := rows.Scan(&e.ID, &e.EntityType, &e.EntityID, &e.Payload, &e.CreatedAt, &e.Version, &e.Status); err != nil {
			return nil, fmt.Errorf("scan outbox row: %w", err)
		}
		entries = append(entries, e)
	}
	return entries, rows.Err()
}

// MarkPublished — mark_published: UPDATE после успешной публикации в Kafka.
func (s *Store) MarkPublished(ctx context.Context, id int64) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE config.config_outbox
		SET published = true, published_at = now()
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