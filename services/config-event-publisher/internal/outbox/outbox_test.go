package outbox

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("CONFIG_EVENT_PUBLISHER_TEST_DSN")
	if dsn == "" {
		dsn = "postgres://localhost:5432/mpp"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Skipf("не удалось создать пул подключений к Postgres (%v) — пропуск", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Skipf("Postgres недоступен на %q (%v) — пропуск", dsn, err)
	}
	return pool
}

func uniqueEntityID(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}

func insertUnpublishedOutboxRow(t *testing.T, pool *pgxpool.Pool, entityType, entityID string, payload []byte) int64 {
	t.Helper()
	var id int64
	err := pool.QueryRow(context.Background(), `
		INSERT INTO config.config_outbox (config_version_id, entity_type, entity_id, payload)
		VALUES (NULL, $1, $2, $3)
		RETURNING id
	`, entityType, entityID, payload).Scan(&id)
	if err != nil {
		t.Fatalf("insert test outbox row failed: %v", err)
	}
	return id
}

func TestPollOutboxReturnsOnlyUnpublishedRows(t *testing.T) {
	pool := testPool(t)
	defer pool.Close()
	s := New(pool)
	ctx := context.Background()

	entityID := uniqueEntityID("acme")
	id := insertUnpublishedOutboxRow(t, pool, "partner", entityID, []byte(`{"partner_id":"acme"}`))

	entries, err := s.PollOutbox(ctx, 1000)
	if err != nil {
		t.Fatalf("PollOutbox failed: %v", err)
	}

	found := false
	for _, e := range entries {
		if e.ID == id {
			found = true
			if e.EntityID != entityID || e.EntityType != "partner" {
				t.Fatalf("неверные поля записи: %+v", e)
			}
		}
	}
	if !found {
		t.Fatalf("ожидали найти вставленную unpublished-запись id=%d среди %d результатов", id, len(entries))
	}

	if err := s.MarkPublished(ctx, id); err != nil {
		t.Fatalf("MarkPublished failed: %v", err)
	}

	entriesAfter, err := s.PollOutbox(ctx, 1000)
	if err != nil {
		t.Fatalf("PollOutbox after mark_published failed: %v", err)
	}
	for _, e := range entriesAfter {
		if e.ID == id {
			t.Fatalf("запись id=%d должна была исчезнуть из poll_outbox после mark_published", id)
		}
	}
}

func TestMarkPublishedSetsPublishedAtNotNull(t *testing.T) {
	pool := testPool(t)
	defer pool.Close()
	s := New(pool)
	ctx := context.Background()

	id := insertUnpublishedOutboxRow(t, pool, "operator", uniqueEntityID("beeline_uz"), []byte(`{}`))
	if err := s.MarkPublished(ctx, id); err != nil {
		t.Fatalf("MarkPublished failed: %v", err)
	}

	var published bool
	var publishedAtIsNull bool
	err := pool.QueryRow(ctx, `SELECT published, published_at IS NULL FROM config.config_outbox WHERE id = $1`, id).
		Scan(&published, &publishedAtIsNull)
	if err != nil {
		t.Fatalf("readback failed: %v", err)
	}
	if !published || publishedAtIsNull {
		t.Fatalf("ожидали published=true и непустой published_at, получили published=%v published_at_is_null=%v", published, publishedAtIsNull)
	}
}

func TestMarkPublishedErrorsOnUnknownID(t *testing.T) {
	pool := testPool(t)
	defer pool.Close()
	s := New(pool)

	if err := s.MarkPublished(context.Background(), -1); err == nil {
		t.Fatalf("ожидали ошибку для несуществующего id")
	}
}

func TestPollOutboxOrdersByCreatedAtAscending(t *testing.T) {
	pool := testPool(t)
	defer pool.Close()
	s := New(pool)
	ctx := context.Background()

	prefix := uniqueEntityID("order-test")
	id1 := insertUnpublishedOutboxRow(t, pool, "partner", prefix+"-1", []byte(`{}`))
	time.Sleep(5 * time.Millisecond)
	id2 := insertUnpublishedOutboxRow(t, pool, "partner", prefix+"-2", []byte(`{}`))

	entries, err := s.PollOutbox(ctx, 1000)
	if err != nil {
		t.Fatalf("PollOutbox failed: %v", err)
	}

	pos := map[int64]int{}
	for i, e := range entries {
		pos[e.ID] = i
	}
	if _, ok := pos[id1]; !ok {
		t.Fatalf("id1 не найден в результатах")
	}
	if _, ok := pos[id2]; !ok {
		t.Fatalf("id2 не найден в результатах")
	}
	if pos[id1] >= pos[id2] {
		t.Fatalf("ожидали id1 раньше id2 (created_at ASC), получили позиции %d и %d", pos[id1], pos[id2])
	}
}