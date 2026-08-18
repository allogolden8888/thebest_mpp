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

// insertUnpublishedOutboxRow — реальная находка: postgres://localhost:5432/mpp
// (дефолтный DSN этих тестов) не изолированная тестовая БД, а тот же общий
// локальный dev-инстанс, что используют другие сервисы/тесты и постоянный
// демо-сид (migrations/README.md, V022: ~6300 строк policy_template,
// намеренно оставлены unpublished). PollOutbox — ORDER BY created_at ASC
// LIMIT N — корректное поведение для реального продюсера очереди (старые
// записи первыми, ограниченный батч), не баг: только что вставленная строка
// хронологически САМАЯ НОВАЯ, поэтому никогда не попадает в top-N среди
// тысяч более старых строк общей таблицы. Тест не должен предполагать, что
// владеет позицией в неограниченной общей таблице — и не должен трогать/
// удалять чужие данные, которые нужны другим тестам. Фикс: явно проставляем
// created_at далеко в прошлом (не текущим now() по умолчанию), так что
// тестовая строка гарантированно сортируется раньше любых реальных/демо-
// данных, при этом сдвиг на константу сохраняет ОТНОСИТЕЛЬНЫЙ порядок между
// несколькими строками, вставленными одним тестом (см.
// TestPollOutboxOrdersByCreatedAtAscending).
func insertUnpublishedOutboxRow(t *testing.T, pool *pgxpool.Pool, entityType, entityID string, payload []byte) int64 {
	t.Helper()
	var id int64
	err := pool.QueryRow(context.Background(), `
		INSERT INTO config.config_outbox (config_version_id, entity_type, entity_id, payload, created_at)
		VALUES (NULL, $1, $2, $3, now() - interval '100 years')
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

// CODE_REVIEW.md finding #3 — SELECT ... FOR UPDATE SKIP LOCKED: once a row
// is claimed by one poll, a concurrent poll (simulating a second replica)
// must not see it again until the claim expires.
func TestPollOutboxDoesNotReturnAlreadyClaimedRow(t *testing.T) {
	pool := testPool(t)
	defer pool.Close()
	s := New(pool)
	ctx := context.Background()

	id := insertUnpublishedOutboxRow(t, pool, "partner", uniqueEntityID("claim-test"), []byte(`{}`))

	first, err := s.PollOutboxWithLimits(ctx, 1000, DefaultMaxAttempts, time.Hour)
	if err != nil {
		t.Fatalf("first PollOutboxWithLimits failed: %v", err)
	}
	foundInFirst := false
	for _, e := range first {
		if e.ID == id {
			foundInFirst = true
		}
	}
	if !foundInFirst {
		t.Fatalf("ожидали найти id=%d в первом poll", id)
	}

	// Второй "replica" poll в течение claimTTL не должен снова увидеть ту же
	// строку — иначе обе реплики опубликуют одно и то же событие дважды.
	second, err := s.PollOutboxWithLimits(ctx, 1000, DefaultMaxAttempts, time.Hour)
	if err != nil {
		t.Fatalf("second PollOutboxWithLimits failed: %v", err)
	}
	for _, e := range second {
		if e.ID == id {
			t.Fatalf("id=%d не должен был снова быть выбран, пока claim не истёк (multi-replica safety)", id)
		}
	}
}

// CODE_REVIEW.md finding #3 — claim истекает через claimTTL, так что запись
// не потеряна навсегда, если реплика упала между claim и Mark*.
func TestPollOutboxReclaimsRowAfterClaimTTLExpires(t *testing.T) {
	pool := testPool(t)
	defer pool.Close()
	s := New(pool)
	ctx := context.Background()

	id := insertUnpublishedOutboxRow(t, pool, "partner", uniqueEntityID("claim-expiry-test"), []byte(`{}`))

	if _, err := s.PollOutboxWithLimits(ctx, 1000, DefaultMaxAttempts, 0); err != nil {
		t.Fatalf("first PollOutboxWithLimits failed: %v", err)
	}

	// claimTTL=0 — claim истекает немедленно, следующий poll должен снова
	// увидеть ту же строку (реплика, держащая claim, считается упавшей).
	again, err := s.PollOutboxWithLimits(ctx, 1000, DefaultMaxAttempts, 0)
	if err != nil {
		t.Fatalf("second PollOutboxWithLimits failed: %v", err)
	}
	found := false
	for _, e := range again {
		if e.ID == id {
			found = true
		}
	}
	if !found {
		t.Fatalf("ожидали, что id=%d снова станет доступен после истечения claimTTL=0", id)
	}
}

// CODE_REVIEW.md finding #2 — poison-message head-of-line blocking, no
// bound, no DLQ: after maxAttempts failed MarkPublishFailed calls, the row
// must stop being selected by PollOutbox (bounded), instead of crowding out
// real pending rows forever.
func TestMarkPublishFailedStopsBeingPolledAfterMaxAttempts(t *testing.T) {
	pool := testPool(t)
	defer pool.Close()
	s := New(pool)
	ctx := context.Background()

	id := insertUnpublishedOutboxRow(t, pool, "partner", uniqueEntityID("poison-test"), []byte(`{}`))
	const maxAttempts = 3

	for i := 0; i < maxAttempts; i++ {
		entries, err := s.PollOutboxWithLimits(ctx, 1000, maxAttempts, 0)
		if err != nil {
			t.Fatalf("PollOutboxWithLimits (attempt %d) failed: %v", i, err)
		}
		found := false
		for _, e := range entries {
			if e.ID == id {
				found = true
			}
		}
		if !found {
			t.Fatalf("attempt %d: ожидали, что id=%d ещё выбирается (attempts=%d < maxAttempts=%d)", i, id, i, maxAttempts)
		}

		exhausted, err := s.MarkPublishFailed(ctx, id, fmt.Errorf("simulated kafka error"), maxAttempts)
		if err != nil {
			t.Fatalf("MarkPublishFailed (attempt %d) failed: %v", i, err)
		}
		wantExhausted := i == maxAttempts-1
		if exhausted != wantExhausted {
			t.Fatalf("attempt %d: exhausted=%v, ожидали %v", i, exhausted, wantExhausted)
		}
	}

	// attempts теперь == maxAttempts — строка больше не должна выбираться.
	entriesAfter, err := s.PollOutboxWithLimits(ctx, 1000, maxAttempts, 0)
	if err != nil {
		t.Fatalf("final PollOutboxWithLimits failed: %v", err)
	}
	for _, e := range entriesAfter {
		if e.ID == id {
			t.Fatalf("id=%d исчерпал %d попыток, но всё ещё выбирается poll_outbox — poison-message HOL blocking не устранён", id, maxAttempts)
		}
	}

	var attempts int32
	var lastError *string
	if err := pool.QueryRow(ctx, `SELECT attempts, last_error FROM config.config_outbox WHERE id = $1`, id).Scan(&attempts, &lastError); err != nil {
		t.Fatalf("readback failed: %v", err)
	}
	if attempts != maxAttempts {
		t.Fatalf("ожидали attempts=%d, получили %d", maxAttempts, attempts)
	}
	if lastError == nil || *lastError == "" {
		t.Fatalf("ожидали непустой last_error после MarkPublishFailed")
	}
}

// CODE_REVIEW.md finding #2 — a retriable failure must not silently
// disappear: MarkPublishFailed clears the claim so the row is immediately
// eligible for retry by the next poll (below maxAttempts), it must not be
// treated as if it were published.
func TestMarkPublishFailedClearsClaimForImmediateRetry(t *testing.T) {
	pool := testPool(t)
	defer pool.Close()
	s := New(pool)
	ctx := context.Background()

	id := insertUnpublishedOutboxRow(t, pool, "partner", uniqueEntityID("retry-test"), []byte(`{}`))

	if _, err := s.PollOutboxWithLimits(ctx, 1000, DefaultMaxAttempts, time.Hour); err != nil {
		t.Fatalf("PollOutboxWithLimits failed: %v", err)
	}
	if _, err := s.MarkPublishFailed(ctx, id, fmt.Errorf("transient redis timeout"), DefaultMaxAttempts); err != nil {
		t.Fatalf("MarkPublishFailed failed: %v", err)
	}

	// Даже с большим claimTTL строка должна быть немедленно доступна снова
	// — MarkPublishFailed обязан снять claim.
	entries, err := s.PollOutboxWithLimits(ctx, 1000, DefaultMaxAttempts, time.Hour)
	if err != nil {
		t.Fatalf("PollOutboxWithLimits after failure failed: %v", err)
	}
	found := false
	for _, e := range entries {
		if e.ID == id {
			found = true
		}
	}
	if !found {
		t.Fatalf("ожидали, что id=%d снова доступен сразу после MarkPublishFailed (retriable failure не должен теряться)", id)
	}
}