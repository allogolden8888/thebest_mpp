package store

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("PARTNER_API_TEST_DSN")
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

func uniqueID() string {
	return fmt.Sprintf("%08x-0000-0000-0000-000000000000", time.Now().UnixNano()&0xFFFFFFFF)
}

func insertReadModel(t *testing.T, pool *pgxpool.Pool, messageID, partnerID, appID, traceID, status string, terminal bool) {
	t.Helper()
	_, err := pool.Exec(context.Background(), `
		INSERT INTO messaging.message_read_model
			(message_id, partner_id, application_id, trace_id, pipeline_id, pipeline_version, current_status, terminal)
		VALUES ($1, $2, $3, $4, 'default', 'v1', $5, $6)
	`, messageID, partnerID, appID, traceID, status, terminal)
	if err != nil {
		t.Fatalf("insert message_read_model failed: %v", err)
	}
}

func TestStatusByMessageIDScopedToPartner(t *testing.T) {
	pool := testPool(t)
	defer pool.Close()
	s := NewPostgres(pool)
	ctx := context.Background()

	messageID := uniqueID()
	insertReadModel(t, pool, messageID, "acme", "app1", uniqueID(), "DELIVERED", true)

	status, err := s.StatusByMessageID(ctx, "acme", messageID)
	if err != nil {
		t.Fatalf("StatusByMessageID failed: %v", err)
	}
	if status.CurrentStatus != "DELIVERED" || !status.Terminal {
		t.Fatalf("неверные поля: %+v", status)
	}

	_, err = s.StatusByMessageID(ctx, "other-partner", messageID)
	if err == nil {
		t.Fatalf("ожидали ошибку — сообщение принадлежит другому партнёру")
	}
}

func TestStatusByTraceID(t *testing.T) {
	pool := testPool(t)
	defer pool.Close()
	s := NewPostgres(pool)
	ctx := context.Background()

	traceID := uniqueID()
	messageID := uniqueID()
	insertReadModel(t, pool, messageID, "acme", "app1", traceID, "SENT_TO_OPERATOR", false)

	status, err := s.StatusByTraceID(ctx, "acme", traceID)
	if err != nil {
		t.Fatalf("StatusByTraceID failed: %v", err)
	}
	if status.MessageID != messageID {
		t.Fatalf("message_id = %s, want %s", status.MessageID, messageID)
	}
}

func TestLifecycleHistoryRejectsForeignPartner(t *testing.T) {
	pool := testPool(t)
	defer pool.Close()
	s := NewPostgres(pool)
	ctx := context.Background()

	messageID := uniqueID()
	insertReadModel(t, pool, messageID, "acme", "app1", uniqueID(), "DELIVERED", true)

	_, err := s.LifecycleHistory(ctx, "other-partner", messageID)
	if err != pgx.ErrNoRows {
		t.Fatalf("ожидали pgx.ErrNoRows для чужого partner_id, получили %v", err)
	}
}

func TestLifecycleHistoryReturnsOrderedEvents(t *testing.T) {
	pool := testPool(t)
	defer pool.Close()
	s := NewPostgres(pool)
	ctx := context.Background()

	messageID := uniqueID()
	insertReadModel(t, pool, messageID, "acme", "app1", uniqueID(), "DELIVERED", true)

	now := time.Now().Truncate(time.Millisecond)
	_, err := pool.Exec(ctx, `
		INSERT INTO messaging.message_lifecycle_history (message_id, lifecycle_version, status, event_id, occurred_at, source)
		VALUES ($1, 1, 'ACCEPTED', $2, $3, 'partner-smpp-gateway'), ($1, 2, 'DELIVERED', $4, $5, 'delivery-reconciliation-service')
	`, messageID, uniqueID(), now, uniqueID(), now.Add(time.Second))
	if err != nil {
		t.Fatalf("insert lifecycle history failed: %v", err)
	}

	events, err := s.LifecycleHistory(ctx, "acme", messageID)
	if err != nil {
		t.Fatalf("LifecycleHistory failed: %v", err)
	}
	if len(events) != 2 || events[0].Status != "ACCEPTED" || events[1].Status != "DELIVERED" {
		t.Fatalf("неверный порядок/содержимое: %+v", events)
	}
}

func TestSearchFiltersByPartnerAndStatus(t *testing.T) {
	pool := testPool(t)
	defer pool.Close()
	s := NewPostgres(pool)
	ctx := context.Background()

	appID := uniqueID()
	insertReadModel(t, pool, uniqueID(), "acme", appID, uniqueID(), "DELIVERED", true)
	insertReadModel(t, pool, uniqueID(), "acme", appID, uniqueID(), "FAILED", true)
	insertReadModel(t, pool, uniqueID(), "other-partner", appID, uniqueID(), "DELIVERED", true)

	results, err := s.Search(ctx, SearchFilter{PartnerID: "acme", ApplicationID: appID, Status: "DELIVERED"})
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}
	if len(results) != 1 || results[0].CurrentStatus != "DELIVERED" || results[0].PartnerID != "acme" {
		t.Fatalf("неверный результат поиска: %+v", results)
	}
}