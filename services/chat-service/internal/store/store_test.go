package store

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// testPool — реальный локальный PostgreSQL 17 (brew), migrations/V034__chat_messages.sql
// уже применена в этой песочнице. Пропускается, если Postgres недоступен —
// тот же обход, что в остальной сессии (incident-service/store_test.go).
func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("CHAT_SERVICE_TEST_DSN")
	if dsn == "" {
		dsn = "postgres://localhost:5432/mpp"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Skipf("не удалось создать пул подключений к Postgres (%v) — пропуск, БД недоступна в этой песочнице", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Skipf("Postgres недоступен на %q (%v) — пропуск", dsn, err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// testPartnerID — уникальный partner_id на тест, чтобы тесты не видели чужие
// строки при параллельном запуске / повторных прогонах.
func testPartnerID(t *testing.T) string {
	t.Helper()
	partnerID := "test-partner-" + uuid.NewString()
	return partnerID
}

func cleanupPartner(t *testing.T, pool *pgxpool.Pool, partnerID string) {
	t.Helper()
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM support.chat_messages WHERE partner_id = $1`, partnerID)
	})
}

func TestSendMessageRoundTrip(t *testing.T) {
	pool := testPool(t)
	pg := NewPostgres(pool)
	partnerID := testPartnerID(t)
	cleanupPartner(t, pool, partnerID)

	m, err := pg.SendMessage(context.Background(), partnerID, "partner", "partner-user-1", "когда одобрят паттерн b-2381?")
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if m.ID == 0 {
		t.Fatalf("ожидали ненулевой id")
	}
	if m.PartnerID != partnerID || m.SenderType != "partner" || m.SenderID != "partner-user-1" || m.Body != "когда одобрят паттерн b-2381?" {
		t.Fatalf("неожиданный readback: %+v", m)
	}
	if m.CreatedAt.IsZero() {
		t.Fatalf("ожидали непустой created_at (DEFAULT now())")
	}
	if m.ReadAt != nil {
		t.Fatalf("свежеотправленное сообщение не должно быть прочитано: %+v", m)
	}
}

func TestListMessagesReturnsChronologicalOrderAndRespectsSince(t *testing.T) {
	pool := testPool(t)
	pg := NewPostgres(pool)
	ctx := context.Background()
	partnerID := testPartnerID(t)
	cleanupPartner(t, pool, partnerID)

	m1, err := pg.SendMessage(ctx, partnerID, "partner", "partner-user-1", "первое сообщение")
	if err != nil {
		t.Fatalf("SendMessage (1): %v", err)
	}
	time.Sleep(10 * time.Millisecond)
	m2, err := pg.SendMessage(ctx, partnerID, "admin", "a.karimov", "второе сообщение")
	if err != nil {
		t.Fatalf("SendMessage (2): %v", err)
	}

	all, err := pg.ListMessages(ctx, partnerID, nil, "admin")
	if err != nil {
		t.Fatalf("ListMessages(since=nil): %v", err)
	}
	if len(all) != 2 || all[0].ID != m1.ID || all[1].ID != m2.ID {
		t.Fatalf("ожидали [m1, m2] в хронологическом порядке, получили %+v", all)
	}

	since := m1.CreatedAt
	onlyNew, err := pg.ListMessages(ctx, partnerID, &since, "admin")
	if err != nil {
		t.Fatalf("ListMessages(since=m1.CreatedAt): %v", err)
	}
	if len(onlyNew) != 1 || onlyNew[0].ID != m2.ID {
		t.Fatalf("ожидали только m2 после since=m1.CreatedAt, получили %+v", onlyNew)
	}
}

// TestListMessagesMarksOppositeSenderMessagesRead — доказывает побочный
// эффект ListMessages (chat.proto ChatService.ListMessages docstring): читая
// тред как admin, партнёрские сообщения помечаются read_at; собственные
// (admin) сообщения — не трогаются этим же вызовом.
func TestListMessagesMarksOppositeSenderMessagesRead(t *testing.T) {
	pool := testPool(t)
	pg := NewPostgres(pool)
	ctx := context.Background()
	partnerID := testPartnerID(t)
	cleanupPartner(t, pool, partnerID)

	partnerMsg, err := pg.SendMessage(ctx, partnerID, "partner", "partner-user-1", "вопрос от партнёра")
	if err != nil {
		t.Fatalf("SendMessage (partner): %v", err)
	}
	adminMsg, err := pg.SendMessage(ctx, partnerID, "admin", "a.karimov", "ответ от админа")
	if err != nil {
		t.Fatalf("SendMessage (admin): %v", err)
	}

	// Читаем как admin -> партнёрское сообщение должно стать прочитанным,
	// админское — остаться непрочитанным (партнёр его ещё не читал).
	messages, err := pg.ListMessages(ctx, partnerID, nil, "admin")
	if err != nil {
		t.Fatalf("ListMessages(viewer=admin): %v", err)
	}
	byID := map[int64]ChatMessage{}
	for _, m := range messages {
		byID[m.ID] = m
	}
	if byID[partnerMsg.ID].ReadAt == nil {
		t.Errorf("ожидали, что партнёрское сообщение помечено read_at после чтения admin'ом")
	}
	if byID[adminMsg.ID].ReadAt != nil {
		t.Errorf("не ожидали read_at на собственном (admin) сообщении после чтения admin'ом: %+v", byID[adminMsg.ID])
	}

	// Теперь читаем как partner -> админское сообщение тоже должно стать
	// прочитанным.
	messages, err = pg.ListMessages(ctx, partnerID, nil, "partner")
	if err != nil {
		t.Fatalf("ListMessages(viewer=partner): %v", err)
	}
	for _, m := range messages {
		if m.ID == adminMsg.ID && m.ReadAt == nil {
			t.Errorf("ожидали, что админское сообщение помечено read_at после чтения партнёром")
		}
	}
}

func TestListThreadsAggregatesLastMessageAndUnreadCount(t *testing.T) {
	pool := testPool(t)
	pg := NewPostgres(pool)
	ctx := context.Background()
	partnerID := testPartnerID(t)
	cleanupPartner(t, pool, partnerID)

	if _, err := pg.SendMessage(ctx, partnerID, "partner", "partner-user-1", "первое"); err != nil {
		t.Fatalf("SendMessage (1): %v", err)
	}
	time.Sleep(10 * time.Millisecond)
	if _, err := pg.SendMessage(ctx, partnerID, "partner", "partner-user-1", "второе, ещё непрочитанное"); err != nil {
		t.Fatalf("SendMessage (2): %v", err)
	}

	threads, err := pg.ListThreads(ctx)
	if err != nil {
		t.Fatalf("ListThreads: %v", err)
	}
	var found *ChatThread
	for i := range threads {
		if threads[i].PartnerID == partnerID {
			found = &threads[i]
		}
	}
	if found == nil {
		t.Fatalf("ожидали найти тред %q среди ListThreads", partnerID)
	}
	if found.LastBody != "второе, ещё непрочитанное" {
		t.Errorf("ожидали LastBody = последнее отправленное сообщение, получили %q", found.LastBody)
	}
	if found.UnreadCount != 2 {
		t.Errorf("ожидали unread_count=2 (оба сообщения от партнёра ещё не прочитаны админом), получили %d", found.UnreadCount)
	}

	// Админ читает тред -> unread_count должен обнулиться.
	if _, err := pg.ListMessages(ctx, partnerID, nil, "admin"); err != nil {
		t.Fatalf("ListMessages(viewer=admin): %v", err)
	}
	threads, err = pg.ListThreads(ctx)
	if err != nil {
		t.Fatalf("ListThreads (после прочтения): %v", err)
	}
	found = nil
	for i := range threads {
		if threads[i].PartnerID == partnerID {
			found = &threads[i]
		}
	}
	if found == nil {
		t.Fatalf("ожидали найти тред %q среди ListThreads после прочтения", partnerID)
	}
	if found.UnreadCount != 0 {
		t.Errorf("ожидали unread_count=0 после того, как админ прочитал тред, получили %d", found.UnreadCount)
	}
}
