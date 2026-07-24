package pending

import (
	"context"
	"os"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	eventsv1 "mpp/platformcontracts/events/v1"
)

// TestSaveGetRoundTripsAgainstRealRedis — реальный round-trip против
// локального Redis (brew, `redis-server`, PONG проверен вручную перед
// написанием этого теста). Пропускается, если Redis недоступен — тот же
// принцип, что для PostgreSQL-тестов в этой сессии.
func TestSaveGetRoundTripsAgainstRealRedis(t *testing.T) {
	url := os.Getenv("DLR_MANAGER_TEST_REDIS_URL")
	if url == "" {
		url = "redis://localhost:6379/0"
	}

	store, err := NewStore(url)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	defer store.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := store.client.Ping(ctx).Err(); err != nil {
		t.Skipf("Redis недоступен на %q (%v) — пропуск", url, err)
	}

	eventID := "test-event-" + time.Now().Format("20060102150405.000000000")
	dlr := &eventsv1.OperatorDlr{
		OperatorId:    "beeline",
		SmscMessageId: "smsc-123",
		SegmentId:     1,
		RawStatus:     "DELIVRD",
		ReceivedAt:    timestamppb.New(time.Now().Truncate(time.Millisecond)),
	}

	if err := store.Save(ctx, eventID, dlr, time.Minute); err != nil {
		t.Fatalf("Save: %v", err)
	}

	got, found, err := store.Get(ctx, eventID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !found {
		t.Fatal("ожидали found=true сразу после Save")
	}
	if got.GetOperatorId() != dlr.GetOperatorId() || got.GetSmscMessageId() != dlr.GetSmscMessageId() {
		t.Errorf("round-trip не сохранил поля: got %+v, want %+v", got, dlr)
	}

	if err := store.Delete(ctx, eventID); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	_, found, err = store.Get(ctx, eventID)
	if err != nil {
		t.Fatalf("Get после Delete: %v", err)
	}
	if found {
		t.Fatal("ожидали found=false после Delete")
	}
}

func TestGetReturnsNotFoundForUnknownKey(t *testing.T) {
	url := os.Getenv("DLR_MANAGER_TEST_REDIS_URL")
	if url == "" {
		url = "redis://localhost:6379/0"
	}

	store, err := NewStore(url)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	defer store.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := store.client.Ping(ctx).Err(); err != nil {
		t.Skipf("Redis недоступен на %q (%v) — пропуск", url, err)
	}

	_, found, err := store.Get(ctx, "definitely-never-saved-key-xyz")
	if err != nil {
		t.Fatalf("Get для несуществующего ключа не должен возвращать ошибку: %v", err)
	}
	if found {
		t.Fatal("ожидали found=false для никогда не сохранённого ключа")
	}
}

func TestSaveRespectsTTL(t *testing.T) {
	url := os.Getenv("DLR_MANAGER_TEST_REDIS_URL")
	if url == "" {
		url = "redis://localhost:6379/0"
	}

	store, err := NewStore(url)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	defer store.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := store.client.Ping(ctx).Err(); err != nil {
		t.Skipf("Redis недоступен на %q (%v) — пропуск", url, err)
	}

	eventID := "test-ttl-event-" + time.Now().Format("20060102150405.000000000")
	dlr := &eventsv1.OperatorDlr{OperatorId: "beeline", SmscMessageId: "x", RawStatus: "DELIVRD"}

	if err := store.Save(ctx, eventID, dlr, 50*time.Millisecond); err != nil {
		t.Fatalf("Save: %v", err)
	}
	time.Sleep(150 * time.Millisecond)

	_, found, err := store.Get(ctx, eventID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if found {
		t.Fatal("ожидали, что запись истечёт по TTL")
	}
}
