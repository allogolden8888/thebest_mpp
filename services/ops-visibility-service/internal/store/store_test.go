package store

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"mpp/ops-visibility-service/internal/kafkalag"
	"mpp/ops-visibility-service/internal/readyz"
)

// newTestClient — реальный локальный Redis (brew services, localhost:6379,
// без пароля — тот же локальный dev-инстанс, что использует остальной
// кодбейз для не-Docker тестов), не miniredis: задача этой фазы явно
// требует проверить хранение/TTL/ключевую схему против настоящего Redis,
// не мока. Используем выделенный номер БД (15), чтобы не задеть чужие
// данные на DB 0, и чистим за собой после каждого теста.
const testDB = 15

func newTestClient(t *testing.T, ttl time.Duration) *Client {
	t.Helper()

	addr := "localhost:6379"
	conn, err := net.DialTimeout("tcp", addr, 1*time.Second)
	if err != nil {
		t.Skipf("локальный Redis (%s) недостижим, пропускаю: %v", addr, err)
	}
	_ = conn.Close()

	rdb := redis.NewClient(&redis.Options{Addr: addr, DB: testDB})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Skipf("PING локального Redis не удался: %v", err)
	}

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		rdb.Del(ctx, KafkaLagKey, ReadyzKey)
		_ = rdb.Close()
	})

	return NewClientFromRedis(rdb, ttl)
}

func TestPingAgainstRealRedis(t *testing.T) {
	c := newTestClient(t, time.Minute)
	if err := c.Ping(context.Background()); err != nil {
		t.Errorf("Ping failed: %v", err)
	}
}

func TestReadKafkaLagReturnsNilWhenKeyMissing(t *testing.T) {
	c := newTestClient(t, time.Minute)
	snap, err := c.ReadKafkaLag(context.Background())
	if err != nil {
		t.Fatalf("ReadKafkaLag: %v", err)
	}
	if snap != nil {
		t.Errorf("ожидали nil для отсутствующего ключа, got %+v", snap)
	}
}

func TestReadReadyzReturnsNilWhenKeyMissing(t *testing.T) {
	c := newTestClient(t, time.Minute)
	snap, err := c.ReadReadyz(context.Background())
	if err != nil {
		t.Fatalf("ReadReadyz: %v", err)
	}
	if snap != nil {
		t.Errorf("ожидали nil для отсутствующего ключа, got %+v", snap)
	}
}

func TestWriteThenReadKafkaLagRoundTrips(t *testing.T) {
	c := newTestClient(t, time.Minute)
	ctx := context.Background()

	want := kafkalag.Snapshot{
		GeneratedAt:      time.Now().UTC().Truncate(time.Second),
		BootstrapServers: []string{"kafka-bootstrap.mpp.svc:9092"},
		Groups: []kafkalag.ConsumerGroupLag{
			{
				Group:    "config-cache-projector",
				State:    "Stable",
				TotalLag: 42,
				Partitions: []kafkalag.PartitionLag{
					{Topic: "config.changes", Partition: 0, CommitOffset: 100, EndOffset: 142, Lag: 42},
				},
			},
		},
	}

	if err := c.WriteKafkaLag(ctx, want); err != nil {
		t.Fatalf("WriteKafkaLag: %v", err)
	}

	got, err := c.ReadKafkaLag(ctx)
	if err != nil {
		t.Fatalf("ReadKafkaLag: %v", err)
	}
	if got == nil {
		t.Fatal("ReadKafkaLag вернул nil после успешной записи")
	}
	if !got.GeneratedAt.Equal(want.GeneratedAt) {
		t.Errorf("GeneratedAt = %v, want %v", got.GeneratedAt, want.GeneratedAt)
	}
	if len(got.Groups) != 1 || got.Groups[0].Group != "config-cache-projector" {
		t.Errorf("Groups = %+v", got.Groups)
	}
	if got.Groups[0].TotalLag != 42 {
		t.Errorf("TotalLag = %d, want 42", got.Groups[0].TotalLag)
	}
}

func TestWriteThenReadReadyzRoundTrips(t *testing.T) {
	c := newTestClient(t, time.Minute)
	ctx := context.Background()

	want := readyz.Snapshot{
		GeneratedAt: time.Now().UTC().Truncate(time.Second),
		Services: []readyz.Result{
			{Service: "iam-service", Ready: true, HTTPStatus: 200, LatencyMS: 12},
			{Service: "down-service", Ready: false, LatencyMS: 3001, Error: "context deadline exceeded"},
		},
	}

	if err := c.WriteReadyz(ctx, want); err != nil {
		t.Fatalf("WriteReadyz: %v", err)
	}

	got, err := c.ReadReadyz(ctx)
	if err != nil {
		t.Fatalf("ReadReadyz: %v", err)
	}
	if got == nil {
		t.Fatal("ReadReadyz вернул nil после успешной записи")
	}
	if len(got.Services) != 2 {
		t.Fatalf("Services = %+v", got.Services)
	}
	if got.Services[1].Error != "context deadline exceeded" {
		t.Errorf("Services[1].Error = %q", got.Services[1].Error)
	}
}

// TestSnapshotExpiresAfterTTL — сердце контракта "короткий TTL, не
// история": запись с TTL=300мс должна реально исчезнуть из Redis по
// истечении этого времени, не просто логически считаться устаревшей на
// уровне приложения.
func TestSnapshotExpiresAfterTTL(t *testing.T) {
	c := newTestClient(t, 300*time.Millisecond)
	ctx := context.Background()

	if err := c.WriteKafkaLag(ctx, kafkalag.Snapshot{GeneratedAt: time.Now().UTC()}); err != nil {
		t.Fatalf("WriteKafkaLag: %v", err)
	}

	got, err := c.ReadKafkaLag(ctx)
	if err != nil {
		t.Fatalf("ReadKafkaLag (до истечения TTL): %v", err)
	}
	if got == nil {
		t.Fatal("ключ должен существовать сразу после записи")
	}

	time.Sleep(500 * time.Millisecond)

	got, err = c.ReadKafkaLag(ctx)
	if err != nil {
		t.Fatalf("ReadKafkaLag (после истечения TTL): %v", err)
	}
	if got != nil {
		t.Errorf("ключ должен был истечь по TTL, но всё ещё присутствует: %+v", got)
	}
}

// TestWriteKafkaLagOverwritesPreviousSnapshotAtomically — "нет истории":
// второй Write должен полностью заменить первый (не смёржить/накопить), в
// одном ключе живёт только последний цикл опроса.
func TestWriteKafkaLagOverwritesPreviousSnapshotAtomically(t *testing.T) {
	c := newTestClient(t, time.Minute)
	ctx := context.Background()

	first := kafkalag.Snapshot{GeneratedAt: time.Now().UTC(), Groups: []kafkalag.ConsumerGroupLag{{Group: "first-cycle-group"}}}
	if err := c.WriteKafkaLag(ctx, first); err != nil {
		t.Fatalf("WriteKafkaLag (first): %v", err)
	}

	second := kafkalag.Snapshot{GeneratedAt: time.Now().UTC(), Groups: []kafkalag.ConsumerGroupLag{{Group: "second-cycle-group"}}}
	if err := c.WriteKafkaLag(ctx, second); err != nil {
		t.Fatalf("WriteKafkaLag (second): %v", err)
	}

	got, err := c.ReadKafkaLag(ctx)
	if err != nil {
		t.Fatalf("ReadKafkaLag: %v", err)
	}
	if got == nil || len(got.Groups) != 1 || got.Groups[0].Group != "second-cycle-group" {
		t.Errorf("ожидали ровно второй цикл, got %+v", got)
	}
}
