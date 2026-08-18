// Package store — хранение снапшотов (kafka lag, readyz) в Redis с
// коротким TTL, без истории (Фаза 8 плана явно исключает таблицу трендов
// из MVP — см. README.md сервиса).
//
// Ключевая схема (см. также README.md — этот файл единственный владелец
// правды об именах ключей, README документирует то же самое для внешних
// читателей, не завязанных на этот пакет):
//
//	ops:kafka-lag:snapshot   STRING (JSON kafkalag.Snapshot)  TTL
//	ops:readyz:snapshot      STRING (JSON readyz.Snapshot)    TTL
//
// Один ключ на категорию, не один ключ на группу/сервис — при ~15 consumer
// group и ~34 сервисах кардинальность низкая, а один JSON-блоб на цикл
// опроса даёт атомарность снапшота целиком (все группы/сервисы в одном
// GET/SET относятся к одному и тому же циклу опроса, не к разным) и не
// требует отдельного индекса имён (SET из имён), который бы сам протух
// независимо от TTL отдельных записей и требовал бы отдельной чистки.
// Compare: config-cache-projector — там per-entity ключи оправданы, потому
// что entity_id пишутся по одному, событие за событием, а не пачкой раз в
// N секунд.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"mpp/ops-visibility-service/internal/kafkalag"
	"mpp/ops-visibility-service/internal/readyz"
)

const (
	// KafkaLagKey — снапшот lag всех consumer group на момент последнего
	// успешного цикла опроса Kafka.
	KafkaLagKey = "ops:kafka-lag:snapshot"
	// ReadyzKey — снапшот /readyz-грида всех сервисов на момент последнего
	// цикла опроса.
	ReadyzKey = "ops:readyz:snapshot"
)

type Client struct {
	rdb *redis.Client
	ttl time.Duration
}

func NewClient(addr, password string, db int, ttl time.Duration) *Client {
	return &Client{
		rdb: redis.NewClient(&redis.Options{Addr: addr, Password: password, DB: db}),
		ttl: ttl,
	}
}

func NewClientFromRedis(rdb *redis.Client, ttl time.Duration) *Client {
	return &Client{rdb: rdb, ttl: ttl}
}

func (c *Client) Close() error { return c.rdb.Close() }

// Ping — /readyz dependency check.
func (c *Client) Ping(ctx context.Context) error { return c.rdb.Ping(ctx).Err() }

func (c *Client) WriteKafkaLag(ctx context.Context, snap kafkalag.Snapshot) error {
	return c.writeJSON(ctx, KafkaLagKey, snap)
}

// ReadKafkaLag возвращает (nil, nil), если ключ отсутствует/протух —
// вызывающая сторона (httpapi.SnapshotHandler) должна отличать "данных
// пока/уже нет" от реальной ошибки Redis.
func (c *Client) ReadKafkaLag(ctx context.Context) (*kafkalag.Snapshot, error) {
	var snap kafkalag.Snapshot
	ok, err := c.readJSON(ctx, KafkaLagKey, &snap)
	if err != nil || !ok {
		return nil, err
	}
	return &snap, nil
}

func (c *Client) WriteReadyz(ctx context.Context, snap readyz.Snapshot) error {
	return c.writeJSON(ctx, ReadyzKey, snap)
}

func (c *Client) ReadReadyz(ctx context.Context) (*readyz.Snapshot, error) {
	var snap readyz.Snapshot
	ok, err := c.readJSON(ctx, ReadyzKey, &snap)
	if err != nil || !ok {
		return nil, err
	}
	return &snap, nil
}

func (c *Client) writeJSON(ctx context.Context, key string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshal %s: %w", key, err)
	}
	if err := c.rdb.Set(ctx, key, b, c.ttl).Err(); err != nil {
		return fmt.Errorf("SET %s: %w", key, err)
	}
	return nil
}

func (c *Client) readJSON(ctx context.Context, key string, v any) (bool, error) {
	b, err := c.rdb.Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("GET %s: %w", key, err)
	}
	if err := json.Unmarshal(b, v); err != nil {
		return false, fmt.Errorf("unmarshal %s: %w", key, err)
	}
	return true, nil
}
