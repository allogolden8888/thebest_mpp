// Package registry — register_route + heartbeat_tick (service_internal_methods.md
// §1.3a) в Runtime Redis: operator_route:{operator_id}:{route_id} HASH,
// общий с Operator SMPP Session Manager (data_infrastructure_spec.md §2.1,
// HLD §11.4) — protocol=HTTP здесь, protocol=SMPP там, единый паттерн владения.
package registry

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

type Client struct {
	rdb              *redis.Client
	owningInstanceID string
	heartbeatTTL     time.Duration
}

func NewClient(addr, password, owningInstanceID string, heartbeatTTL time.Duration) *Client {
	return &Client{
		rdb:              redis.NewClient(&redis.Options{Addr: addr, Password: password}),
		owningInstanceID: owningInstanceID,
		heartbeatTTL:     heartbeatTTL,
	}
}

func NewClientFromRedis(rdb *redis.Client, owningInstanceID string, heartbeatTTL time.Duration) *Client {
	return &Client{rdb: rdb, owningInstanceID: owningInstanceID, heartbeatTTL: heartbeatTTL}
}

func (c *Client) Close() error { return c.rdb.Close() }

// Ping — используется /readyz (CODE_REVIEW.md Low finding: раньше
// неудачная register_route на старте только логировалась, /readyz был
// безусловным 200 независимо от реального состояния Redis).
func (c *Client) Ping(ctx context.Context) error {
	return c.rdb.Ping(ctx).Err()
}

func key(operatorID, routeID string) string {
	return fmt.Sprintf("operator_route:%s:%s", operatorID, routeID)
}

func (c *Client) Register(ctx context.Context, operatorID, routeID string, routeEpoch int64, endpoint string) error {
	k := key(operatorID, routeID)
	err := c.rdb.HSet(ctx, k, map[string]any{
		"protocol":           "HTTP",
		"owning_instance_id": c.owningInstanceID,
		"endpoint":           endpoint,
		"route_epoch":        strconv.FormatInt(routeEpoch, 10),
		"heartbeat":          strconv.FormatInt(time.Now().UnixMilli(), 10),
	}).Err()
	if err != nil {
		return fmt.Errorf("HSET %s: %w", k, err)
	}
	return c.rdb.Expire(ctx, k, c.heartbeatTTL*3).Err()
}

func (c *Client) Heartbeat(ctx context.Context, operatorID, routeID string) error {
	k := key(operatorID, routeID)
	if err := c.rdb.HSet(ctx, k, "heartbeat", strconv.FormatInt(time.Now().UnixMilli(), 10)).Err(); err != nil {
		return fmt.Errorf("HSET heartbeat %s: %w", k, err)
	}
	return c.rdb.Expire(ctx, k, c.heartbeatTTL*3).Err()
}

func (c *Client) Unregister(ctx context.Context, operatorID, routeID string) error {
	return c.rdb.Del(ctx, key(operatorID, routeID)).Err()
}

func (c *Client) Lookup(ctx context.Context, operatorID, routeID string) (map[string]string, error) {
	return c.rdb.HGetAll(ctx, key(operatorID, routeID)).Result()
}