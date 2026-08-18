// Package redisio — читает те же ключи Runtime Redis, что пишет
// consent-cache-projector (services/consent-cache-projector/internal/projector/projector.go):
//
//	consent:category_blacklist:{msisdn}  SET  заблокированные категории
//	consent:sender_blacklist:{msisdn}    SET  заблокированные sender_id
//
// Только чтение — запись остаётся исключительно через
// config.changes -> consent-cache-projector (см. internal/configclient),
// этот клиент никогда не пишет в эти ключи напрямую, чтобы не разойтись с
// единственным местом, которое формально владеет записью туда.
package redisio

import (
	"context"

	"github.com/redis/go-redis/v9"
)

type Client struct {
	rdb *redis.Client
}

func NewClient(addr, password string) *Client {
	return &Client{rdb: redis.NewClient(&redis.Options{Addr: addr, Password: password})}
}

func (c *Client) Close() error { return c.rdb.Close() }

func (c *Client) Ping(ctx context.Context) error { return c.rdb.Ping(ctx).Err() }

func categoryBlacklistKey(msisdn string) string { return "consent:category_blacklist:" + msisdn }
func senderBlacklistKey(msisdn string) string   { return "consent:sender_blacklist:" + msisdn }

// ConsentStatus — снимок текущего состояния блэклистов для одного MSISDN.
type ConsentStatus struct {
	MSISDN            string   `json:"msisdn"`
	BlockedCategories []string `json:"blocked_categories"`
	BlockedSenders    []string `json:"blocked_senders"`
}

// Lookup — SMEMBERS обоих множеств для msisdn. Пустые слайсы (не ошибка),
// если ничего не заблокировано — отсутствие блэклиста для MSISDN является
// штатным, самым частым случаем, не исключительным.
func (c *Client) Lookup(ctx context.Context, msisdn string) (ConsentStatus, error) {
	categories, err := c.rdb.SMembers(ctx, categoryBlacklistKey(msisdn)).Result()
	if err != nil {
		return ConsentStatus{}, err
	}
	senders, err := c.rdb.SMembers(ctx, senderBlacklistKey(msisdn)).Result()
	if err != nil {
		return ConsentStatus{}, err
	}
	return ConsentStatus{MSISDN: msisdn, BlockedCategories: categories, BlockedSenders: senders}, nil
}
