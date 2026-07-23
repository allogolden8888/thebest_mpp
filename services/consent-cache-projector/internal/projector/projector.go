// Package projector — Consent Cache Projector (data_infrastructure_spec.md
// §1.9c, симметричен Config Cache Projector, но проецирует в Runtime Redis,
// не Configuration Redis — Policy читает consent-данные на каждое
// сообщение, hot path, не bootstrap):
//
//	consent:category_blacklist:{msisdn}  SET  заблокированные категории
//	consent:sender_blacklist:{msisdn}    SET  заблокированные sender_id
//
// entity_type=subscriber_consent — append/delete таблица по PRIMARY KEY
// (msisdn, scope_type, scope_value, channel), не version-based
// (config_schemas/subscriber_consent.schema.json) — ConfigChangeEvent.status
// здесь означает "active"=добавить в блэклист, "archived"=убрать
// (opt-out отозван).
package projector

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/redis/go-redis/v9"

	eventsv1 "mpp/platformcontracts/events/v1"
)

// ConsentPayload — форма payload_json, config_schemas/subscriber_consent.schema.json.
type ConsentPayload struct {
	MSISDN     string `json:"msisdn"`
	ScopeType  string `json:"scope_type"` // "CATEGORY" | "SENDER"
	ScopeValue string `json:"scope_value"`
	Channel    string `json:"channel"`
}

// ParsePayload — чистая функция разбора payload_json.
func ParsePayload(payloadJSON []byte) (ConsentPayload, error) {
	var p ConsentPayload
	if err := json.Unmarshal(payloadJSON, &p); err != nil {
		return ConsentPayload{}, fmt.Errorf("unmarshal subscriber_consent payload: %w", err)
	}
	if p.MSISDN == "" {
		return ConsentPayload{}, fmt.Errorf("subscriber_consent payload без msisdn")
	}
	if p.ScopeType != "CATEGORY" && p.ScopeType != "SENDER" {
		return ConsentPayload{}, fmt.Errorf("неизвестный scope_type %q", p.ScopeType)
	}
	return p, nil
}

func blacklistKey(p ConsentPayload) string {
	if p.ScopeType == "CATEGORY" {
		return "consent:category_blacklist:" + p.MSISDN
	}
	return "consent:sender_blacklist:" + p.MSISDN
}

type Client struct {
	rdb *redis.Client
}

func NewClient(addr, password string) *Client {
	return &Client{rdb: redis.NewClient(&redis.Options{Addr: addr, Password: password})}
}

func NewClientFromRedis(rdb *redis.Client) *Client {
	return &Client{rdb: rdb}
}

func (c *Client) Close() error { return c.rdb.Close() }

// ApplyConsentChange — on_config_change для entity_type=subscriber_consent:
// status="active" -> SADD (opt-out зарегистрирован); status="archived" ->
// SREM (opt-out отозван).
func (c *Client) ApplyConsentChange(ctx context.Context, event *eventsv1.ConfigChangeEvent) error {
	payload, err := ParsePayload(event.GetPayloadJson())
	if err != nil {
		return err
	}
	key := blacklistKey(payload)

	if event.GetStatus() == "archived" {
		return c.rdb.SRem(ctx, key, payload.ScopeValue).Err()
	}
	return c.rdb.SAdd(ctx, key, payload.ScopeValue).Err()
}

// IsBlacklisted — read-путь для тестов (то же, что Policy::check_category_blacklist/
// check_sender_blacklist делали бы).
func (c *Client) IsBlacklisted(ctx context.Context, scopeType, msisdn, scopeValue string) (bool, error) {
	key := blacklistKey(ConsentPayload{ScopeType: scopeType, MSISDN: msisdn})
	return c.rdb.SIsMember(ctx, key, scopeValue).Result()
}
