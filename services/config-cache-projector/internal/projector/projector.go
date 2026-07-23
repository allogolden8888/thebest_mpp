// Package projector — on_config_change + write_projection
// (service_internal_methods.md §3.4), в Configuration Redis
// (data_infrastructure_spec.md §2.2):
//
//	config:current:{entity_type}:{entity_id}          STRING  номер активной версии
//	config:version:{entity_type}:{entity_id}:{version} STRING  сериализованный payload
package projector

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"

	commonv1 "mpp/platformcontracts/common/v1"
	eventsv1 "mpp/platformcontracts/events/v1"
)

// entityTypeString — обратный маппинг ConfigEntityType -> то же
// lower_snake_case, что config.config_versions.entity_type
// (migrations/V002__config_versions.sql CHECK), а не сырое имя
// protobuf-константы (CONFIG_ENTITY_TYPE_PARTNER) — Redis-ключи должны
// оставаться в одном алфавите с PostgreSQL для консистентного дебага.
func entityTypeString(e commonv1.ConfigEntityType) string {
	switch e {
	case commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_PIPELINE:
		return "pipeline"
	case commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_POLICY_RULESET:
		return "policy_ruleset"
	case commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_POLICY_TEMPLATE:
		return "policy_template"
	case commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_BILLING_TARIFF:
		return "billing_tariff"
	case commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_ROUTING_TABLE:
		return "routing_table"
	case commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_NUMBER_RANGE:
		return "number_range"
	case commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_PARTNER:
		return "partner"
	case commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_OPERATOR:
		return "operator"
	case commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_SUBSCRIBER_CONSENT:
		return "subscriber_consent"
	default:
		return "unspecified"
	}
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

func currentKey(entityType, entityID string) string {
	return fmt.Sprintf("config:current:%s:%s", entityType, entityID)
}

func versionKey(entityType, entityID string, version int64) string {
	return fmt.Sprintf("config:version:%s:%s:%d", entityType, entityID, version)
}

// WriteProjection — write_projection: всегда пишет config:version:... (для
// истории/бутстрапа явно запрошенной версии), и обновляет config:current
// только для status="active" — архивная версия не должна становиться
// текущей для bootstrap hot-path сервисов (data_infrastructure_spec.md
// §2.2: "Только bootstrap/cache-miss").
func (c *Client) WriteProjection(ctx context.Context, event *eventsv1.ConfigChangeEvent) error {
	entityType := entityTypeString(event.GetEntityType())
	entityID := event.GetEntityId()
	version := event.GetVersion()

	if err := c.rdb.Set(ctx, versionKey(entityType, entityID, version), event.GetPayloadJson(), 0).Err(); err != nil {
		return fmt.Errorf("SET %s: %w", versionKey(entityType, entityID, version), err)
	}

	if event.GetStatus() == "active" {
		if err := c.rdb.Set(ctx, currentKey(entityType, entityID), version, 0).Err(); err != nil {
			return fmt.Errorf("SET %s: %w", currentKey(entityType, entityID), err)
		}
	}

	return nil
}

// CurrentVersion — вспомогательный read-путь для тестов/bootstrap.
func (c *Client) CurrentVersion(ctx context.Context, entityType, entityID string) (int64, error) {
	return c.rdb.Get(ctx, currentKey(entityType, entityID)).Int64()
}

// VersionPayload — read-путь для конкретной версии.
func (c *Client) VersionPayload(ctx context.Context, entityType, entityID string, version int64) ([]byte, error) {
	return c.rdb.Get(ctx, versionKey(entityType, entityID, version)).Bytes()
}