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

// Ping — /readyz dependency check (CODE_REVIEW.md: "/readyz never
// reflects real downstream health after startup").
func (c *Client) Ping(ctx context.Context) error { return c.rdb.Ping(ctx).Err() }

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
//
// CODE_REVIEW.md finding #2: entityTypeString возвращает "unspecified" по
// умолчанию для незнакомых значений — раньше это тихо принималось и
// писало данные под бракованным ключом config:version:unspecified:....
// kafkaio.DecodeConfigChangeEvent теперь отклоняет такие события ДО
// вызова WriteProjection, но здесь оставлена та же проверка defense in
// depth — WriteProjection не должен быть единственной линией защиты, но
// и не должен молча доверять вызывающей стороне.
//
// CODE_REVIEW.md finding #3: раньше это были два независимых SET без
// пайплайна/транзакции — если первый (version) успевал, а второй
// (current) падал по сети, читатели видели новую версию в истории, но
// старый current-указатель, и (из-за cross-cutting автокоммит-бага,
// отдельно исправленного) это скорее всего никогда не переигрывалось.
// Теперь оба SET идут через TxPipelined (MULTI/EXEC) — Redis применяет их
// атомарно; либо оба применились, либо (при сетевой ошибке до EXEC) ни
// один — никакого частично применённого состояния.
func (c *Client) WriteProjection(ctx context.Context, event *eventsv1.ConfigChangeEvent) error {
	entityType := entityTypeString(event.GetEntityType())
	if entityType == "unspecified" {
		return fmt.Errorf("write_projection: неизвестный/unset entity_type %v для entity_id=%q — отклонено", event.GetEntityType(), event.GetEntityId())
	}
	entityID := event.GetEntityId()
	version := event.GetVersion()

	_, err := c.rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Set(ctx, versionKey(entityType, entityID, version), event.GetPayloadJson(), 0)
		if event.GetStatus() == "active" {
			pipe.Set(ctx, currentKey(entityType, entityID), version, 0)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("write_projection tx pipeline (entity_type=%s entity_id=%s version=%d): %w", entityType, entityID, version, err)
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