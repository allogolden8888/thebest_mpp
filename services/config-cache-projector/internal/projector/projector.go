// Package projector — on_config_change + write_projection
// (service_internal_methods.md §3.4), в Configuration Redis
// (data_infrastructure_spec.md §2.2):
//
//	config:current:{entity_type}:{entity_id}          STRING  номер активной версии
//	config:version:{entity_type}:{entity_id}:{version} STRING  сериализованный payload
//	config:sender:{sender_id}                          STRING  partner_id владельца отправителя
//	                                                            (только entity_type=partner, см.
//	                                                            "Реестр отправителей" в luminous-
//	                                                            hugging-charm.md, Фаза 2)
package projector

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

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

func senderOwnerKey(senderID string) string {
	return fmt.Sprintf("config:sender:%s", senderID)
}

// parseSenderOwners — best-effort разбор senders[] из partner-payload
// (config_schemas/partner.schema.json) для проекции
// config:sender:{sender_id} -> partner_id.
//
// partner_id берётся из самого payload (top-level поле, обязательное по
// схеме), а НЕ из event.GetEntityId(): проверено —
// configuration-service/internal/grpcserver/server.go (CreateVersion) и
// internal/validate/semantic.go нигде не сверяют entity_id с
// payload["partner_id"]; semanticChecks вообще не регистрирует проверку
// для EntityPartner. Полагаться на entity_id как на partner_id было бы
// недокументированным допущением, которое ничего в этом сервисе не
// гарантирует — payload несёт свой собственный partner_id, он и есть
// источник истины для владения sender_id.
//
// Намеренно не возвращает ошибку наверх: если payload_json не парсится
// как JSON, или senders отсутствует/пуст, WriteProjection всё равно
// обязан записать config:current/config:version для самого partner-
// объекта — здесь только логируется и пропускается sender-проекция.
//
// Фаза 2 плана (luminous-hugging-charm.md, "Реестр отправителей") —
// осознанное упрощение: если sender_id убран из senders[] в более новой
// версии, либо его status стал archived, старая запись
// config:sender:{id} НЕ удаляется/не помечается. Владение (какому
// партнёру принадлежит sender_id) не меняется при архивации — меняется
// только его активность; настоящий remove потребовал бы диффа старого и
// нового payload, что осознанно оставлено вне скоупа этого прохода.
func parseSenderOwners(payloadJSON []byte, entityID string) map[string]string {
	var payload struct {
		PartnerID string `json:"partner_id"`
		Senders   []struct {
			SenderID string `json:"sender_id"`
		} `json:"senders"`
	}
	if err := json.Unmarshal(payloadJSON, &payload); err != nil {
		log.Printf("write_projection: payload_json не парсится как JSON для sender-реестра (entity_id=%s): %v — config:sender:* пропущен, config:current/config:version не затронуты", entityID, err)
		return nil
	}
	if len(payload.Senders) == 0 {
		return nil
	}
	if payload.PartnerID == "" {
		log.Printf("write_projection: senders[] непуст, но partner_id пуст в payload (entity_id=%s) — config:sender:* пропущен для этого события", entityID)
		return nil
	}
	owners := make(map[string]string, len(payload.Senders))
	for _, s := range payload.Senders {
		if s.SenderID == "" {
			continue
		}
		owners[s.SenderID] = payload.PartnerID
	}
	if len(owners) == 0 {
		return nil
	}
	return owners
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

	// Ф2 плана (luminous-hugging-charm.md, "Реестр отправителей"): для
	// entity_type=partner + status=active (то же гейтирование, что и у
	// config:current — архивная/superseded версия не должна затирать
	// живой реестр отправителей устаревшими данными) дополнительно
	// проецируем sender_id -> partner_id. Разбор payload вынесен ДО
	// TxPipelined: ошибка парсинга не должна мешать основной транзакции
	// записи config:current/config:version (см. parseSenderOwners).
	var senderOwners map[string]string
	if entityType == "partner" && event.GetStatus() == "active" {
		senderOwners = parseSenderOwners(event.GetPayloadJson(), entityID)
	}

	_, err := c.rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Set(ctx, versionKey(entityType, entityID, version), event.GetPayloadJson(), 0)
		if event.GetStatus() == "active" {
			pipe.Set(ctx, currentKey(entityType, entityID), version, 0)
		}
		for senderID, partnerID := range senderOwners {
			pipe.Set(ctx, senderOwnerKey(senderID), partnerID, 0)
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