// Package kafkaio — publish_config_change (service_internal_methods.md
// §3.3): чистая сборка ConfigChangeEvent + публикация в config.changes
// (compacted), platform-contracts/events/config_and_control.proto.
package kafkaio

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "mpp/platformcontracts/common/v1"
	eventsv1 "mpp/platformcontracts/events/v1"

	"mpp/config-event-publisher/internal/outbox"
)

const Topic = "config.changes"

// entityTypeToProto — те же значения, что config.config_versions.entity_type
// CHECK (migrations/V002), сверено с ConfigEntityType (proto).
func entityTypeToProto(s string) commonv1.ConfigEntityType {
	switch s {
	case "pipeline":
		return commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_PIPELINE
	case "policy_ruleset":
		return commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_POLICY_RULESET
	case "policy_template":
		return commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_POLICY_TEMPLATE
	case "billing_tariff":
		return commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_BILLING_TARIFF
	case "routing_table":
		return commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_ROUTING_TABLE
	case "number_range":
		return commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_NUMBER_RANGE
	case "partner":
		return commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_PARTNER
	case "operator":
		return commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_OPERATOR
	case "subscriber_consent":
		return commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_SUBSCRIBER_CONSENT
	// Реальная находка живой проверки Экрана 36 "Pattern Placeholders" (не
	// гипотетическая): этот switch не обновлялся, когда platform-contracts/
	// common/enums.proto обзавёлся CATEGORY(10)/CTN(11)/PATTERN_PLACEHOLDER(12)/
	// GUIDE(13) — вендоренный internal/proto/gen/.../enums.pb.go в ЭТОМ
	// сервисе тоже отстал (не содержал этих значений вообще, скопирован
	// свежий из configuration-service тем же коммитом). Итог: ЛЮБАЯ запись
	// этих четырёх entity_type в config_outbox проваливала
	// entityTypeToProto -> UNSPECIFIED -> "неизвестный entity_type" ->
	// 10 попыток -> запись НАВСЕГДА выпадает из PollOutbox (DefaultMaxAttempts)
	// — то есть ни один из этих entity_type НИКОГДА не долетал до
	// config.changes ни в одном окружении, где развёрнут ИМЕННО этот
	// собранный образ, независимо от того, что configuration-service/
	// backoffice-api полностью корректно валидировали и сохраняли записи в
	// config.config_versions. Обнаружено только потому, что hot-reload
	// Pattern Placeholder в policy-service (см. template_matching.rs/
	// config_reload.rs) специально проверялся живьём через реальный
	// config.changes, а не только через config_versions/Postgres.
	case "category":
		return commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_CATEGORY
	case "ctn":
		return commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_CTN
	case "pattern_placeholder":
		return commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_PATTERN_PLACEHOLDER
	case "guide":
		return commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_GUIDE
	default:
		return commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_UNSPECIFIED
	}
}

// ResolveStatus — CODE_REVIEW.md Critical, compliance-sensitive finding:
// старый код резолвил status через SQL `COALESCE(cv.status, 'active')`
// (internal/outbox/outbox.go), но config_version_id — и потому cv.status —
// всегда NULL для policy_template/subscriber_consent (V003 комментарий,
// это entity_type, у которых собственные исходные таблицы, не
// config_versions) — так что status для ОБОИХ навсегда резолвился в
// "active", и subscriber_consent revocation (status=archived) не мог
// опубликоваться как archived НИКОГДА.
//
// Оба entity_type без config_versions строки теперь разрешаются одинаково —
// status читается из самого payload_json:
//
//   - policy_template: config_schemas/policy_template.schema.json требует
//     поле "status" прямо в payload_json — читаем оттуда.
//   - subscriber_consent: ДОБАВЛЕНО (Фаза 6 плана закрытия API-пробелов,
//     compliance-api) — config_schemas/subscriber_consent.schema.json
//     теперь тоже требует "status". Раньше это поле отсутствовало
//     намеренно ("append/delete... payload физически не может закодировать
//     revocation"), из-за чего revocation был структурно недостижим через
//     весь пайплайн — это и был "новый контракт", которого раньше не было
//     ни у одного вызывающего (ничто в репозитории не писало
//     subscriber_consent outbox-строки в принципе); compliance-api —
//     первый реальный производитель этих строк, и он этот контракт
//     соблюдает.
func ResolveStatus(e outbox.Entry) (status string, unresolved bool, err error) {
	if e.Status != "" {
		return e.Status, false, nil
	}
	switch e.EntityType {
	case "policy_template", "subscriber_consent":
		var p struct {
			Status string `json:"status"`
		}
		if unmarshalErr := json.Unmarshal(e.Payload, &p); unmarshalErr != nil {
			return "", false, fmt.Errorf("%s outbox id=%d: payload_json не парсится: %w", e.EntityType, e.ID, unmarshalErr)
		}
		if p.Status != "active" && p.Status != "archived" {
			return "", false, fmt.Errorf("%s outbox id=%d: payload_json.status=%q — ожидали active/archived", e.EntityType, e.ID, p.Status)
		}
		return p.Status, false, nil
	default:
		// Любой будущий entity_type без config_versions-строки и без
		// status в payload_json — тот же fail-open-в-active-но-громко
		// паттерн, что раньше применялся к обоим случаям выше.
		return "active", true, nil
	}
}

// BuildConfigChangeEvent — чистая функция, без сети.
func BuildConfigChangeEvent(e outbox.Entry) (*eventsv1.ConfigChangeEvent, error) {
	entityType := entityTypeToProto(e.EntityType)
	if entityType == commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_UNSPECIFIED {
		return nil, fmt.Errorf("неизвестный entity_type %q для outbox id=%d", e.EntityType, e.ID)
	}

	status, _, err := ResolveStatus(e)
	if err != nil {
		return nil, err
	}

	return &eventsv1.ConfigChangeEvent{
		EntityType:  entityType,
		EntityId:    e.EntityID,
		Version:     e.Version,
		PayloadJson: e.Payload,
		Status:      status,
		CreatedAt:   timestamppb.New(e.CreatedAt),
	}, nil
}

type Publisher struct {
	client *kgo.Client
}

func NewPublisher(brokers []string) (*Publisher, error) {
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return nil, fmt.Errorf("kgo.NewClient: %w", err)
	}
	return &Publisher{client: client}, nil
}

func (p *Publisher) Close() { p.client.Close() }

// Ping — /readyz dependency check (CODE_REVIEW.md: "/readyz never reflects
// real downstream health after startup").
func (p *Publisher) Ping(ctx context.Context) error { return p.client.Ping(ctx) }

// Publish — ключ = entity_id, compacted-топик по (entity_type, entity_id)
// был бы точнее, но mpp.events.v1.ConfigChangeEvent не даёт составного
// ключа на уровне Kafka message key без доп. соглашения — используется
// entity_id (в проде разные entity_type должны использовать разные
// value/namespace entity_id, иначе возможна коллизия compaction между,
// например, partner "acme" и operator "acme" — см. README "Открытый вопрос").
func (p *Publisher) Publish(ctx context.Context, e outbox.Entry) error {
	event, err := BuildConfigChangeEvent(e)
	if err != nil {
		return err
	}
	payload, err := proto.Marshal(event)
	if err != nil {
		return fmt.Errorf("proto.Marshal(ConfigChangeEvent): %w", err)
	}
	record := &kgo.Record{Topic: Topic, Key: []byte(e.EntityID), Value: payload}
	return p.client.ProduceSync(ctx, record).FirstErr()
}
