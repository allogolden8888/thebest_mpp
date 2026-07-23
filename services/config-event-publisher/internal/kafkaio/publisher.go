// Package kafkaio — publish_config_change (service_internal_methods.md
// §3.3): чистая сборка ConfigChangeEvent + публикация в config.changes
// (compacted), platform-contracts/events/config_and_control.proto.
package kafkaio

import (
	"context"
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
	default:
		return commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_UNSPECIFIED
	}
}

// BuildConfigChangeEvent — чистая функция, без сети.
func BuildConfigChangeEvent(e outbox.Entry) (*eventsv1.ConfigChangeEvent, error) {
	entityType := entityTypeToProto(e.EntityType)
	if entityType == commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_UNSPECIFIED {
		return nil, fmt.Errorf("неизвестный entity_type %q для outbox id=%d", e.EntityType, e.ID)
	}

	return &eventsv1.ConfigChangeEvent{
		EntityType:  entityType,
		EntityId:    e.EntityID,
		Version:     e.Version,
		PayloadJson: e.Payload,
		Status:      e.Status,
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