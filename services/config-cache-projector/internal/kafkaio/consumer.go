// Package kafkaio — on_config_change (service_internal_methods.md §3.4):
// консьюмер config.changes (compacted), декодирует ConfigChangeEvent.
package kafkaio

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	commonv1 "mpp/platformcontracts/common/v1"
	eventsv1 "mpp/platformcontracts/events/v1"
)

const Topic = "config.changes"

// validConfigEntityTypes — всё, что Config Cache Projector реально
// проецирует в Configuration Redis. Явный allow-list вместо "всё, что не
// пустая строка" — CODE_REVIEW.md finding: malformed/unset entity_type
// раньше молча принимался и проецировался под бракованным ключом
// (projector.entityTypeString's "default: unspecified" fallback).
// SUBSCRIBER_CONSENT намеренно исключён — это не наш проекционный путь
// (Consent Cache Projector, Runtime Redis, hot path), см.
// DecodeConfigChangeEvent.
var validConfigEntityTypes = map[commonv1.ConfigEntityType]bool{
	commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_PIPELINE:        true,
	commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_POLICY_RULESET:  true,
	commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_POLICY_TEMPLATE: true,
	commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_BILLING_TARIFF:  true,
	commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_ROUTING_TABLE:   true,
	commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_NUMBER_RANGE:    true,
	commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_PARTNER:         true,
	commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_OPERATOR:        true,
}

// DecodeConfigChangeEvent — чистая функция, разбор одного сообщения.
//
// CODE_REVIEW.md findings (config-cache-projector):
//  1. "subscriber_consent events aren't filtered out and get written into
//     Configuration Redis" — data_infrastructure_spec.md §1.9c требует
//     проекции consent-данных в Runtime Redis через Consent Cache
//     Projector, не сюда (bootstrap-only Configuration Redis не
//     рассчитана на per-subscriber объём и другие retention/access
//     ожидания). Теперь явно пропускаем (nil, nil), симметрично тому, как
//     Consent Cache Projector пропускает все остальные entity_type.
//  2. "malformed/unset entity_type is silently accepted and projected
//     under a bogus 'unspecified' key" — теперь явно отклоняем (error)
//     всё, что не входит в validConfigEntityTypes, вместо того, чтобы
//     полагаться на projector.entityTypeString's "default: unspecified"
//     как единственный guard.
func DecodeConfigChangeEvent(payload []byte) (*eventsv1.ConfigChangeEvent, error) {
	var event eventsv1.ConfigChangeEvent
	if err := proto.Unmarshal(payload, &event); err != nil {
		return nil, fmt.Errorf("unmarshal ConfigChangeEvent: %w", err)
	}
	if event.GetEntityId() == "" {
		return nil, fmt.Errorf("ConfigChangeEvent без entity_id")
	}
	if event.GetEntityType() == commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_SUBSCRIBER_CONSENT {
		return nil, nil // не наш путь — Consent Cache Projector
	}
	if !validConfigEntityTypes[event.GetEntityType()] {
		return nil, fmt.Errorf("ConfigChangeEvent entity_id=%q: неизвестный/unset entity_type %v — отклонено вместо проекции под бракованным ключом", event.GetEntityId(), event.GetEntityType())
	}
	return &event, nil
}

// Consumer — реальный franz-go консьюмер config.changes. Не проверялся
// против живого брокера в этой песочнице.
//
// CODE_REVIEW.md cross-cutting finding — "Kafka autocommit is decoupled
// from processing success": раньше NewConsumer не передавал
// kgo.DisableAutoCommit(), так что offset коммитился каждые 5с franz-go's
// дефолтным таймером НЕЗАВИСИМО от того, успел ли WriteProjection
// отработать. Теперь автокоммит выключен, и Run() коммитит offset только
// после успешной обработки (см. processRecords).
type Consumer struct {
	client *kgo.Client
}

func NewConsumer(brokers []string, groupID string) (*Consumer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeTopics(Topic),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return nil, fmt.Errorf("kgo.NewClient: %w", err)
	}
	return &Consumer{client: client}, nil
}

func (c *Consumer) Close() { c.client.Close() }

// Ping — /readyz dependency check.
func (c *Consumer) Ping(ctx context.Context) error { return c.client.Ping(ctx) }

// processRecords — чистая функция оркестрации consume-цикла, вынесена из
// Run() специально ради юнит-тестируемости (CODE_REVIEW.md test-quality
// note: "no test exercises Consumer.Run — only the pure
// DecodeConfigChangeEvent function is tested... untestable-in-principle
// against the current suite"). Обрабатывает записи в порядке offset
// per-partition:
//   - decode-ошибка (poison message) — пропускается и коммитится (не
//     блокирует партицию навечно на заведомо неисправимом сообщении);
//   - handle() возвращает ошибку (retriable, напр. Redis timeout) —
//     останавливает обработку ЭТОЙ партиции на этом fetch, не коммитит
//     эту и последующие записи партиции в этом fetch (иначе offset
//     коммитился бы мимо необработанного сообщения — тот самый
//     cross-cutting баг, который эта функция должна устранить);
//   - handle() успешен — запись попадает в возвращаемый toCommit.
//
// Разные партиции обрабатываются независимо — retriable-ошибка в одной
// партиции не блокирует прогресс по остальным.
func processRecords(records []*kgo.Record, handle func(*eventsv1.ConfigChangeEvent) error, onPoison func(*kgo.Record, error), onRetry func(*kgo.Record, error)) []*kgo.Record {
	byPartition := make(map[int32][]*kgo.Record)
	var order []int32
	for _, r := range records {
		if _, seen := byPartition[r.Partition]; !seen {
			order = append(order, r.Partition)
		}
		byPartition[r.Partition] = append(byPartition[r.Partition], r)
	}

	var toCommit []*kgo.Record
	for _, part := range order {
		for _, r := range byPartition[part] {
			event, err := DecodeConfigChangeEvent(r.Value)
			if err != nil {
				if onPoison != nil {
					onPoison(r, err)
				}
				toCommit = append(toCommit, r)
				continue
			}
			if event == nil {
				// Другой entity_type (subscriber_consent) — не ошибка,
				// просто не наш путь, коммитим и идём дальше.
				toCommit = append(toCommit, r)
				continue
			}
			if err := handle(event); err != nil {
				if onRetry != nil {
					onRetry(r, err)
				}
				break // не коммитим эту и последующие записи ЭТОЙ партиции
			}
			toCommit = append(toCommit, r)
		}
	}
	return toCommit
}

// Run — цикл потребления; handler вызывается на каждое успешно
// декодированное событие интересующего нас entity_type и должен вернуть
// ошибку, если обработка (write_projection) не удалась — offset для этой
// записи (и любых последующих в той же партиции этого fetch) тогда НЕ
// коммитится, запись будет передоставлена на следующем PollFetches.
func (c *Consumer) Run(ctx context.Context, handler func(*eventsv1.ConfigChangeEvent) error, errHandler func(error)) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		fetches := c.client.PollFetches(ctx)
		if ctx.Err() != nil {
			return
		}
		fetches.EachError(func(_ string, _ int32, err error) {
			if errHandler != nil {
				errHandler(fmt.Errorf("fetch error: %w", err))
			}
		})

		var records []*kgo.Record
		fetches.EachRecord(func(r *kgo.Record) { records = append(records, r) })
		if len(records) == 0 {
			continue
		}

		toCommit := processRecords(records, handler,
			func(r *kgo.Record, err error) {
				if errHandler != nil {
					errHandler(fmt.Errorf("poison message skipped at partition=%d offset=%d: %w", r.Partition, r.Offset, err))
				}
			},
			func(r *kgo.Record, err error) {
				if errHandler != nil {
					errHandler(fmt.Errorf("write_projection failed at partition=%d offset=%d, will retry: %w", r.Partition, r.Offset, err))
				}
			},
		)

		if len(toCommit) > 0 {
			if err := c.client.CommitRecords(ctx, toCommit...); err != nil {
				if errHandler != nil {
					errHandler(fmt.Errorf("commit offsets: %w", err))
				}
			}
		}
	}
}
