// Package kafkaio — консьюмер config.changes, фильтрует
// entity_type=SUBSCRIBER_CONSENT (остальные entity_type — не наш проекционный
// путь, Config Cache Projector их обрабатывает отдельно).
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

// DecodeConsentEvent — чистая функция: разбирает запись и возвращает nil
// (без ошибки), если entity_type не subscriber_consent — тот же топик
// несёт события всех entity_type, этот сервис отфильтровывает свои.
func DecodeConsentEvent(payload []byte) (*eventsv1.ConfigChangeEvent, error) {
	var event eventsv1.ConfigChangeEvent
	if err := proto.Unmarshal(payload, &event); err != nil {
		return nil, fmt.Errorf("unmarshal ConfigChangeEvent: %w", err)
	}
	if event.GetEntityType() != commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_SUBSCRIBER_CONSENT {
		return nil, nil
	}
	if event.GetEntityId() == "" {
		return nil, fmt.Errorf("subscriber_consent ConfigChangeEvent без entity_id")
	}
	return &event, nil
}

// Consumer — реальный franz-go консьюмер config.changes.
//
// CODE_REVIEW.md cross-cutting finding — "Kafka autocommit is decoupled
// from processing success": раньше NewConsumer не передавал
// kgo.DisableAutoCommit(), так что offset коммитился каждые 5с franz-go's
// дефолтным таймером НЕЗАВИСИМО от того, успел ли ApplyConsentChange
// отработать. Особенно значимо здесь — это единственный durable-backed
// (не эфемерный) Redis-кластер из трёх (data_infrastructure_spec.md §2.1),
// и Consent Cache Projector сам не реализует restore-from-changelog (см.
// README) — молча потерянный offset означал молча и навсегда потерянный
// opt-out/revocation без единого способа его переиграть. Теперь автокоммит
// выключен, и Run() коммитит offset только после успешной обработки (см.
// processRecords).
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
// note: "no test exercises the actual Kafka consume/produce loop... only
// pure decode/build helper functions are unit-tested"). Та же семантика,
// что config-cache-projector/internal/kafkaio.processRecords — держится
// намеренно в обоих сервисах одинаково, см. комментарий там:
//   - decode-ошибка (poison message) — пропускается и коммитится;
//   - handle() возвращает ошибку (retriable) — останавливает эту
//     партицию на этом fetch, не коммитит;
//   - handle() успешен, либо событие не subscriber_consent (nil, nil) —
//     коммитится.
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
			event, err := DecodeConsentEvent(r.Value)
			if err != nil {
				if onPoison != nil {
					onPoison(r, err)
				}
				toCommit = append(toCommit, r)
				continue
			}
			if event == nil {
				toCommit = append(toCommit, r) // другой entity_type, не наш
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
// декодированное subscriber_consent-событие и должен вернуть ошибку, если
// обработка (apply_consent_change) не удалась — offset для этой записи (и
// любых последующих в той же партиции этого fetch) тогда НЕ коммитится.
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
					errHandler(fmt.Errorf("apply_consent_change failed at partition=%d offset=%d, will retry: %w", r.Partition, r.Offset, err))
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
