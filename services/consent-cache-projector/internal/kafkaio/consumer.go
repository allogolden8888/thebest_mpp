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

type Consumer struct {
	client *kgo.Client
}

func NewConsumer(brokers []string, groupID string) (*Consumer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeTopics(Topic),
	)
	if err != nil {
		return nil, fmt.Errorf("kgo.NewClient: %w", err)
	}
	return &Consumer{client: client}, nil
}

func (c *Consumer) Close() { c.client.Close() }

func (c *Consumer) Run(ctx context.Context, handler func(*eventsv1.ConfigChangeEvent), errHandler func(error)) {
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
		fetches.EachRecord(func(rec *kgo.Record) {
			event, err := DecodeConsentEvent(rec.Value)
			if err != nil {
				if errHandler != nil {
					errHandler(err)
				}
				return
			}
			if event == nil {
				return // другой entity_type, не наш
			}
			handler(event)
		})
	}
}
