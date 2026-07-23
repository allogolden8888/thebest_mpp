// Package kafkaio — on_config_change (service_internal_methods.md §3.4):
// консьюмер config.changes (compacted), декодирует ConfigChangeEvent.
package kafkaio

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	eventsv1 "mpp/platformcontracts/events/v1"
)

const Topic = "config.changes"

// DecodeConfigChangeEvent — чистая функция, разбор одного сообщения.
func DecodeConfigChangeEvent(payload []byte) (*eventsv1.ConfigChangeEvent, error) {
	var event eventsv1.ConfigChangeEvent
	if err := proto.Unmarshal(payload, &event); err != nil {
		return nil, fmt.Errorf("unmarshal ConfigChangeEvent: %w", err)
	}
	if event.GetEntityId() == "" {
		return nil, fmt.Errorf("ConfigChangeEvent без entity_id")
	}
	return &event, nil
}

// Consumer — реальный franz-go консьюмер config.changes. Не проверялся
// против живого брокера в этой песочнице.
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
			event, err := DecodeConfigChangeEvent(rec.Value)
			if err != nil {
				if errHandler != nil {
					errHandler(err)
				}
				return
			}
			handler(event)
		})
	}
}