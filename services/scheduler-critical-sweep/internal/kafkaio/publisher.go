// Publisher — реальный franz-go клиент для publish_retry/
// publish_timeout_result/publish_dlq. Как и в
// services/destination-resolution-service/src/kafka_io.rs и
// services/execution-control-service/internal/kafkaio: бизнес-логика
// (builders.go) — чистые функции, здесь только сетевая обвязка. Не
// проверялось против живого Kafka-брокера в этой песочнице.
package kafkaio

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	commonv1 "mpp/platformcontracts/common/v1"
	eventsv1 "mpp/platformcontracts/events/v1"
)

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

func (p *Publisher) produce(ctx context.Context, topic, key string, payload []byte) error {
	rec := &kgo.Record{Topic: topic, Key: []byte(key), Value: payload}
	return p.client.ProduceSync(ctx, rec).FirstErr()
}

// PublishRetry — republish на исходный stage.* топик, ключ = message_id
// (упорядоченность внутри стадии, service_io_contracts.md "Ключевание").
func (p *Publisher) PublishRetry(ctx context.Context, cmd *commonv1.StageExecuteCommand) error {
	topic, err := StageTopic(cmd.GetStageName())
	if err != nil {
		return err
	}
	payload, err := proto.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("proto.Marshal(StageExecuteCommand): %w", err)
	}
	return p.produce(ctx, topic, cmd.GetMessageId(), payload)
}

// PublishTimeout — publish_timeout_result на stage.completed.
func (p *Publisher) PublishTimeout(ctx context.Context, ev *commonv1.StageCompletedEvent) error {
	payload, err := proto.Marshal(ev)
	if err != nil {
		return fmt.Errorf("proto.Marshal(StageCompletedEvent): %w", err)
	}
	return p.produce(ctx, StageCompletedTopic, ev.GetMessageId(), payload)
}

// PublishDlq — publish_dlq на {stage_topic}.dlq.
func (p *Publisher) PublishDlq(ctx context.Context, rec *eventsv1.DlqRecord) error {
	topic, err := DlqTopic(rec.GetStageName())
	if err != nil {
		return err
	}
	payload, err := proto.Marshal(rec)
	if err != nil {
		return fmt.Errorf("proto.Marshal(DlqRecord): %w", err)
	}
	return p.produce(ctx, topic, rec.GetMessageId(), payload)
}
