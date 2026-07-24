// Package kafkaio — реальный franz-go I/O для DLR Manager: два консьюмера
// (operator.dlr, operator.dlr.unresolved), три продюсера (delivery.status,
// scheduler.background.commands, operator.dlr.dlq). Autocommit явно
// выключен — тот же принцип, что в dlr-correlation-writer/policy-service/
// billing-service этой сессии: коммит только после успешной обработки.
package kafkaio

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	eventsv1 "mpp/platformcontracts/events/v1"
)

const (
	TopicOperatorDlr           = "operator.dlr"
	TopicOperatorDlrUnresolved = "operator.dlr.unresolved"
	TopicDeliveryStatus        = "delivery.status"
	TopicSchedulerBackground   = "scheduler.background.commands"
	TopicOperatorDlrDlq        = "operator.dlr.dlq"
)

type Consumer struct {
	client *kgo.Client
}

// NewConsumer — консьюмер, подписанный СРАЗУ на оба входных топика
// (operator.dlr, operator.dlr.unresolved) в одной consumer group — оба
// нуждаются в одной и той же обработке (on_raw_dlr), различие только в
// протобуф-типе (OperatorDlr vs SchedulerBackgroundTask), см. Run.
func NewConsumer(brokers []string, groupID string) (*Consumer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeTopics(TopicOperatorDlr, TopicOperatorDlrUnresolved),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return nil, fmt.Errorf("kgo.NewClient: %w", err)
	}
	return &Consumer{client: client}, nil
}

func (c *Consumer) Close() { c.client.Close() }

func (c *Consumer) CommitRecords(ctx context.Context, records ...*kgo.Record) error {
	return c.client.CommitRecords(ctx, records...)
}

// FetchedRecord — одна запись с уже определённым источником (какой из двух
// топиков), обработчик решает, как декодировать payload.
type FetchedRecord struct {
	Raw       *kgo.Record
	FromRetry bool // true — operator.dlr.unresolved (SchedulerBackgroundTask), false — operator.dlr (OperatorDlr)
}

func (c *Consumer) PollOnce(ctx context.Context, onRecord func(FetchedRecord), errHandler func(error)) {
	fetches := c.client.PollFetches(ctx)
	fetches.EachError(func(_ string, _ int32, err error) {
		if errHandler != nil {
			errHandler(fmt.Errorf("fetch error: %w", err))
		}
	})
	fetches.EachRecord(func(rec *kgo.Record) {
		onRecord(FetchedRecord{Raw: rec, FromRetry: rec.Topic == TopicOperatorDlrUnresolved})
	})
}

type Producer struct {
	client *kgo.Client
}

func NewProducer(brokers []string) (*Producer, error) {
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return nil, fmt.Errorf("kgo.NewClient (producer): %w", err)
	}
	return &Producer{client: client}, nil
}

func (p *Producer) Close() { p.client.Close() }

func (p *Producer) produce(ctx context.Context, topic, key string, payload []byte) error {
	record := &kgo.Record{Topic: topic, Key: []byte(key), Value: payload}
	result := p.client.ProduceSync(ctx, record)
	if err := result.FirstErr(); err != nil {
		return fmt.Errorf("produce to %s: %w", topic, err)
	}
	return nil
}

func (p *Producer) PublishDeliveryStatus(ctx context.Context, event *eventsv1.DeliveryStatusEvent) error {
	payload, err := proto.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal DeliveryStatusEvent: %w", err)
	}
	return p.produce(ctx, TopicDeliveryStatus, event.GetMessageId(), payload)
}

func (p *Producer) PublishRetryTask(ctx context.Context, task *eventsv1.SchedulerBackgroundTask) error {
	payload, err := proto.Marshal(task)
	if err != nil {
		return fmt.Errorf("marshal SchedulerBackgroundTask: %w", err)
	}
	return p.produce(ctx, TopicSchedulerBackground, task.GetSourceEventId(), payload)
}

func (p *Producer) PublishDlq(ctx context.Context, dlrEvent *eventsv1.OperatorDlr) error {
	payload, err := proto.Marshal(dlrEvent)
	if err != nil {
		return fmt.Errorf("marshal OperatorDlr (dlq): %w", err)
	}
	return p.produce(ctx, TopicOperatorDlrDlq, dlrEvent.GetOperatorId(), payload)
}
