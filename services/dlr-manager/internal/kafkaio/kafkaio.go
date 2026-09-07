// Package kafkaio — реальный franz-go I/O для DLR Manager: два консьюмера
// (operator.dlr, operator.dlr.unresolved), три продюсера (delivery.status,
// scheduler.background.commands, operator.dlr.dlq). Autocommit явно
// выключен — тот же принцип, что в dlr-correlation-writer/policy-service/
// billing-service этой сессии: коммит только после успешной обработки.
package kafkaio

import (
	"context"
	"errors"
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

// isExpectedPollOutcome — ОЖИДАЕМЫЙ, штатный исход опроса, а не ошибка.
//
// Почему это выделено отдельно и почему это важно. Вызывающий (см.
// cmd/dlr-manager/main.go) опрашивает Kafka в бесконечном цикле с
// pollCtx = context.WithTimeout(ctx, 5s). Когда новых DLR в топике нет —
// а это нормальное состояние между всплесками трафика, — дедлайн
// истекает и franz-go возвращает context.DeadlineExceeded по каждой
// назначенной партиции. Раньше это безусловно уходило в errHandler и
// сервис писал `dlr-manager: fetch error: context deadline exceeded`
// каждые ~5 секунд, круглосуточно, при полностью исправной работе.
//
// Цена этого лога измерена на живой системе: consumer group dlr-manager
// имела лаг 0 по operator.dlr (188 889 из 188 889 записей вычитано), то
// есть сервис работал штатно, а по логам выглядел сломанным — на ложный
// вывод «DLR не приходят, dlr-manager сломан» ушла существенная часть
// отладочной сессии, тогда как реальная поломка была в двух других
// сервисах. Хуже того, поток одинаковых строк маскирует настоящие
// ошибки fetch этого сервиса: если он действительно начнёт падать на
// опросе, это утонет в шуме.
//
// context.Canceled — тот же класс: при shutdown родительский ctx
// отменяется по SIGTERM/SIGINT, и незавершённый опрос возвращает
// Canceled. Штатная остановка, не авария.
//
// Глушится РОВНО эти два исхода и только через errors.Is (franz-go
// оборачивает контекстные ошибки). Всё остальное — брокер недоступен,
// ошибки партиции, потеря данных, проблемы авторизации — логируется как
// прежде.
func isExpectedPollOutcome(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)
}

// reportFetchErrors — отдаёт в errHandler только неожидаемые ошибки
// опроса. Выделено из PollFetches отдельной функцией, потому что
// PollFetches требует живого *kgo.Client, а эта логика должна
// покрываться unit-тестом (см. kafkaio_test.go).
func reportFetchErrors(fetches kgo.Fetches, errHandler func(error)) {
	if errHandler == nil {
		return
	}
	fetches.EachError(func(_ string, _ int32, err error) {
		if isExpectedPollOutcome(err) {
			return
		}
		errHandler(fmt.Errorf("fetch error: %w", err))
	})
}

// PollFetches — раньше называлась PollOnce и коммитила каждую запись
// индивидуально сразу в callback'е вызывающего кода; см. OffsetTracker за
// тем, почему это было небезопасно и как это исправлено — коммит теперь
// целиком на стороне вызывающего, после того как он применит OffsetTracker
// ко всем записям этого poll'а.
func (c *Consumer) PollFetches(ctx context.Context, errHandler func(error)) []FetchedRecord {
	fetches := c.client.PollFetches(ctx)
	reportFetchErrors(fetches, errHandler)
	var records []FetchedRecord
	fetches.EachRecord(func(rec *kgo.Record) {
		records = append(records, FetchedRecord{Raw: rec, FromRetry: rec.Topic == TopicOperatorDlrUnresolved})
	})
	return records
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
