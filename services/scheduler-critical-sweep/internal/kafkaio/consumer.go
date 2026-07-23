package kafkaio

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	eventsv1 "mpp/platformcontracts/events/v1"
)

// DecodeManualCommand — чистая функция, разбор одного сообщения
// scheduler.critical.commands (on_manual_command, service_internal_methods.md
// §2.1: FORCE_TIMEOUT/FORCE_RETRY).
func DecodeManualCommand(payload []byte) (*eventsv1.SchedulerCriticalCommand, error) {
	var cmd eventsv1.SchedulerCriticalCommand
	if err := proto.Unmarshal(payload, &cmd); err != nil {
		return nil, fmt.Errorf("unmarshal SchedulerCriticalCommand: %w", err)
	}
	if cmd.GetStageExecutionId() == "" {
		return nil, fmt.Errorf("SchedulerCriticalCommand без stage_execution_id")
	}
	return &cmd, nil
}

// DecodeControlRecord — чистая функция, разбор одного сообщения
// execution.control (для controlsnapshot.Snapshot.Apply).
func DecodeControlRecord(payload []byte) (*eventsv1.ExecutionControlRecord, error) {
	var rec eventsv1.ExecutionControlRecord
	if err := proto.Unmarshal(payload, &rec); err != nil {
		return nil, fmt.Errorf("unmarshal ExecutionControlRecord: %w", err)
	}
	return &rec, nil
}

// ManualCommandConsumer — реальный franz-go консьюмер scheduler.critical.commands.
// Не проверялся против живого брокера в этой песочнице (см. README).
type ManualCommandConsumer struct {
	client *kgo.Client
}

func NewManualCommandConsumer(brokers []string, groupID string) (*ManualCommandConsumer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeTopics("scheduler.critical.commands"),
	)
	if err != nil {
		return nil, fmt.Errorf("kgo.NewClient: %w", err)
	}
	return &ManualCommandConsumer{client: client}, nil
}

func (c *ManualCommandConsumer) Close() { c.client.Close() }

// ControlSnapshotConsumer — консьюмер execution.control (compacted),
// строит и поддерживает локальный immutable snapshot (см.
// internal/controlsnapshot). Не проверялся против живого брокера.
type ControlSnapshotConsumer struct {
	client *kgo.Client
}

func NewControlSnapshotConsumer(brokers []string, groupID string) (*ControlSnapshotConsumer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeTopics("execution.control"),
	)
	if err != nil {
		return nil, fmt.Errorf("kgo.NewClient: %w", err)
	}
	return &ControlSnapshotConsumer{client: client}, nil
}

func (c *ControlSnapshotConsumer) Close() { c.client.Close() }

func (c *ControlSnapshotConsumer) Run(ctx context.Context, handler func(*eventsv1.ExecutionControlRecord), errHandler func(error)) {
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
			decoded, err := DecodeControlRecord(rec.Value)
			if err != nil {
				if errHandler != nil {
					errHandler(err)
				}
				return
			}
			handler(decoded)
		})
	}
}

// Run — цикл потребления; handler вызывается на каждую успешно
// декодированную команду, ошибки декодирования логируются вызывающей
// стороной через errHandler, не останавливают цикл (одно повреждённое
// сообщение не должен ронять весь sweep).
func (c *ManualCommandConsumer) Run(ctx context.Context, handler func(*eventsv1.SchedulerCriticalCommand), errHandler func(error)) {
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
			cmd, err := DecodeManualCommand(rec.Value)
			if err != nil {
				if errHandler != nil {
					errHandler(err)
				}
				return
			}
			handler(cmd)
		})
	}
}
