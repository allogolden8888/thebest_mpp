// Package kafkaio — handle_force_scheduler_command (service_internal_methods.md
// §7.3): Kafka publish на scheduler.critical.commands. Backoffice API —
// единственный producer этого топика (service_io_contracts.md строка 301,
// HLD §9.1); consumer — Scheduler Critical Sweep (on_manual_command,
// FORCE_TIMEOUT/FORCE_RETRY).
package kafkaio

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "mpp/platformcontracts/common/v1"
	eventsv1 "mpp/platformcontracts/events/v1"
)

const criticalCommandsTopic = "scheduler.critical.commands"

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

// BuildCriticalCommand — чистая функция: HTTP-вход -> protobuf-событие.
// taskType ограничен разрешённым списком (FORCE_TIMEOUT/FORCE_RETRY) —
// защита от использования Scheduler как открытого редиректа внутри Kafka
// (тот же принцип, что и у target_topic в SchedulerBackgroundTask, HLD §14).
func BuildCriticalCommand(stageExecutionID string, taskType commonv1.CriticalCommandType, requestedBy, reason string, requestedAt time.Time) *eventsv1.SchedulerCriticalCommand {
	return &eventsv1.SchedulerCriticalCommand{
		StageExecutionId: stageExecutionID,
		TaskType:         taskType,
		RequestedBy:      requestedBy,
		Reason:           reason,
		RequestedAt:      timestamppb.New(requestedAt),
	}
}

func (p *Publisher) PublishCriticalCommand(ctx context.Context, cmd *eventsv1.SchedulerCriticalCommand) error {
	payload, err := proto.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("proto.Marshal(SchedulerCriticalCommand): %w", err)
	}
	record := &kgo.Record{Topic: criticalCommandsTopic, Key: []byte(cmd.GetStageExecutionId()), Value: payload}
	return p.client.ProduceSync(ctx, record).FirstErr()
}
