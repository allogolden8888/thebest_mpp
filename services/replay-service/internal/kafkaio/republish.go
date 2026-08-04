// Package kafkaio — republish (service_internal_methods.md §7.4): прошедший
// все проверки DlqRecord -> KafkaAck на исходный stage.*.
package kafkaio

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	commonv1 "mpp/platformcontracts/common/v1"
)

// StageTopic — тот же маппинг, что services/scheduler-critical-sweep/internal/kafkaio/topics.go.
func StageTopic(stageName commonv1.StageName) (string, error) {
	switch stageName {
	case commonv1.StageName_STAGE_NAME_DESTINATION_RESOLUTION:
		return "stage.destination-resolution", nil
	case commonv1.StageName_STAGE_NAME_POLICY:
		return "stage.policy", nil
	case commonv1.StageName_STAGE_NAME_BILLING:
		return "stage.billing", nil
	case commonv1.StageName_STAGE_NAME_ROUTING:
		return "stage.routing", nil
	case commonv1.StageName_STAGE_NAME_DELIVERY:
		return "stage.delivery", nil
	case commonv1.StageName_STAGE_NAME_DELIVERY_RECONCILIATION:
		return "stage.delivery-reconciliation", nil
	default:
		return "", fmt.Errorf("нет топика для StageName %v", stageName)
	}
}

// DecodeOriginalCommand — чистая функция, разбор original_command (BYTEA из dlq_record).
func DecodeOriginalCommand(payload []byte) (*commonv1.StageExecuteCommand, error) {
	var cmd commonv1.StageExecuteCommand
	if err := proto.Unmarshal(payload, &cmd); err != nil {
		return nil, fmt.Errorf("unmarshal StageExecuteCommand: %w", err)
	}
	return &cmd, nil
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

// Republish — публикует original_command как есть (тот же attempt, не
// increment — это не retry по таймауту, это ручной republish уже
// провалидированной команды, service_internal_methods.md §7.4 не
// специфицирует увеличение attempt для этого пути).
func (p *Publisher) Republish(ctx context.Context, cmd *commonv1.StageExecuteCommand) error {
	topic, err := StageTopic(cmd.GetStageName())
	if err != nil {
		return err
	}
	payload, err := proto.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("proto.Marshal(StageExecuteCommand): %w", err)
	}
	record := &kgo.Record{Topic: topic, Key: []byte(cmd.GetMessageId()), Value: payload}
	return p.client.ProduceSync(ctx, record).FirstErr()
}
