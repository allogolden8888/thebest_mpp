// Package kafkaio — publish_control_record (service_internal_methods.md
// §3.1): сборка ExecutionControlRecord (чистая функция, тестируется без
// брокера) и публикация в execution.control (compacted), platform-contracts/
// events/config_and_control.proto.
//
// Как и в services/destination-resolution-service/src/kafka_io.rs: бизнес-
// логика (BuildControlRecord) — чистая функция; обвязка (Publisher, реальный
// franz-go клиент) компилируется и использует настоящие типы клиента, но
// против живого брокера в этой песочнице не проверялась (development_plan.md
// "Координация" п.5 — Kafka никогда не поднимался live в этой сессии).
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

	"mpp/execution-control-service/internal/hysteresis"
	"mpp/execution-control-service/internal/registry"
)

const Topic = "execution.control"

func scopeToProto(s hysteresis.Scope) commonv1.ExecutionControlScope {
	switch s {
	case hysteresis.ScopeStage:
		return commonv1.ExecutionControlScope_EXECUTION_CONTROL_SCOPE_STAGE
	case hysteresis.ScopePartner:
		return commonv1.ExecutionControlScope_EXECUTION_CONTROL_SCOPE_PARTNER
	case hysteresis.ScopePartnerStage:
		return commonv1.ExecutionControlScope_EXECUTION_CONTROL_SCOPE_PARTNER_STAGE
	case hysteresis.ScopeOperatorRoute:
		return commonv1.ExecutionControlScope_EXECUTION_CONTROL_SCOPE_OPERATOR_ROUTE
	default:
		return commonv1.ExecutionControlScope_EXECUTION_CONTROL_SCOPE_GLOBAL
	}
}

func stateToProto(s hysteresis.State) commonv1.ExecutionControlState {
	switch s {
	case hysteresis.StateDegraded:
		return commonv1.ExecutionControlState_EXECUTION_CONTROL_STATE_DEGRADED
	case hysteresis.StatePaused:
		return commonv1.ExecutionControlState_EXECUTION_CONTROL_STATE_PAUSED
	default:
		return commonv1.ExecutionControlState_EXECUTION_CONTROL_STATE_ACTIVE
	}
}

// BuildControlRecord собирает ExecutionControlRecord из результата
// registry.Evaluate — чистая функция, без сети, юнит-тестируется напрямую.
func BuildControlRecord(key registry.ScopeKey, eval registry.Evaluation, now time.Time) *eventsv1.ExecutionControlRecord {
	rec := &eventsv1.ExecutionControlRecord{
		Scope:         scopeToProto(key.Scope),
		ScopeId:       key.ScopeID,
		State:         stateToProto(eval.State),
		AdmissionRate: eval.AdmissionRate,
		DispatchRate:  eval.DispatchRate,
		Reason:        eval.Reason,
		Version:       eval.Version,
		CreatedAt:     timestamppb.New(now),
	}
	if eval.ExpiresAt != nil {
		rec.ExpiresAt = timestamppb.New(*eval.ExpiresAt)
	}
	return rec
}

// RecordKey — ключ Kafka-сообщения для compacted-топика: без него записи
// GLOBAL (scope_id="") коллапсировали бы с любым другим scope с пустым
// scope_id на compaction.
func RecordKey(key registry.ScopeKey) []byte {
	scopeName := scopeToProto(key.Scope).String()
	return []byte(fmt.Sprintf("%s:%s", scopeName, key.ScopeID))
}

// Publisher — тонкая обёртка над *kgo.Client для публикации в
// execution.control. Реальный клиент (создаётся kgo.NewClient с реальными
// брокерами), но интеграционно против живого Kafka не проверялся в этой
// песочнице.
type Publisher struct {
	client *kgo.Client
}

func NewPublisher(brokers []string) (*Publisher, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ProducerBatchCompression(kgo.SnappyCompression()),
	)
	if err != nil {
		return nil, fmt.Errorf("kgo.NewClient: %w", err)
	}
	return &Publisher{client: client}, nil
}

func (p *Publisher) Close() {
	p.client.Close()
}

// Publish сериализует record и синхронно продюсирует его в execution.control.
func (p *Publisher) Publish(ctx context.Context, key registry.ScopeKey, rec *eventsv1.ExecutionControlRecord) error {
	payload, err := proto.Marshal(rec)
	if err != nil {
		return fmt.Errorf("proto.Marshal(ExecutionControlRecord): %w", err)
	}

	record := &kgo.Record{
		Topic: Topic,
		Key:   RecordKey(key),
		Value: payload,
	}

	result := p.client.ProduceSync(ctx, record)
	return result.FirstErr()
}
