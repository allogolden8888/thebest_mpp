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
	"bytes"
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

// EnsureGlobalBootstrap закрывает холодный старт всей платформы: consumer'ы
// execution.control (pipeline-engine::execution_control.rs,
// partner-rest-receiver::admission.rs) fail-closed, пока не увидят
// ExecutionControlRecord для scope=GLOBAL, а runGlobalControlLoop публикует
// GLOBAL только после первого УСПЕШНОГО тика Prometheus — на пустом трафике
// global_error_rate = 0/0 = NaN и тик пропускается (см. main.go). Без
// реального трафика метрика никогда не станет валидной, а без GLOBAL sentinel
// трафик никогда не пойдёт — платформа зависает навсегда с абсолютно нуля
// (пустой/новый execution.control), а не только в этом локальном стенде.
//
// Если для GLOBAL уже есть запись (тёплый рестарт на непустом compacted
// топике), эта функция ничего не делает — не затирает легитимный
// DEGRADED/PAUSED state форсированным ACTIVE.
func EnsureGlobalBootstrap(ctx context.Context, brokers []string, pub *Publisher) error {
	globalKey := registry.ScopeKey{Scope: hysteresis.ScopeGlobal}
	wantKey := RecordKey(globalKey)

	found, err := hasExistingRecord(ctx, brokers, wantKey)
	if err != nil {
		return fmt.Errorf("ensure_global_bootstrap: чтение текущего состояния: %w", err)
	}
	if found {
		return nil
	}

	now := time.Now().UTC()
	rec := &eventsv1.ExecutionControlRecord{
		Scope:         commonv1.ExecutionControlScope_EXECUTION_CONTROL_SCOPE_GLOBAL,
		State:         commonv1.ExecutionControlState_EXECUTION_CONTROL_STATE_ACTIVE,
		AdmissionRate: 1.0,
		DispatchRate:  1.0,
		Reason:        "bootstrap_default_no_prior_global_state",
		Version:       1,
		CreatedAt:     timestamppb.New(now),
	}
	if err := pub.Publish(ctx, globalKey, rec); err != nil {
		return fmt.Errorf("ensure_global_bootstrap: публикация дефолта: %w", err)
	}
	return nil
}

// hasExistingRecord — best-effort чтение execution.control с начала до тех
// пор, пока за readIdleTimeout не появится ни одной новой записи (топик мал
// и compacted, так что "тихо" на практике означает "дочитали до конца"),
// в поиске записи с заданным ключом. Отдельный direct-consumer client (без
// consumer group) — не мешает будущим реальным consumer'ам этого сервиса.
func hasExistingRecord(ctx context.Context, brokers []string, wantKey []byte) (bool, error) {
	const readIdleTimeout = 3 * time.Second

	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(Topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(500*time.Millisecond),
	)
	if err != nil {
		return false, fmt.Errorf("kgo.NewClient: %w", err)
	}
	defer client.Close()

	for {
		pollCtx, cancel := context.WithTimeout(ctx, readIdleTimeout)
		fetches := client.PollFetches(pollCtx)
		cancel()

		empty := true
		found := false
		fetches.EachRecord(func(r *kgo.Record) {
			empty = false
			if bytes.Equal(r.Key, wantKey) {
				found = true
			}
		})
		if found {
			return true, nil
		}
		if empty {
			// Ни одной записи за целый readIdleTimeout — считаем, что дочитали
			// до конца существующих данных compacted-топика.
			return false, nil
		}
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
	}
}
