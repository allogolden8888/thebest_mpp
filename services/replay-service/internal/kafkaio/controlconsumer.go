// Package kafkaio — консьюмер execution.control как локальный
// full-mirror снапшот (не shared consumer group). CODE_REVIEW.md HIGH
// finding и cross-service наблюдение ревью: service_io_contracts.md
// документирует execution.control как "Kafka (compacted, local snapshot)"
// примерно 10 раз, но ни один из существующих консьюмеров этой сессии не
// был реализован именно так — shared consumer group делит партиции между
// репликами, и под, которому не досталась партиция с нужным (scope,
// scope_id), никогда не увидит эту запись, а IsPaused() fail-open
// трактует отсутствующий ключ как "не приостановлено". ControlConsumer
// здесь НЕ использует kgo.ConsumerGroup — без консьюмер-группы franz-go
// напрямую назначает клиенту ВСЕ партиции топика, то есть каждая реплика
// сервиса действительно строит полную локальную копию, как и
// специфицировано.
package kafkaio

import (
	"context"
	"fmt"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	commonv1 "mpp/platformcontracts/common/v1"
	eventsv1 "mpp/platformcontracts/events/v1"
)

const ExecutionControlTopic = "execution.control"

// DecodeExecutionControlRecord — чистая функция, разбор одного сообщения.
func DecodeExecutionControlRecord(payload []byte) (*eventsv1.ExecutionControlRecord, error) {
	var rec eventsv1.ExecutionControlRecord
	if err := proto.Unmarshal(payload, &rec); err != nil {
		return nil, fmt.Errorf("unmarshal ExecutionControlRecord: %w", err)
	}
	return &rec, nil
}

// ParseRecordKey — обратная операция к RecordKey в
// execution-control-service/internal/kafkaio/publisher.go
// ("<ExecutionControlScope proto string>:<scope_id>") — нужна для
// tombstone-записей compacted-топика, у которых Value пуст и (scope,
// scope_id) можно восстановить только из Key.
func ParseRecordKey(key []byte) (commonv1.ExecutionControlScope, string, error) {
	scopeName, scopeID, ok := strings.Cut(string(key), ":")
	if !ok {
		return 0, "", fmt.Errorf("execution.control record key %q не в формате SCOPE:scope_id", key)
	}
	scopeValue, ok := commonv1.ExecutionControlScope_value[scopeName]
	if !ok {
		return 0, "", fmt.Errorf("execution.control record key: неизвестный scope %q", scopeName)
	}
	return commonv1.ExecutionControlScope(scopeValue), scopeID, nil
}

// ControlConsumer — реальный franz-go консьюмер execution.control, БЕЗ
// consumer group (см. package doc) — читает с начала топика на каждом
// старте, чтобы построить полный снапшот с нуля (тот же принцип, что и
// у любого compacted-топика: текущее состояние = развёрнутый по ключу
// весь лог). Не проверялся против живого брокера в этой песочнице.
type ControlConsumer struct {
	client *kgo.Client
}

func NewControlConsumer(brokers []string) (*ControlConsumer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(ExecutionControlTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return nil, fmt.Errorf("kgo.NewClient: %w", err)
	}
	return &ControlConsumer{client: client}, nil
}

func (c *ControlConsumer) Close() { c.client.Close() }

// Run — блокирующий цикл. Для обычной записи onRecord вызывается с
// декодированным *ExecutionControlRecord и tombstone=false; для
// tombstone (пустой Value) — rec=nil, tombstone=true, а scope/scopeID
// разобраны из Key (см. ParseRecordKey), поскольку Value ничего не
// несёт. Ошибки декодирования идут в errHandler и не останавливают
// цикл (одна повреждённая запись не должна убивать снапшот целиком).
func (c *ControlConsumer) Run(ctx context.Context, onRecord func(rec *eventsv1.ExecutionControlRecord, scope commonv1.ExecutionControlScope, scopeID string, tombstone bool), errHandler func(error)) {
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
		fetches.EachRecord(func(kr *kgo.Record) {
			if len(kr.Value) == 0 {
				scope, scopeID, err := ParseRecordKey(kr.Key)
				if err != nil {
					if errHandler != nil {
						errHandler(err)
					}
					return
				}
				onRecord(nil, scope, scopeID, true)
				return
			}
			rec, err := DecodeExecutionControlRecord(kr.Value)
			if err != nil {
				if errHandler != nil {
					errHandler(err)
				}
				return
			}
			onRecord(rec, rec.GetScope(), rec.GetScopeId(), false)
		})
	}
}
