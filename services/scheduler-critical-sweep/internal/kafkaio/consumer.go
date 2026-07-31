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

// ParseRecordKey — разбор ключа Kafka-записи execution.control обратно в
// (scope, scope_id). Формат ключа — "<ExecutionControlScope proto
// string>:<scope_id>", см. services/execution-control-service/internal/
// kafkaio/publisher.go::RecordKey (единственный producer этого топика).
// Нужен отдельно от DecodeControlRecord: tombstone-запись компактированного
// топика (удаление ключа при compaction/явном delete) несёт value=nil — ни
// scope, ни scope_id в value взять неоткуда, только из самого ключа записи.
func ParseRecordKey(key []byte) (commonv1.ExecutionControlScope, string, error) {
	s := string(key)
	idx := strings.IndexByte(s, ':')
	if idx < 0 {
		return commonv1.ExecutionControlScope_EXECUTION_CONTROL_SCOPE_UNSPECIFIED, "", fmt.Errorf("execution.control record key %q: нет разделителя ':'", s)
	}
	scopeName, scopeID := s[:idx], s[idx+1:]
	scopeVal, ok := commonv1.ExecutionControlScope_value[scopeName]
	if !ok {
		return commonv1.ExecutionControlScope_EXECUTION_CONTROL_SCOPE_UNSPECIFIED, "", fmt.Errorf("execution.control record key %q: неизвестный scope %q", s, scopeName)
	}
	return commonv1.ExecutionControlScope(scopeVal), scopeID, nil
}

// ManualCommandConsumer — реальный franz-go консьюмер scheduler.critical.commands.
// В группе (partitioned) — это командный поток от Backoffice API, не
// compacted local snapshot, поэтому разделение работы по партициям между
// репликами здесь корректно, в отличие от execution.control (см.
// ControlSnapshotConsumer). Не проверялся против живого брокера (см. README).
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

// ControlSnapshotConsumer — консьюмер execution.control (compacted),
// строит и поддерживает ПОЛНЫЙ локальный immutable snapshot на каждой
// реплике (internal/controlsnapshot) — CODE_REVIEW.md finding #4/#5.
//
// **Осознанно без kgo.ConsumerGroup()**: service_io_contracts.md
// документирует execution.control как "Kafka (compacted, local snapshot)"
// примерно 10 раз для разных потребителей — каждая реплика должна
// независимо зеркалировать ВЕСЬ топик, а не делить партиции между собой в
// группе. С consumer group партиции execution.control делятся между
// репликами Critical Sweep — под-под, не назначенный конкретной партиции,
// никогда не видит записи для (scope, scope_id) из неё; с fail-open
// IsPaused() (controlsnapshot.Snapshot) это значит, что GLOBAL PAUSE может
// молча игнорироваться частью реплик. Без ConsumerGroup franz-go назначает
// ВСЕ партиции топика напрямую каждому клиенту (обычный, не группа,
// consumer) — именно это нужно для локального зеркала.
//
// **kgo.ConsumeResetOffset(kgo.NewOffset().AtStart())** — без committed
// offset'ов группы каждый под должен сам перечитать топик с начала при
// старте/рестарте, чтобы восстановить снапшот с нуля (это compacted-топик,
// поэтому "с начала" — не то же самое, что "весь исторический трафик": на
// нём остаются только последние записи по каждому ключу + tombstones).
type ControlSnapshotConsumer struct {
	client *kgo.Client
}

func NewControlSnapshotConsumer(brokers []string) (*ControlSnapshotConsumer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(ExecutionControlTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return nil, fmt.Errorf("kgo.NewClient: %w", err)
	}
	return &ControlSnapshotConsumer{client: client}, nil
}

func (c *ControlSnapshotConsumer) Close() { c.client.Close() }

// Run — цикл потребления. apply вызывается на каждую обычную запись
// (upsert), remove — на tombstone (value=nil, значит ключ compaction'ится
// прочь — см. ParseRecordKey докстринг). polled вызывается после КАЖДОГО
// успешного (без фатальной ошибки соединения) вызова PollFetches — main.go
// использует это как сигнал "консьюмер реально жив и делает прогресс" для
// warm-up перед SetReady(true) (CODE_REVIEW.md finding #6).
func (c *ControlSnapshotConsumer) Run(
	ctx context.Context,
	apply func(scope commonv1.ExecutionControlScope, scopeID string, state commonv1.ExecutionControlState),
	remove func(scope commonv1.ExecutionControlScope, scopeID string),
	polled func(),
	errHandler func(error),
) {
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
			scope, scopeID, err := ParseRecordKey(rec.Key)
			if err != nil {
				if errHandler != nil {
					errHandler(err)
				}
				return
			}

			if rec.Value == nil {
				// Tombstone — compaction убирает этот (scope, scope_id):
				// снимаем override целиком, не переводим в какое-то state.
				remove(scope, scopeID)
				return
			}

			decoded, err := DecodeControlRecord(rec.Value)
			if err != nil {
				if errHandler != nil {
					errHandler(err)
				}
				return
			}
			apply(scope, scopeID, decoded.GetState())
		})

		if polled != nil {
			polled()
		}
	}
}
