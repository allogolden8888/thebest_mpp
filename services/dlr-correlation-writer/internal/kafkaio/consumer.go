// Package kafkaio — on_submit_accepted (service_internal_methods.md §4.1):
// консьюмер operator.submit.accepted, декодирует OperatorSubmitAccepted в
// writer.CorrelationRecord.
package kafkaio

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	eventsv1 "mpp/platformcontracts/events/v1"

	"mpp/dlr-correlation-writer/internal/writer"
)

const Topic = "operator.submit.accepted"

// DecodeOperatorSubmitAccepted — чистая функция, разбор одного сообщения.
// Найдено при реализации: `smsc_message_id` документирован как опциональный
// ("не все операторы возвращают его синхронно") — пустая строка допустима
// здесь, не отклоняется как невалидная запись; но `operator_id` — обязателен
// (без него запись бесполезна для будущего lookup в DLR Manager) и
// `message_id`/`stage_execution_id` — обязательны (иначе DLR Manager не
// сможет опубликовать delivery.status на правильный message_id).
func DecodeOperatorSubmitAccepted(payload []byte) (writer.CorrelationRecord, error) {
	var event eventsv1.OperatorSubmitAccepted
	if err := proto.Unmarshal(payload, &event); err != nil {
		return writer.CorrelationRecord{}, fmt.Errorf("unmarshal OperatorSubmitAccepted: %w", err)
	}
	if event.GetOperatorId() == "" {
		return writer.CorrelationRecord{}, fmt.Errorf("OperatorSubmitAccepted без operator_id")
	}
	if event.GetMessageId() == "" {
		return writer.CorrelationRecord{}, fmt.Errorf("OperatorSubmitAccepted без message_id")
	}
	if event.GetStageExecutionId() == "" {
		return writer.CorrelationRecord{}, fmt.Errorf("OperatorSubmitAccepted без stage_execution_id")
	}

	var submittedAt, expiresAt time.Time
	if ts := event.GetSubmittedAt(); ts != nil {
		submittedAt = ts.AsTime()
	}
	if ts := event.GetCorrelationExpiresAt(); ts != nil {
		expiresAt = ts.AsTime()
	}

	return writer.CorrelationRecord{
		OperatorID:       event.GetOperatorId(),
		SmscMessageID:    event.GetSmscMessageId(),
		SegmentID:        event.GetSegmentId(),
		MessageID:        event.GetMessageId(),
		StageExecutionID: event.GetStageExecutionId(),
		SubmittedAt:      submittedAt,
		ExpiresAt:        expiresAt,
	}, nil
}

// Consumer — реальный franz-go консьюмер operator.submit.accepted, ручной
// commit (autocommit намеренно выключен — тот же класс проблемы, что
// CODE_REVIEW.md нашло у трёх Go-сервисов Субагента 1: "Kafka autocommit
// decoupled from processing success — silent, permanent data loss"; здесь
// не воспроизведено с самого начала, не исправлено задним числом).
type Consumer struct {
	client *kgo.Client
}

func NewConsumer(brokers []string, groupID string) (*Consumer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeTopics(Topic),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return nil, fmt.Errorf("kgo.NewClient: %w", err)
	}
	return &Consumer{client: client}, nil
}

func (c *Consumer) Close() { c.client.Close() }

// CommitRecords — коммитит явно переданные записи (per-partition, самая
// свежая офсет+epoch среди переданных — см. kgo.Client.CommitRecords).
// Вызывается ТОЛЬКО после успешного writer.PgWriter.Flush для
// соответствующего снапшота буфера, не автоматически.
func (c *Consumer) CommitRecords(ctx context.Context, records ...*kgo.Record) error {
	return c.client.CommitRecords(ctx, records...)
}

// PollOnce — один цикл фетча, декодирует все полученные записи. `ctx`
// обычно оборачивается вызывающей стороной в `context.WithTimeout` на
// интервал flush'а (см. cmd/dlr-correlation-writer/main.go) — это то, что
// позволяет одному циклу опроса одновременно обслуживать и "по размеру", и
// "по таймеру" flush без отдельной горутины/мьютекса поверх буфера.
//
// MEDIUM находка кодревью (PART 2, dlr-correlation-writer #1): запись,
// которая не декодируется (malformed protobuf/отсутствует обязательное
// поле), логировалась через errHandler и пропускалась — но
// `latestByPartition` (main.go) обновлялся только для успешно
// декодированных записей, так что следующая УСПЕШНАЯ запись той же
// партиции коммитила offset мимо поломанной, и её correlation-строка
// терялась молча и навсегда, а не оставалась "застрявшей", как
// предполагалось. Теперь `onDecodeFailure` сообщает вызывающей стороне
// НОМЕР ПАРТИЦИИ, где произошла ошибка — main.go приостанавливает приём
// новых записей этой партиции (см. `suspendedPartitions`) до перезапуска
// процесса, вместо того чтобы дать более свежей записи молча продвинуть
// commit мимо непрочитанной.
func (c *Consumer) PollOnce(
	ctx context.Context,
	onRecord func(rec writer.BufferedRecord, raw *kgo.Record),
	onDecodeFailure func(partition int32),
	errHandler func(error),
) {
	fetches := c.client.PollFetches(ctx)
	fetches.EachError(func(_ string, _ int32, err error) {
		if errHandler != nil {
			errHandler(fmt.Errorf("fetch error: %w", err))
		}
	})
	fetches.EachRecord(func(rec *kgo.Record) {
		correlation, err := DecodeOperatorSubmitAccepted(rec.Value)
		if err != nil {
			if errHandler != nil {
				errHandler(err)
			}
			if onDecodeFailure != nil {
				onDecodeFailure(rec.Partition)
			}
			return
		}
		onRecord(writer.BufferedRecord{Correlation: correlation, Partition: rec.Partition, Offset: rec.Offset}, rec)
	})
}
