// Package kafkaio — on_submit_accepted (service_internal_methods.md §4.1):
// консьюмер operator.submit.accepted, декодирует OperatorSubmitAccepted в
// writer.CorrelationRecord.
package kafkaio

import (
	"context"
	"errors"
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

// isExpectedPollOutcome — ОЖИДАЕМЫЙ, штатный исход опроса, а не ошибка.
//
// Почему это выделено отдельно и почему это важно. Вызывающий (см.
// cmd/dlr-correlation-writer/main.go) опрашивает Kafka в бесконечном цикле
// с pollCtx = context.WithTimeout(ctx, flushInterval), а flushInterval по
// умолчанию 2 секунды. Когда новых operator.submit.accepted в топике нет —
// нормальное состояние между всплесками трафика, — дедлайн истекает и
// franz-go возвращает context.DeadlineExceeded по каждой назначенной
// партиции. Раньше это безусловно уходило в errHandler и сервис писал
// `dlr-correlation-writer: fetch error: context deadline exceeded` каждые
// ~2 секунды, круглосуточно, при полностью исправной работе.
//
// Цена такого лога уже измерена на живой системе в соседнем сервисе: у
// dlr-manager ровно эта строка стоила ложного диагноза «DLR не приходят,
// сервис сломан» и увела отладку в сторону на существенную часть сессии,
// тогда как реальная поломка была в другом месте (см. коммит 7e796fa и
// dlr-manager/internal/kafkaio/kafkaio.go). Хуже того, поток одинаковых
// строк маскирует настоящие ошибки fetch этого сервиса: если он
// действительно начнёт падать на опросе, это утонет в шуме.
//
// context.Canceled — тот же класс: при shutdown родительский ctx
// отменяется по SIGTERM/SIGINT, и незавершённый опрос возвращает Canceled.
// Штатная остановка, не авария.
//
// Глушатся РОВНО эти два исхода и только через errors.Is (franz-go
// оборачивает контекстные ошибки). Всё остальное — брокер недоступен,
// ошибки партиции, потеря данных, проблемы авторизации — логируется как
// прежде, с тем же форматом `fetch error: %w`.
func isExpectedPollOutcome(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)
}

// reportFetchErrors — отдаёт в errHandler только неожидаемые ошибки опроса.
// Выделено из PollOnce отдельной функцией, потому что PollOnce требует
// живого *kgo.Client, а эта логика должна покрываться unit-тестом (см.
// consumer_test.go).
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
	reportFetchErrors(fetches, errHandler)
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
