package kafkaio

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "mpp/platformcontracts/common/v1"
	eventsv1 "mpp/platformcontracts/events/v1"
)

func validEvent() *eventsv1.OperatorSubmitAccepted {
	now := time.Now().UTC().Truncate(time.Second)
	return &eventsv1.OperatorSubmitAccepted{
		MessageId:            "m1",
		StageExecutionId:     "se1",
		OperatorId:           "beeline",
		Protocol:             commonv1.Protocol_PROTOCOL_SMPP,
		SmscMessageId:        "smsc-123",
		SegmentId:            1,
		SubmittedAt:          timestamppb.New(now),
		CorrelationExpiresAt: timestamppb.New(now.Add(48 * time.Hour)),
	}
}

func TestDecodeValidEventRoundTrips(t *testing.T) {
	event := validEvent()
	payload, err := proto.Marshal(event)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	record, err := DecodeOperatorSubmitAccepted(payload)
	if err != nil {
		t.Fatalf("DecodeOperatorSubmitAccepted: %v", err)
	}
	if record.OperatorID != "beeline" {
		t.Errorf("OperatorID = %q, want beeline", record.OperatorID)
	}
	if record.SmscMessageID != "smsc-123" {
		t.Errorf("SmscMessageID = %q, want smsc-123", record.SmscMessageID)
	}
	if record.MessageID != "m1" || record.StageExecutionID != "se1" {
		t.Errorf("MessageID/StageExecutionID = %q/%q, want m1/se1", record.MessageID, record.StageExecutionID)
	}
	if record.SegmentID != 1 {
		t.Errorf("SegmentID = %d, want 1", record.SegmentID)
	}
	if !record.ExpiresAt.After(record.SubmittedAt) {
		t.Errorf("ExpiresAt (%v) должен быть после SubmittedAt (%v)", record.ExpiresAt, record.SubmittedAt)
	}
}

func TestDecodeEmptySmscMessageIdIsAcceptedNotRejected(t *testing.T) {
	// "не все операторы возвращают его синхронно" (operator_events.proto) —
	// пустая строка легитимна, не ошибка декодирования.
	event := validEvent()
	event.SmscMessageId = ""
	payload, _ := proto.Marshal(event)

	record, err := DecodeOperatorSubmitAccepted(payload)
	if err != nil {
		t.Fatalf("пустой smsc_message_id не должен быть ошибкой декодирования: %v", err)
	}
	if record.SmscMessageID != "" {
		t.Errorf("SmscMessageID = %q, want пустую строку", record.SmscMessageID)
	}
}

func TestDecodeRejectsMissingOperatorId(t *testing.T) {
	event := validEvent()
	event.OperatorId = ""
	payload, _ := proto.Marshal(event)

	if _, err := DecodeOperatorSubmitAccepted(payload); err == nil {
		t.Fatal("ожидали ошибку на отсутствующий operator_id")
	}
}

func TestDecodeRejectsMissingMessageId(t *testing.T) {
	event := validEvent()
	event.MessageId = ""
	payload, _ := proto.Marshal(event)

	if _, err := DecodeOperatorSubmitAccepted(payload); err == nil {
		t.Fatal("ожидали ошибку на отсутствующий message_id")
	}
}

func TestDecodeRejectsMissingStageExecutionId(t *testing.T) {
	event := validEvent()
	event.StageExecutionId = ""
	payload, _ := proto.Marshal(event)

	if _, err := DecodeOperatorSubmitAccepted(payload); err == nil {
		t.Fatal("ожидали ошибку на отсутствующий stage_execution_id")
	}
}

func TestDecodeRejectsGarbageBytes(t *testing.T) {
	if _, err := DecodeOperatorSubmitAccepted([]byte{0xFF, 0xFE, 0x00, 0x01}); err == nil {
		t.Fatal("ожидали ошибку unmarshal на мусорные байты")
	}
}

// --- Регрессия на шумный лог холостого опроса ---
//
// Сервис писал `dlr-correlation-writer: fetch error: context deadline
// exceeded` каждые ~2 секунды (flushInterval) при полностью исправной
// работе: холостой опрос логировался как авария и маскировал реальные
// ошибки fetch. Тот же класс правки, что 7e796fa в dlr-manager.
// См. isExpectedPollOutcome.

func fetchesWithErr(topic string, partition int32, err error) kgo.Fetches {
	return kgo.Fetches{{
		Topics: []kgo.FetchTopic{{
			Topic:      topic,
			Partitions: []kgo.FetchPartition{{Partition: partition, Err: err}},
		}},
	}}
}

func collectErrors(fetches kgo.Fetches) []error {
	var got []error
	reportFetchErrors(fetches, func(err error) { got = append(got, err) })
	return got
}

func TestReportFetchErrorsIgnoresDeadlineExceeded(t *testing.T) {
	got := collectErrors(fetchesWithErr(Topic, 0, context.DeadlineExceeded))
	if len(got) != 0 {
		t.Fatalf("холостой опрос по таймауту не должен попадать в errHandler, получили %+v", got)
	}
}

func TestReportFetchErrorsIgnoresWrappedDeadlineExceeded(t *testing.T) {
	// franz-go оборачивает контекстную ошибку — проверяем, что фильтр
	// работает через errors.Is, а не по сравнению значений.
	wrapped := fmt.Errorf("fetching from broker: %w", context.DeadlineExceeded)
	got := collectErrors(fetchesWithErr(Topic, 0, wrapped))
	if len(got) != 0 {
		t.Fatalf("обёрнутый DeadlineExceeded тоже ожидаемый исход, получили %+v", got)
	}
}

func TestReportFetchErrorsIgnoresCanceledOnShutdown(t *testing.T) {
	// При SIGTERM родительский ctx отменяется — это штатная остановка.
	got := collectErrors(fetchesWithErr(Topic, 3, context.Canceled))
	if len(got) != 0 {
		t.Fatalf("отмена ctx при shutdown не ошибка, получили %+v", got)
	}
}

func TestReportFetchErrorsReportsRealError(t *testing.T) {
	real := errors.New("unknown topic or partition")
	got := collectErrors(fetchesWithErr(Topic, 1, real))
	if len(got) != 1 {
		t.Fatalf("настоящая ошибка fetch должна логироваться, получили %+v", got)
	}
	if !errors.Is(got[0], real) {
		t.Fatalf("исходная ошибка должна сохраняться в цепочке, получили %v", got[0])
	}
	if got[0].Error() != "fetch error: unknown topic or partition" {
		t.Fatalf("формат сообщения изменился: %q", got[0].Error())
	}
}

func TestReportFetchErrorsReportsRealErrorAlongsideDeadline(t *testing.T) {
	// Разные партиции в одном poll'е: таймаут по одной не должен глушить
	// настоящую ошибку по другой.
	fetches := kgo.Fetches{{
		Topics: []kgo.FetchTopic{{
			Topic: Topic,
			Partitions: []kgo.FetchPartition{
				{Partition: 0, Err: context.DeadlineExceeded},
				{Partition: 1, Err: errors.New("broker unavailable")},
			},
		}},
	}}
	got := collectErrors(fetches)
	if len(got) != 1 {
		t.Fatalf("ожидалась ровно одна залогированная ошибка, получили %+v", got)
	}
	if got[0].Error() != "fetch error: broker unavailable" {
		t.Fatalf("залогирована не та ошибка: %q", got[0].Error())
	}
}

func TestReportFetchErrorsNilHandlerDoesNotPanic(t *testing.T) {
	reportFetchErrors(fetchesWithErr(Topic, 0, errors.New("boom")), nil)
}
