package kafkaio

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Регрессия на шумный лог: сервис сутками писал
// `dlr-manager: fetch error: context deadline exceeded` каждые ~5 секунд
// при лаге 0 по operator.dlr — холостой опрос логировался как авария и
// маскировал реальные ошибки. См. isExpectedPollOutcome.

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
	got := collectErrors(fetchesWithErr(TopicOperatorDlr, 0, context.DeadlineExceeded))
	if len(got) != 0 {
		t.Fatalf("холостой опрос по таймауту не должен попадать в errHandler, получили %+v", got)
	}
}

func TestReportFetchErrorsIgnoresWrappedDeadlineExceeded(t *testing.T) {
	// franz-go оборачивает контекстную ошибку — проверяем, что фильтр
	// работает через errors.Is, а не по сравнению значений.
	wrapped := fmt.Errorf("fetching from broker: %w", context.DeadlineExceeded)
	got := collectErrors(fetchesWithErr(TopicOperatorDlr, 0, wrapped))
	if len(got) != 0 {
		t.Fatalf("обёрнутый DeadlineExceeded тоже ожидаемый исход, получили %+v", got)
	}
}

func TestReportFetchErrorsIgnoresCanceledOnShutdown(t *testing.T) {
	// При SIGTERM родительский ctx отменяется — это штатная остановка.
	got := collectErrors(fetchesWithErr(TopicOperatorDlrUnresolved, 3, context.Canceled))
	if len(got) != 0 {
		t.Fatalf("отмена ctx при shutdown не ошибка, получили %+v", got)
	}
}

func TestReportFetchErrorsReportsRealError(t *testing.T) {
	real := errors.New("unknown topic or partition")
	got := collectErrors(fetchesWithErr(TopicOperatorDlr, 1, real))
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
			Topic: TopicOperatorDlr,
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
	reportFetchErrors(fetchesWithErr(TopicOperatorDlr, 0, errors.New("boom")), nil)
}
