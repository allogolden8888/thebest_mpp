package core

import (
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "mpp/platformcontracts/common/v1"
)

// Регрессия на реальную находку: ни один сервис-стадия не заполняет
// completed_at, из-за чего все пер-стадийные строки в ClickHouse ложились
// с occurred_at=1970 и длительности стадий были нулевыми.
func TestFromStageCompletedFallsBackToKafkaTimestamp(t *testing.T) {
	kafkaTime := time.Date(2026, 8, 21, 6, 7, 40, 0, time.UTC)
	rec := FromStageCompletedAt(&commonv1.StageCompletedEvent{
		EventId:   "evt-1",
		MessageId: "msg-1",
	}, kafkaTime)

	if !rec.OccurredAt.Equal(kafkaTime) {
		t.Fatalf("OccurredAt = %v, ожидался fallback %v", rec.OccurredAt, kafkaTime)
	}
}

// Значение из payload обязано побеждать fallback — иначе, когда продюсеры
// починят, аналитика продолжит писать время приёма вместо времени
// завершения стадии.
func TestPayloadCompletedAtWinsOverFallback(t *testing.T) {
	payloadTime := time.Date(2026, 8, 21, 5, 0, 0, 0, time.UTC)
	kafkaTime := time.Date(2026, 8, 21, 6, 0, 0, 0, time.UTC)

	rec := FromStageCompletedAt(&commonv1.StageCompletedEvent{
		EventId:     "evt-2",
		MessageId:   "msg-2",
		CompletedAt: timestamppb.New(payloadTime),
	}, kafkaTime)

	if !rec.OccurredAt.Equal(payloadTime) {
		t.Fatalf("OccurredAt = %v, ожидалось payload-значение %v", rec.OccurredAt, payloadTime)
	}
}

// Без fallback'а (нулевое время) поведение прежнее — не подменяем на
// "сейчас", чтобы не выдумывать данные.
func TestNoFallbackKeepsZeroTime(t *testing.T) {
	rec := FromStageCompletedAt(&commonv1.StageCompletedEvent{EventId: "e", MessageId: "m"}, time.Time{})
	if rec.OccurredAt.Unix() > 0 {
		t.Fatalf("ожидалось нулевое время без fallback, получено %v", rec.OccurredAt)
	}
}
