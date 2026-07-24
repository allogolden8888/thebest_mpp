package dlr

import (
	"testing"
	"time"

	"mpp/dlr-manager/internal/correlation"
)

func TestDecideUnrecognizedStatusDropsRegardlessOfCorrelation(t *testing.T) {
	now := time.Now()
	rec := &correlation.Record{MessageID: "m1"}
	decision := Decide(now.Add(-time.Minute), "", false, rec, now, time.Hour)
	if decision.Kind != KindDropUnrecognizedStatus {
		t.Fatalf("ожидали KindDropUnrecognizedStatus, получили %v", decision.Kind)
	}
}

func TestDecidePublishesWhenCorrelationFoundAndStatusRecognized(t *testing.T) {
	now := time.Now()
	rec := &correlation.Record{MessageID: "m1"}
	decision := Decide(now.Add(-time.Minute), "DELIVERED", true, rec, now, time.Hour)
	if decision.Kind != KindPublishDeliveryStatus {
		t.Fatalf("ожидали KindPublishDeliveryStatus, получили %v", decision.Kind)
	}
	if decision.NormalizedStatus != "DELIVERED" {
		t.Errorf("NormalizedStatus = %q, want DELIVERED", decision.NormalizedStatus)
	}
}

func TestDecideSchedulesRetryWhenNotFoundAndWindowOpen(t *testing.T) {
	now := time.Now()
	receivedAt := now.Add(-10 * time.Minute)
	decision := Decide(receivedAt, "DELIVERED", true, nil, now, time.Hour)
	if decision.Kind != KindScheduleRetry {
		t.Fatalf("ожидали KindScheduleRetry, получили %v", decision.Kind)
	}
}

func TestDecidePublishesDlqWhenNotFoundAndWindowExpired(t *testing.T) {
	now := time.Now()
	receivedAt := now.Add(-2 * time.Hour) // окно 1 час — уже истекло
	decision := Decide(receivedAt, "DELIVERED", true, nil, now, time.Hour)
	if decision.Kind != KindPublishDlq {
		t.Fatalf("ожидали KindPublishDlq, получили %v", decision.Kind)
	}
}

func TestDecideWindowBoundaryIsExclusive(t *testing.T) {
	now := time.Now()
	// Дедлайн ровно сейчас — now.Before(deadline) должно быть false.
	receivedAt := now.Add(-time.Hour)
	decision := Decide(receivedAt, "DELIVERED", true, nil, now, time.Hour)
	if decision.Kind != KindPublishDlq {
		t.Fatalf("на границе окна (now == deadline) ожидали Expire (KindPublishDlq), получили %v", decision.Kind)
	}
}

func TestDecideCorrelationFoundTakesPriorityOverExpiredWindow(t *testing.T) {
	// Если correlation каким-то образом нашлась ПОСЛЕ истечения окна (гонка,
	// либо повторная попытка успела найти запись прямо перед dlq) — публикуем
	// статус, не отбрасываем найденную корреляцию.
	now := time.Now()
	receivedAt := now.Add(-2 * time.Hour)
	rec := &correlation.Record{MessageID: "m1"}
	decision := Decide(receivedAt, "DELIVERED", true, rec, now, time.Hour)
	if decision.Kind != KindPublishDeliveryStatus {
		t.Fatalf("найденная correlation должна побеждать истёкшее окно, получили %v", decision.Kind)
	}
}
