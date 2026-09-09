package store

import (
	"testing"
	"time"

	"mpp/lifecycle-writer/internal/core"
)

// Регрессия на измеренный отказ: сервис 8 684 раза подряд падал с
// SQLSTATE 23514, потому что партиции создавались под время flush'а, а не
// под occurred_at записей. При replay после простоя это гарантированный
// вечный цикл, блокирующий коммит офсетов по всем топикам сразу.
func TestPlanHistoryPartitionsCoversReplayHours(t *testing.T) {
	now := time.Date(2026, 9, 9, 12, 30, 0, 0, time.UTC)
	rows := []core.LifecycleHistoryRow{
		{OccurredAt: now.Add(-5 * time.Hour)},
		{OccurredAt: now.Add(-5 * time.Hour).Add(10 * time.Minute)}, // тот же час
		{OccurredAt: now.Add(-30 * time.Hour)},
		{OccurredAt: now},
	}
	plan := PlanHistoryPartitions(rows, now, DefaultPartitionMaxPast, DefaultPartitionMaxFuture)

	if len(plan.Accepted) != 4 {
		t.Fatalf("все 4 записи в окне 48ч, принято %d", len(plan.Accepted))
	}
	if len(plan.Rejected) != 0 {
		t.Fatalf("отвергать нечего, отвергнуто %d", len(plan.Rejected))
	}
	want := map[time.Time]bool{
		now.Truncate(time.Hour):                     true,
		now.Truncate(time.Hour).Add(time.Hour):      true,
		now.Add(-5 * time.Hour).Truncate(time.Hour): true,
		now.Add(-30 * time.Hour).Truncate(time.Hour): true,
	}
	if len(plan.Hours) != len(want) {
		t.Fatalf("часов ожидалось %d, получено %d: %v", len(want), len(plan.Hours), plan.Hours)
	}
	for _, h := range plan.Hours {
		if !want[h] {
			t.Fatalf("лишний час %v", h)
		}
	}
	for i := 1; i < len(plan.Hours); i++ {
		if !plan.Hours[i-1].Before(plan.Hours[i]) {
			t.Fatalf("часы не отсортированы: %v", plan.Hours)
		}
	}
}

// Вне окна — отбрасываем, а не копим: повтор вечно был бы poison pill'ом
// того же класса, что здесь чинится. Партиция под такие часы всё равно
// будет удалена retention'ом.
func TestPlanHistoryPartitionsRejectsOutsideWindow(t *testing.T) {
	now := time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)
	rows := []core.LifecycleHistoryRow{
		{OccurredAt: now.Add(-100 * time.Hour)}, // слишком старая
		{OccurredAt: now.Add(100 * time.Hour)},  // слишком будущая
		{OccurredAt: now.Add(-1 * time.Hour)},   // нормальная
	}
	plan := PlanHistoryPartitions(rows, now, DefaultPartitionMaxPast, DefaultPartitionMaxFuture)

	if len(plan.Accepted) != 1 {
		t.Fatalf("принять надо ровно одну, принято %d", len(plan.Accepted))
	}
	if len(plan.Rejected) != 2 {
		t.Fatalf("отвергнуть надо две, отвергнуто %d", len(plan.Rejected))
	}
}

// Даже на пустом батче нужны текущий и следующий час: буфер может пересечь
// границу часа между первой записью и flush'ем.
func TestPlanHistoryPartitionsEmptyBatchStillCoversNow(t *testing.T) {
	now := time.Date(2026, 9, 9, 12, 59, 59, 0, time.UTC)
	plan := PlanHistoryPartitions(nil, now, DefaultPartitionMaxPast, DefaultPartitionMaxFuture)
	if len(plan.Hours) != 2 {
		t.Fatalf("ожидалось 2 часа (now, now+1h), получено %d", len(plan.Hours))
	}
}
