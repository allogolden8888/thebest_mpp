package redisio

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"mpp/scheduler-critical-sweep/internal/sweep"
)

// newTestClient — реальный Redis-протокол против miniredis (встроенный
// Go-сервер, не мок транспорта) — ZADD/ZRANGEBYSCORE/HSET/GET реально
// выполняются, не симулируются вручную.
func newTestClient(t *testing.T) (*Client, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	return NewClientFromRedis(rdb, 4), mr
}

func TestTickSweepFindsExpiredEntriesAcrossBuckets(t *testing.T) {
	c, mr := newTestClient(t)
	ctx := context.Background()
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)

	// Просрочено (deadline в прошлом), bucket 0.
	mr.ZAdd("deadlines:0", float64(now.Add(-time.Minute).UnixMilli()), "exec-expired-1")
	// Ещё не просрочено (deadline в будущем), тот же bucket — не должно попасть в результат.
	mr.ZAdd("deadlines:0", float64(now.Add(time.Hour).UnixMilli()), "exec-future-1")
	// Просрочено, другой bucket.
	mr.ZAdd("deadlines:2", float64(now.Add(-time.Second).UnixMilli()), "exec-expired-2")

	entries, err := c.TickSweep(ctx, now)
	if err != nil {
		t.Fatalf("TickSweep failed: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("ожидали 2 просроченные записи, получили %d: %+v", len(entries), entries)
	}

	found := map[string]int{}
	for _, e := range entries {
		found[e.StageExecutionID] = e.Bucket
	}
	if found["exec-expired-1"] != 0 {
		t.Fatalf("exec-expired-1 должен быть в bucket 0, получили %d", found["exec-expired-1"])
	}
	if found["exec-expired-2"] != 2 {
		t.Fatalf("exec-expired-2 должен быть в bucket 2, получили %d", found["exec-expired-2"])
	}
	if _, ok := found["exec-future-1"]; ok {
		t.Fatalf("будущий дедлайн не должен попадать в просроченные записи")
	}
}

func TestClearDeadlineRemovesEntry(t *testing.T) {
	c, mr := newTestClient(t)
	ctx := context.Background()
	mr.ZAdd("deadlines:1", 100, "exec-1")

	err := c.ClearDeadline(ctx, sweep.ExpiredEntry{StageExecutionID: "exec-1", Bucket: 1})
	if err != nil {
		t.Fatalf("ClearDeadline failed: %v", err)
	}

	members, err := c.rdb.ZRange(ctx, "deadlines:1", 0, -1).Result()
	if err != nil {
		t.Fatalf("readback failed: %v", err)
	}
	if len(members) != 0 {
		t.Fatalf("ожидали пустой ZSET после ClearDeadline, получили %v", members)
	}
}

func TestResolveMessageIDReturnsErrorWhenIndexMissing(t *testing.T) {
	c, _ := newTestClient(t)
	_, err := c.ResolveMessageID(context.Background(), "unknown-stage-exec")
	if err == nil {
		t.Fatalf("ожидали ошибку для отсутствующего stage_exec_index")
	}
}

func TestResolveMessageIDReturnsMessageIDWhenPresent(t *testing.T) {
	c, mr := newTestClient(t)
	mr.Set("stage_exec_index:exec-1", "msg-1")

	got, err := c.ResolveMessageID(context.Background(), "exec-1")
	if err != nil {
		t.Fatalf("ResolveMessageID failed: %v", err)
	}
	if got != "msg-1" {
		t.Fatalf("ожидали msg-1, получили %s", got)
	}
}

func TestLoadExecutionStateParsesAllFields(t *testing.T) {
	c, mr := newTestClient(t)
	deadline := time.Date(2026, 7, 23, 13, 0, 0, 0, time.UTC)

	mr.HSet("exec:msg-1",
		"pipeline_version", "3",
		"node_id", "node-billing",
		"stage_execution_id", "exec-1",
		"current_state", "BILLING",
		"attempt", "2",
		"deadline", strconv.FormatInt(deadline.UnixMilli(), 10),
		"last_applied_event_id", "evt-9",
	)

	state, err := c.LoadExecutionState(context.Background(), "msg-1")
	if err != nil {
		t.Fatalf("LoadExecutionState failed: %v", err)
	}
	if state.MessageID != "msg-1" || state.StageExecutionID != "exec-1" || state.StageName != "BILLING" {
		t.Fatalf("неверные базовые поля: %+v", state)
	}
	if state.Attempt != 2 {
		t.Fatalf("ожидали attempt=2, получили %d", state.Attempt)
	}
	if !state.Deadline.Equal(deadline) {
		t.Fatalf("ожидали deadline=%v, получили %v", deadline, state.Deadline)
	}
	if state.LastAppliedEventID != "evt-9" {
		t.Fatalf("неверный last_applied_event_id: %s", state.LastAppliedEventID)
	}
}

func TestLoadExecutionStateErrorsWhenMissing(t *testing.T) {
	c, _ := newTestClient(t)
	_, err := c.LoadExecutionState(context.Background(), "no-such-message")
	if err == nil {
		t.Fatalf("ожидали ошибку для отсутствующего exec:{message_id}")
	}
}

// TestLoadExecutionStateReadsOptionalStageResultFields — CODE_REVIEW.md
// finding #1: LoadExecutionState должен оппортунистически прочитать
// накопленные результаты предыдущих стадий, если Pipeline Engine их пишет,
// чтобы buildStageExtension могла собрать stage_extension при republish.
func TestLoadExecutionStateReadsOptionalStageResultFields(t *testing.T) {
	c, mr := newTestClient(t)
	mr.HSet("exec:msg-1",
		"pipeline_version", "3",
		"node_id", "node-billing",
		"stage_execution_id", "exec-1",
		"current_state", "BILLING",
		"attempt", "1",
		"deadline", "1000",
		"destination_address", "998901234567",
		"resolved_operator_id", "beeline",
		"category", "TRANSACTION",
		"segment_count", "3",
		"route_id", "route-1",
		"protocol", "1",
		"route_version", "v1",
	)

	state, err := c.LoadExecutionState(context.Background(), "msg-1")
	if err != nil {
		t.Fatalf("LoadExecutionState failed: %v", err)
	}
	if state.DestinationAddress != "998901234567" || state.ResolvedOperatorID != "beeline" || state.Category != "TRANSACTION" {
		t.Fatalf("неверные опциональные поля: %+v", state)
	}
	if state.SegmentCount != 3 || state.RouteID != "route-1" || state.Protocol != 1 || state.RouteVersion != "v1" {
		t.Fatalf("неверные опциональные поля: %+v", state)
	}
}

// TestLoadExecutionStateToleratesMissingOptionalFields — сегодняшняя норма
// (data_infrastructure_spec.md §2.1 их не документирует): отсутствие этих
// полей — не ошибка, просто нулевые значения.
func TestLoadExecutionStateToleratesMissingOptionalFields(t *testing.T) {
	c, mr := newTestClient(t)
	mr.HSet("exec:msg-1",
		"pipeline_version", "3",
		"node_id", "node-billing",
		"stage_execution_id", "exec-1",
		"current_state", "BILLING",
		"attempt", "1",
		"deadline", "1000",
	)

	state, err := c.LoadExecutionState(context.Background(), "msg-1")
	if err != nil {
		t.Fatalf("LoadExecutionState failed: %v", err)
	}
	if state.DestinationAddress != "" || state.ResolvedOperatorID != "" || state.SegmentCount != 0 {
		t.Fatalf("ожидали нулевые значения для отсутствующих опциональных полей: %+v", state)
	}
}

// TestClaimDeadlineOnlyOneOfTwoConcurrentCallersWins — CODE_REVIEW.md
// finding #3: прямое доказательство того, зачем ClaimDeadline вообще
// написан. Два "реплики" пытаются застолбить одну и ту же просроченную
// запись — ровно одна должна получить claimed=true.
func TestClaimDeadlineOnlyOneOfTwoConcurrentCallersWins(t *testing.T) {
	c, mr := newTestClient(t)
	ctx := context.Background()
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	mr.ZAdd("deadlines:0", float64(now.Add(-time.Minute).UnixMilli()), "exec-1")
	entry := sweep.ExpiredEntry{StageExecutionID: "exec-1", Bucket: 0, DeadlineUnixMs: now.Add(-time.Minute).UnixMilli()}

	claimed1, err := c.ClaimDeadline(ctx, entry, now)
	if err != nil {
		t.Fatalf("первый клэйм failed: %v", err)
	}
	claimed2, err := c.ClaimDeadline(ctx, entry, now)
	if err != nil {
		t.Fatalf("второй клэйм failed: %v", err)
	}

	if !claimed1 {
		t.Fatalf("первый клэйм должен победить (claimed=true)")
	}
	if claimed2 {
		t.Fatalf("второй клэйм для уже забранной записи должен вернуть claimed=false — иначе дублирующая публикация (finding #3)")
	}

	members, _ := c.rdb.ZRange(ctx, "deadlines:0", 0, -1).Result()
	if len(members) != 0 {
		t.Fatalf("после успешного клэйма запись должна быть удалена из ZSET, получили %v", members)
	}
}

// TestClaimDeadlineFalseWhenNotYetExpired — score всё ещё в будущем (могла
// быть переустановлена между TickSweep и клэймом) — клэйм не должен молча
// удалить её.
func TestClaimDeadlineFalseWhenNotYetExpired(t *testing.T) {
	c, mr := newTestClient(t)
	ctx := context.Background()
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	mr.ZAdd("deadlines:0", float64(now.Add(time.Hour).UnixMilli()), "exec-1")

	claimed, err := c.ClaimDeadline(ctx, sweep.ExpiredEntry{StageExecutionID: "exec-1", Bucket: 0}, now)
	if err != nil {
		t.Fatalf("ClaimDeadline failed: %v", err)
	}
	if claimed {
		t.Fatalf("не должны клэймить запись с дедлайном в будущем")
	}
}

// TestRestoreDeadlineReAddsEntry — CODE_REVIEW.md finding #7: если клэйм
// прошёл, но публикация не удалась, запись должна вернуться в ZSET, а не
// потеряться из планирования.
func TestRestoreDeadlineReAddsEntry(t *testing.T) {
	c, mr := newTestClient(t)
	ctx := context.Background()
	deadlineMs := int64(123456)
	entry := sweep.ExpiredEntry{StageExecutionID: "exec-1", Bucket: 0, DeadlineUnixMs: deadlineMs}

	if err := c.RestoreDeadline(ctx, entry); err != nil {
		t.Fatalf("RestoreDeadline failed: %v", err)
	}

	score, err := c.rdb.ZScore(ctx, "deadlines:0", "exec-1").Result()
	if err != nil {
		t.Fatalf("readback failed: %v", err)
	}
	if int64(score) != deadlineMs {
		t.Fatalf("ожидали score=%d, получили %v", deadlineMs, score)
	}
	_ = mr
}

// TestDeferDeadlinePushesScoreForward — CODE_REVIEW.md finding #9: held
// (paused) записи должны откладываться, а не пересчитываться на каждом
// тике.
func TestDeferDeadlinePushesScoreForward(t *testing.T) {
	c, mr := newTestClient(t)
	ctx := context.Background()
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	mr.ZAdd("deadlines:0", float64(now.Add(-time.Minute).UnixMilli()), "exec-1")

	until := now.Add(5 * time.Second)
	if err := c.DeferDeadline(ctx, sweep.ExpiredEntry{StageExecutionID: "exec-1", Bucket: 0}, until); err != nil {
		t.Fatalf("DeferDeadline failed: %v", err)
	}

	entries, err := c.TickSweep(ctx, now)
	if err != nil {
		t.Fatalf("TickSweep failed: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("после DeferDeadline запись не должна считаться просроченной на now, получили %+v", entries)
	}

	future, err := c.TickSweep(ctx, until.Add(time.Second))
	if err != nil {
		t.Fatalf("TickSweep failed: %v", err)
	}
	if len(future) != 1 {
		t.Fatalf("после наступления until запись снова должна считаться просроченной, получили %+v", future)
	}
}
