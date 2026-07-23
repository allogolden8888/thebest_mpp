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
