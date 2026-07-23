// Package redisio — tick_sweep / load_execution_state / clear_deadline
// (service_internal_methods.md §2.1) против Runtime Redis
// (data_infrastructure_spec.md §2.1: exec:{message_id}, deadlines:{bucket}).
package redisio

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"mpp/scheduler-critical-sweep/internal/sweep"
)

// Client — тонкая обёртка над go-redis для трёх операций Critical Sweep.
type Client struct {
	rdb        *redis.Client
	numBuckets int
}

func NewClient(addr, password string, numBuckets int) *Client {
	return &Client{
		rdb: redis.NewClient(&redis.Options{Addr: addr, Password: password}),
		numBuckets: numBuckets,
	}
}

func NewClientFromRedis(rdb *redis.Client, numBuckets int) *Client {
	return &Client{rdb: rdb, numBuckets: numBuckets}
}

func (c *Client) Close() error { return c.rdb.Close() }

func bucketKey(bucket int) string {
	return fmt.Sprintf("deadlines:%d", bucket)
}

// TickSweep — tick_sweep: обход ZRANGEBYSCORE deadlines:{bucket} -inf now по
// каждому bucket. Шардирование по data_infrastructure_spec.md §2.1:
// "bucket = hash(stage_execution_id) % N".
func (c *Client) TickSweep(ctx context.Context, now time.Time) ([]sweep.ExpiredEntry, error) {
	nowMs := now.UnixMilli()
	var out []sweep.ExpiredEntry

	for bucket := 0; bucket < c.numBuckets; bucket++ {
		results, err := c.rdb.ZRangeByScoreWithScores(ctx, bucketKey(bucket), &redis.ZRangeBy{
			Min: "-inf",
			Max: strconv.FormatInt(nowMs, 10),
		}).Result()
		if err != nil {
			return nil, fmt.Errorf("ZRANGEBYSCORE %s: %w", bucketKey(bucket), err)
		}
		for _, z := range results {
			stageExecutionID, ok := z.Member.(string)
			if !ok {
				continue
			}
			out = append(out, sweep.ExpiredEntry{
				StageExecutionID: stageExecutionID,
				Bucket:           bucket,
				DeadlineUnixMs:   int64(z.Score),
			})
		}
	}
	return out, nil
}

// ClearDeadline — clear_deadline: ZREM из deadlines:{bucket} после обработки.
func (c *Client) ClearDeadline(ctx context.Context, entry sweep.ExpiredEntry) error {
	return c.rdb.ZRem(ctx, bucketKey(entry.Bucket), entry.StageExecutionID).Err()
}

// stageExecIndexKey — **не входит в data_infrastructure_spec.md §2.1**.
// deadlines:{bucket} хранит только stage_execution_id как ZSET member, а
// exec:{message_id} — HASH, ключ по message_id, не по stage_execution_id.
// Ни один документ этой сессии не специфицирует, как Critical Sweep должен
// получить message_id по stage_execution_id, взятому из просроченной записи.
// Здесь предполагается дополнительный STRING-ключ, который должен писать
// Pipeline Engine той же Lua-транзакцией, что и cas_transition_and_track_deadline
// (§1.4) — симметрично тому, как он уже пишет ZADD в deadlines:{bucket}.
// Это находка для координации с Главным агентом (владеет pipeline-engine и
// схемой Runtime Redis), не тихо принятое решение — см. README.md
// "Открытый вопрос".
func stageExecIndexKey(stageExecutionID string) string {
	return "stage_exec_index:" + stageExecutionID
}

// ResolveMessageID — недостающий шаг между ExpiredEntry.StageExecutionID и
// exec:{message_id}. См. docstring stageExecIndexKey.
func (c *Client) ResolveMessageID(ctx context.Context, stageExecutionID string) (string, error) {
	messageID, err := c.rdb.Get(ctx, stageExecIndexKey(stageExecutionID)).Result()
	if err == redis.Nil {
		return "", fmt.Errorf("stage_exec_index не содержит %q — запись устарела или индекс не был записан Pipeline Engine", stageExecutionID)
	}
	if err != nil {
		return "", fmt.Errorf("GET %s: %w", stageExecIndexKey(stageExecutionID), err)
	}
	return messageID, nil
}

// LoadExecutionState — load_execution_state: HGETALL exec:{message_id},
// поля по data_infrastructure_spec.md §2.1.
func (c *Client) LoadExecutionState(ctx context.Context, messageID string) (sweep.ExecutionState, error) {
	fields, err := c.rdb.HGetAll(ctx, "exec:"+messageID).Result()
	if err != nil {
		return sweep.ExecutionState{}, fmt.Errorf("HGETALL exec:%s: %w", messageID, err)
	}
	if len(fields) == 0 {
		return sweep.ExecutionState{}, fmt.Errorf("exec:%s не найден — Execution State уже освобождён (finalize_pipeline) или истёк TTL", messageID)
	}

	attempt, err := strconv.ParseInt(fields["attempt"], 10, 32)
	if err != nil {
		return sweep.ExecutionState{}, fmt.Errorf("parse attempt %q: %w", fields["attempt"], err)
	}
	deadlineMs, err := strconv.ParseInt(fields["deadline"], 10, 64)
	if err != nil {
		return sweep.ExecutionState{}, fmt.Errorf("parse deadline %q: %w", fields["deadline"], err)
	}

	return sweep.ExecutionState{
		MessageID:          messageID,
		PipelineVersion:    fields["pipeline_version"],
		NodeID:             fields["node_id"],
		StageExecutionID:   fields["stage_execution_id"],
		StageName:          fields["current_state"],
		Attempt:            int32(attempt),
		Deadline:           time.UnixMilli(deadlineMs),
		LastAppliedEventID: fields["last_applied_event_id"],
	}, nil
}
