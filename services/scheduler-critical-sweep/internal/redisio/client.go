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

// Client — тонкая обёртка над go-redis для операций Critical Sweep.
type Client struct {
	rdb        *redis.Client
	numBuckets int

	claimScript *redis.Script
}

func NewClient(addr, password string, numBuckets int) *Client {
	return newClientFromRedis(redis.NewClient(&redis.Options{Addr: addr, Password: password}), numBuckets)
}

func NewClientFromRedis(rdb *redis.Client, numBuckets int) *Client {
	return newClientFromRedis(rdb, numBuckets)
}

func newClientFromRedis(rdb *redis.Client, numBuckets int) *Client {
	return &Client{
		rdb:         rdb,
		numBuckets:  numBuckets,
		claimScript: redis.NewScript(claimDeadlineLua),
	}
}

func (c *Client) Close() error { return c.rdb.Close() }

func bucketKey(bucket int) string {
	return fmt.Sprintf("deadlines:%d", bucket)
}

// TickSweep — tick_sweep: обход ZRANGEBYSCORE deadlines:{bucket} -inf now по
// каждому bucket. Шардирование по data_infrastructure_spec.md §2.1:
// "bucket = hash(stage_execution_id) % N". Только ЧТЕНИЕ — не удаляет и не
// "клэймит" записи (см. ClaimDeadline: клэйм происходит только непосредственно
// перед публикацией, не здесь — CODE_REVIEW.md finding #3/#9).
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

// claimDeadlineLua — CODE_REVIEW.md finding #3: 4+ реплики Critical Sweep
// опрашивают один и тот же Redis keyspace без координации. Раньше ZREM
// вызывался ПОСЛЕ успешной публикации — значит любая реплика, успевшая
// опросить тот же bucket до того, как другая закончит publish+ZREM,
// независимо публикует ту же просроченную запись со своим новым event_id.
// Здесь клэйм — единственный атомарный Redis-вызов: одновременно
// перепроверяет, что запись всё ещё просрочена (score <= now — на случай,
// если что-то успело переустановить дедлайн между TickSweep и клэймом), и
// удаляет её. Из N конкурентных попыток клэйма ровно одна получит 1
// (реально удалила запись), остальные — 0 (запись уже удалена/не
// просрочена) — естественная гарантия ZREM как атомарной Redis-операции,
// проверка score добавлена в тот же скрипт, чтобы не делать отдельный
// ZSCORE round-trip (TOCTOU между ZSCORE и ZREM был бы отдельной гонкой).
const claimDeadlineLua = `
local score = redis.call('ZSCORE', KEYS[1], ARGV[1])
if not score then
    return 0
end
if tonumber(score) > tonumber(ARGV[2]) then
    return 0
end
redis.call('ZREM', KEYS[1], ARGV[1])
return 1
`

// ClaimDeadline — атомарная попытка "застолбить" ровно одну просроченную
// запись перед тем, как публиковать что-либо в Kafka. Возвращает
// claimed=false, если другая реплика уже успела её забрать (или запись уже
// была удалена/передвинута) — вызывающая сторона (main.go::processTick)
// должна в этом случае просто пропустить запись, ничего не публикуя.
func (c *Client) ClaimDeadline(ctx context.Context, entry sweep.ExpiredEntry, now time.Time) (bool, error) {
	res, err := c.claimScript.Run(ctx, c.rdb, []string{bucketKey(entry.Bucket)}, entry.StageExecutionID, now.UnixMilli()).Int()
	if err != nil {
		return false, fmt.Errorf("EVAL claim_deadline %s/%s: %w", bucketKey(entry.Bucket), entry.StageExecutionID, err)
	}
	return res == 1, nil
}

// RestoreDeadline — CODE_REVIEW.md finding #7: если ClaimDeadline успел
// удалить запись, но последующая публикация в Kafka не удалась, запись
// нужно вернуть обратно в ZSET с тем же дедлайном (а не молча терять её из
// планирования навсегда) — следующий тик подхватит её снова. В отличие от
// старого поведения ("publish, потом ZREM") это НЕ создаёт дубликат: ZADD
// возвращает запись назад ТОЛЬКО когда сама публикация точно не удалась.
func (c *Client) RestoreDeadline(ctx context.Context, entry sweep.ExpiredEntry) error {
	return c.rdb.ZAdd(ctx, bucketKey(entry.Bucket), redis.Z{
		Score:  float64(entry.DeadlineUnixMs),
		Member: entry.StageExecutionID,
	}).Err()
}

// DeferDeadline — CODE_REVIEW.md finding #9 (held/paused-записи не
// откладываются — каждая уже просроченная запись для стадии на паузе
// перезагружается на КАЖДОМ тике ~1с на всё время инцидента) и finding #8
// (записи с нерезолвящимся stage_exec_index/отсутствующим exec:{message_id}
// ретраятся каждый тик без backoff). Двигает score записи вперёд на
// until — запись остаётся в ZSET (не теряется, не требует клэйма/
// координации между репликами: ZADD на тот же member идемпотентен,
// конкурентный ZADD от нескольких реплик безвреден — не "клэйм", а просто
// "не проверяй меня раньше until"), просто не будет выбрана TickSweep
// снова, пока until не наступит.
func (c *Client) DeferDeadline(ctx context.Context, entry sweep.ExpiredEntry, until time.Time) error {
	return c.rdb.ZAdd(ctx, bucketKey(entry.Bucket), redis.Z{
		Score:  float64(until.UnixMilli()),
		Member: entry.StageExecutionID,
	}).Err()
}

// ClearDeadline — сохранён как явный "снять с учёта безусловно" (используется
// в тестах и как задокументированный примитив), но processTick сам по себе
// больше НЕ вызывает его после публикации — публикация в живом пути идёт
// через ClaimDeadline (клэйм = удаление ДО публикации, см. выше).
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
// поля по data_infrastructure_spec.md §2.1, плюс оппортунистическое чтение
// дополнительных полей (destination_address/resolved_operator_id/category/
// segment_count/route_id/protocol/route_version), нужных для
// buildStageExtension при republish (CODE_REVIEW.md finding #1) — эти поля
// НЕ входят в документированную схему §2.1 сегодня; их отсутствие НЕ
// ошибка (просто нулевые значения), см. sweep.ExecutionState докстринг и
// README "Открытый вопрос".
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

		DestinationAddress: fields["destination_address"],
		ResolvedOperatorID: fields["resolved_operator_id"],
		Category:           fields["category"],
		SegmentCount:       parseOptionalInt32(fields["segment_count"]),
		RouteID:            fields["route_id"],
		Protocol:           parseOptionalInt32(fields["protocol"]),
		RouteVersion:       fields["route_version"],
	}, nil
}

// parseOptionalInt32 — как strconv.ParseInt, но отсутствующее/невалидное
// поле (сегодняшняя норма для полей, которые exec:{message_id} ещё не
// пишет — см. LoadExecutionState) даёт 0, а не ошибку, обрывающую весь
// load_execution_state.
func parseOptionalInt32(s string) int32 {
	if s == "" {
		return 0
	}
	n, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		return 0
	}
	return int32(n)
}
