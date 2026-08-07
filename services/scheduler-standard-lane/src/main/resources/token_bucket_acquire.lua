-- token_bucket_acquire.lua
-- apply_token_bucket (service_internal_methods.md §2.2), CODE_REVIEW.md
-- HIGH #5 real fix: refill + acquire-up-to-N as ONE atomic Redis
-- operation, so the same bucket key can be safely shared by every
-- HoldCommandProcessor instance across all partitions of
-- scheduler.standard.commands AND every replica of scheduler-standard-lane
-- -- not per-partition, per-instance state anymore (RedisTokenBucket.java).
--
-- KEYS[1] = bucket key, e.g. "scheduler:standard:token_bucket:{stage_name}"
-- ARGV[1] = capacity (double, as string)
-- ARGV[2] = refill_per_second (double, as string)
-- ARGV[3] = now_epoch_ms (integer, as string) -- caller-supplied wall clock,
--           same accepted tradeoff as the rest of this codebase (no Redis
--           TIME command, no cross-node clock-skew detection -- see
--           scheduler-critical-sweep's README for the same disclosed
--           limitation elsewhere in this session)
-- ARGV[4] = requested (integer, as string)
--
-- Returns: granted (integer, 0..requested)

local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill_per_second = tonumber(ARGV[2])
local now_ms = tonumber(ARGV[3])
local requested = tonumber(ARGV[4])

local existing = redis.call('HMGET', key, 'tokens', 'last_refill_ms')
local tokens = tonumber(existing[1])
local last_refill_ms = tonumber(existing[2])

if tokens == nil then
    tokens = capacity
    last_refill_ms = now_ms
end

local elapsed_ms = now_ms - last_refill_ms
if elapsed_ms > 0 then
    local added = (elapsed_ms / 1000.0) * refill_per_second
    tokens = math.min(capacity, tokens + added)
    last_refill_ms = now_ms
end

local granted = math.min(requested, math.floor(tokens))
if granted < 0 then
    granted = 0
end
if granted > 0 then
    tokens = tokens - granted
end

redis.call('HSET', key, 'tokens', tostring(tokens), 'last_refill_ms', tostring(last_refill_ms))
-- Защита от бессрочного роста ключей для scope/stage, переставших
-- появляться (например stage_name, который больше не используется) --
-- TTL продлевается на каждый вызов, так что активные bucket'ы никогда не
-- истекают между тиками (releaseTick — раз в секунду).
redis.call('EXPIRE', key, 3600)

return granted
