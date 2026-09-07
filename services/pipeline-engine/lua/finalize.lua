-- finalize_pipeline (service_internal_methods.md §1.4) — атомарное удаление
-- exec:{message_id} + ZREM последней записи из deadlines:{bucket}, той же
-- гарантией "не отдельный round trip", что cas_transition.lua.
--
-- KEYS[1] = exec:{message_id}
-- KEYS[2] = deadlines:{bucket}
-- KEYS[3] = msgctx:{message_id} — освобождается ЗДЕСЬ, а не по TTL
-- ARGV[1] = expected_awaiting_stage_execution_id — тот же CAS-guard, что в
--           cas_transition.lua: финализировать можно только то состояние,
--           которое реально ожидали (не затереть работу другой реплики,
--           если между чтением события и вызовом finalize кто-то другой
--           уже успел продвинуть/финализировать то же message_id).
--
-- Возврат: {"OK"} либо {"CONFLICT", <реальный awaiting_stage_execution_id>}.

local exec_key = KEYS[1]
local deadlines_key = KEYS[2]
local msgctx_key = KEYS[3]
local expected = ARGV[1]

local current_awaiting = redis.call('HGET', exec_key, 'awaiting_stage_execution_id')
if current_awaiting == false then
    current_awaiting = ''
end

if current_awaiting ~= expected then
    return {'CONFLICT', current_awaiting}
end

if expected ~= '' then
    redis.call('ZREM', deadlines_key, expected)
end
redis.call('DEL', exec_key)
-- ИЗМЕРЕНО 2026-09-07: msgctx:{message_id} жил своим 24-часовым TTL даже
-- после того, как сообщение получило финальный статус и уже уехало в
-- ClickHouse/PostgreSQL. При 300 TPS это давало рост Runtime Redis примерно
-- на 100МБ каждые три минуты (за три прогона — 242 931 ключ / 144МБ,
-- свободная память VM 2145МБ -> 246МБ), а нехватка памяти оборачивалась
-- задержкой пробуждения потоков ~25мс на КАЖДЫЙ блокирующий хоп — это и был
-- главный источник хвостов латентности.
--
-- Контекст нужен только пока сообщение живо в пайплайне. Раз мы уже приняли
-- решение "терминально" и удаляем exec — msgctx не нужен тем более. TTL
-- остаётся страховкой ровно для тех сообщений, которые до терминальной
-- стадии НЕ дошли (упавшая реплика, потерянное событие).
--
-- В том же вызове, а не отдельным round trip: та же гарантия атомарности,
-- ради которой здесь вообще Lua, и ноль дополнительной латентности.
redis.call('DEL', msgctx_key)

return {'OK'}
