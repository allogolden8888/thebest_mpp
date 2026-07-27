-- finalize_pipeline (service_internal_methods.md §1.4) — атомарное удаление
-- exec:{message_id} + ZREM последней записи из deadlines:{bucket}, той же
-- гарантией "не отдельный round trip", что cas_transition.lua.
--
-- KEYS[1] = exec:{message_id}
-- KEYS[2] = deadlines:{bucket}
-- ARGV[1] = expected_awaiting_stage_execution_id — тот же CAS-guard, что в
--           cas_transition.lua: финализировать можно только то состояние,
--           которое реально ожидали (не затереть работу другой реплики,
--           если между чтением события и вызовом finalize кто-то другой
--           уже успел продвинуть/финализировать то же message_id).
--
-- Возврат: {"OK"} либо {"CONFLICT", <реальный awaiting_stage_execution_id>}.

local exec_key = KEYS[1]
local deadlines_key = KEYS[2]
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

return {'OK'}
