-- cas_transition_and_track_deadline (service_internal_methods.md §1.4,
-- development_plan.md 4.2). Один атомарный Lua-вызов: CAS Execution State
-- в exec:{message_id} + ZADD/ZREM в deadlines:{bucket} (data_infrastructure_spec.md
-- §1.4/§282-283) — не отдельный round trip, ровно как задокументировано в
-- hld.md:770/912.
--
-- KEYS[1] = exec:{message_id}
-- KEYS[2] = deadlines:{bucket}      (bucket = hash(new_stage_execution_id) % N, считает вызывающая сторона)
--
-- ARGV[1]  = expected_awaiting_stage_execution_id ("" = ожидаем свежий/несуществующий ключ)
-- ARGV[2]  = new pipeline_version
-- ARGV[3]  = new current_node_id
-- ARGV[4]  = new attempt
-- ARGV[5]  = new awaiting_stage_execution_id
-- ARGV[6]  = new resolved_operator_id ("" если нет)
-- ARGV[7]  = new category ("" если нет)
-- ARGV[8]  = new segment_count
-- ARGV[9]  = new route_id ("" если нет)
-- ARGV[10] = new protocol (-1 если нет)
-- ARGV[11] = new route_version ("" если нет)
-- ARGV[12] = new destination_address
-- ARGV[13] = new deadline_ms (unix ms)
-- ARGV[14] = old_stage_execution_id для ZREM из deadlines ("" если нечего удалять — первый диспетч)
-- ARGV[15] = new priority_flag (SMPP 0-3, партнёрское поле, не меняется по ходу пайплайна)
-- ARGV[16] = new message_ttl_ms (unix ms, i64::MAX если TTL не задан)
--
-- Возврат: {"OK"} при успехе, {"CONFLICT", <реальный awaiting_stage_execution_id>} при гонке.
--
-- Реальная находка при реализации: CAS-проверка "expected" ЗАМЕНЯЕТ отдельный
-- "уже существует ли состояние" пре-чек, который раньше делался отдельным
-- (не атомарным) чтением перед вызовом handle_incoming — тот пречек сам был
-- потенциальным TOCTOU (два реплики могли обе увидеть "не существует" и обе
-- начать строить состояние). Здесь этот же CAS-вызов с expected="" одновременно
-- и есть идемпотентность на входе: только ОДНА из двух гоняющихся попыток
-- получит совпадение.

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

local old_stage_execution_id = ARGV[14]
if old_stage_execution_id ~= '' then
    redis.call('ZREM', deadlines_key, old_stage_execution_id)
end

redis.call('HSET', exec_key,
    'pipeline_version', ARGV[2],
    'current_node_id', ARGV[3],
    'attempt', ARGV[4],
    'awaiting_stage_execution_id', ARGV[5],
    'resolved_operator_id', ARGV[6],
    'category', ARGV[7],
    'segment_count', ARGV[8],
    'route_id', ARGV[9],
    'protocol', ARGV[10],
    'route_version', ARGV[11],
    'destination_address', ARGV[12],
    'deadline_ms', ARGV[13],
    'priority_flag', ARGV[15],
    'message_ttl_ms', ARGV[16])

local new_stage_execution_id = ARGV[5]
if new_stage_execution_id ~= '' then
    redis.call('ZADD', deadlines_key, ARGV[13], new_stage_execution_id)
end

return {'OK'}
