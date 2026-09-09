-- submit_claim — атомарный claim stage_execution_id под реальный submit
-- оператору. Один вызов вместо двух round trip'ов, той же гарантией "не
-- отдельный round trip", что pipeline-engine/lua/cas_transition.lua.
--
-- KEYS[1] = dlvsubmit:{stage_execution_id}
--
-- ARGV[1] = deterministic queue_msg_id ("dlv-{stage_execution_id}", см.
--           KafkaIo.processRecord — стабильный, а не UUID на каждую попытку)
-- ARGV[2] = ttl_seconds для ключа claim'а
--
-- Возврат: {"WON"} — claim захвачен этим вызовом, реальный submit разрешён;
--          {"EXISTS", field, value, ...} — claim уже занят, дальше плоский
--          HGETALL того же хэша (status/outcome/reason_code/smsc_message_id/
--          queue_msg_id), по которому вызывающая сторона отличает
--          AlreadyDone от AmbiguousInFlight.
--
-- ЗАЧЕМ ОДНИМ СКРИПТОМ, две причины, обе измеренные, не стилистические.
--
-- 1) Лишний round trip на КАЖДОЕ сообщение горячего пути. Раньше выигравшая
--    ветка делала HSETNX, затем отдельный EXPIRE, а проигравшая — HSETNX,
--    затем отдельный HGETALL: в обеих ветках два последовательных park'а
--    вместо одного. По JFR на 300 TPS каждый park+wakeup стоит ~25мс из-за
--    контеншена за CPU (то же обоснование, что в recordOutcomeAsync, где
--    HSET+EXPIRE уже свели в один конвейер), а при 300 TPS это ещё и 300
--    лишних round trip'ов в секунду к Runtime Redis.
--
-- 2) Неатомарность пары HSETNX+EXPIRE. Если процесс падал между ними, ключ
--    оставался БЕЗ TTL — навсегда, то есть вечная утечка в Runtime Redis.
--    Это ровно тот класс, который уже был измерен на этом стенде: рост
--    Runtime Redis упирался в память VM (2145МБ -> 246МБ свободных за три
--    прогона), а нехватка памяти оборачивалась теми самыми ~25мс задержки
--    пробуждения на каждый блокирующий хоп (см. finalize.lua). Внутри
--    скрипта Redis выполняет обе команды как один неделимый шаг — окна для
--    падения между ними физически не существует.
--
-- Побочно скрипт закрывает и TOCTOU-щель проигравшей ветки: раньше между
-- HSETNX и HGETALL другая реплика могла успеть дописать исход, и вызывающая
-- сторона видела бы несогласованный срез хэша. Здесь HGETALL читает ровно
-- то состояние, которое видел HSETNX.

local key = KEYS[1]
local queue_msg_id = ARGV[1]
local ttl_seconds = ARGV[2]

if redis.call('HSETNX', key, 'queue_msg_id', queue_msg_id) == 1 then
    -- TTL выставляется В ТОМ ЖЕ вызове, что и сам claim — см. пункт 2 выше.
    redis.call('EXPIRE', key, ttl_seconds)
    return {'WON'}
end

local existing = redis.call('HGETALL', key)
table.insert(existing, 1, 'EXISTS')
return existing
