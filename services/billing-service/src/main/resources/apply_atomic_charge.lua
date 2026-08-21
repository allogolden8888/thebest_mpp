-- apply_atomic_charge (service_internal_methods.md §1.6, development_plan.md 4.2)
--
-- Атомарная реализация BillingAccountState.applyCharge
-- (uz.mpp.billing.BillingAccountState, порт state_machines/billing_account_state.py)
-- — тот же порядок трёх проверок (charge_id dedup -> account_epoch fencing ->
-- account_state), тот же исход при списании. Заменяет WATCH/MULTI/EXEC
-- оптимистичную блокировку (BillingAccountStore.java) настоящей атомарностью:
-- вся Lua-функция выполняется Redis'ом как один неделимый шаг, конкурентная
-- запись с другой реплики физически не может вклиниться между чтением и
-- записью — не "race обнаруживается и разрешается повтором" (WATCH-подход),
-- а "race невозможен структурно".
--
-- KEYS[1] = billing:account:{account_id}
-- KEYS[2] = billing:account:{account_id}:charges — Redis SET, dedup по
--           charge_id. Реальная находка нагрузочного прогона (JFR +
--           thread dump + redis INFO stats под 1000 msg/s): раньше
--           processed_charge_ids хранился запятой-разделённой строкой в
--           этом же хэше и сканировался ЛИНЕЙНО (string.gmatch) на КАЖДОЕ
--           списание — уже задокументированный, но не исправленный
--           unbounded growth. Биллинг здесь per-partner, не per-msisdn
--           (один Redis-ключ на партнёра), так что для одного активного
--           партнёра эта строка реально доросла до 47k+ charge_id за
--           сессию нагрузочных прогонов — Redis (однопоточный) упёрся в
--           100% CPU при ~300 ops/sec просто на O(n) скан+перезапись
--           растущей строки. SADD/SISMEMBER — нативный O(1) hash-set на
--           стороне Redis, без этого роста стоимости на операцию.
-- KEYS[3] = billing:outbox:{shard} — transactional outbox. Реальная
--           находка: раньше скрипт списывал баланс, но НИЧЕГО не писал в
--           outbox, при том что billing-outbox-publisher (который читает
--           этот стрим и публикует в топик billing.ledger) существовал и
--           был написан целиком. В результате billing.ledger был пуст
--           всегда, billing.billing_ledger не наполнялся ни одной реальной
--           строкой, и финансовая отчётность показывала только то, что
--           тесты писали в Postgres напрямую — деньги списывались, а
--           запись о списании не появлялась нигде.
--
--           XADD делается ЗДЕСЬ, внутри той же Lua-функции, а не отдельным
--           вызовом из Java — в этом весь смысл transactional outbox:
--           списание и запись о нём атомарны. Отдельный XADD после
--           EVAL мог бы не выполниться (падение процесса между вызовами),
--           и деньги ушли бы без следа в реестре.
--
-- ARGV[1] = charge_id
-- ARGV[2] = amount (minor units, integer as string)
-- ARGV[3] = expected_epoch (integer as string)
-- ARGV[4] = account_id
-- ARGV[5] = partner_id
-- ARGV[6] = currency_code
-- ARGV[7] = created_at_epoch_ms
--
-- Возврат: {outcome, balance, state, epoch} — все элементы строки/числа,
-- разбирается на Java-стороне (BillingAccountStore.java).

local key = KEYS[1]
local charges_key = KEYS[2]
local outbox_key = KEYS[3]
local charge_id = ARGV[1]
local amount = tonumber(ARGV[2])
local expected_epoch = tonumber(ARGV[3])
local account_id = ARGV[4]
local partner_id = ARGV[5]
local currency_code = ARGV[6]
local created_at_epoch_ms = ARGV[7]

local state = redis.call('HGET', key, 'state')
local balance_raw = redis.call('HGET', key, 'balance')
local epoch_raw = redis.call('HGET', key, 'epoch')

-- Счёт ещё не существовал в Redis — тот же дефолт, что Account.fresh(0)
-- на Java-стороне: ACTIVE, epoch=0, пустой баланс.
if state == false then
    state = 'ACTIVE'
    balance_raw = '0'
    epoch_raw = '0'
end

local balance = tonumber(balance_raw)
local epoch = tonumber(epoch_raw)

if redis.call('SISMEMBER', charges_key, charge_id) == 1 then
    return {'ALREADY_PROCESSED', tostring(balance), state, tostring(epoch)}
end

if epoch ~= expected_epoch then
    return {'STALE_EPOCH', tostring(balance), state, tostring(epoch)}
end

if state == 'FROZEN' then
    return {'ACCOUNT_FROZEN', tostring(balance), state, tostring(epoch)}
end

local new_balance = balance - amount

redis.call('HSET', key, 'state', state, 'balance', tostring(new_balance), 'epoch', tostring(epoch))
redis.call('SADD', charges_key, charge_id)

-- Transactional outbox: запись о списании появляется ровно тогда же, когда
-- само списание, в одном неделимом шаге. Имена полей — контракт
-- billing-outbox-publisher's OutboxStreamReader.toStreamEntry, менять
-- только вместе с ним. source_charge_id намеренно пустой: это списание
-- (entry_type=charge), а не компенсация — Postgres-констрейнт
-- billing_ledger_charge_source_forbidden требует NULL для charge, и
-- LedgerEventBuilder трактует пустую строку именно так.
redis.call('XADD', outbox_key, '*',
    'charge_id', charge_id,
    'account_id', account_id,
    'partner_id', partner_id,
    'amount_minor_units', tostring(amount),
    'currency_code', currency_code,
    'entry_type', 'charge',
    'source_charge_id', '',
    'reason', '',
    'created_at_epoch_ms', created_at_epoch_ms)

return {'APPLIED', tostring(new_balance), state, tostring(epoch)}
