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
-- ARGV[1] = charge_id
-- ARGV[2] = amount (minor units, integer as string)
-- ARGV[3] = expected_epoch (integer as string)
--
-- Возврат: {outcome, balance, state, epoch} — все элементы строки/числа,
-- разбирается на Java-стороне (BillingAccountStore.java).

local key = KEYS[1]
local charges_key = KEYS[2]
local charge_id = ARGV[1]
local amount = tonumber(ARGV[2])
local expected_epoch = tonumber(ARGV[3])

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

return {'APPLIED', tostring(new_balance), state, tostring(epoch)}
