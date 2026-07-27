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
-- ARGV[1] = charge_id
-- ARGV[2] = amount (minor units, integer as string)
-- ARGV[3] = expected_epoch (integer as string)
--
-- Возврат: {outcome, balance, state, epoch} — все элементы строки/числа,
-- разбирается на Java-стороне (BillingAccountStore.java).

local key = KEYS[1]
local charge_id = ARGV[1]
local amount = tonumber(ARGV[2])
local expected_epoch = tonumber(ARGV[3])

local state = redis.call('HGET', key, 'state')
local balance_raw = redis.call('HGET', key, 'balance')
local epoch_raw = redis.call('HGET', key, 'epoch')
local processed_raw = redis.call('HGET', key, 'processed_charge_ids')

-- Счёт ещё не существовал в Redis — тот же дефолт, что Account.fresh(0)
-- на Java-стороне: ACTIVE, epoch=0, пустой баланс/dedup-набор.
if state == false then
    state = 'ACTIVE'
    balance_raw = '0'
    epoch_raw = '0'
    processed_raw = ''
end

local balance = tonumber(balance_raw)
local epoch = tonumber(epoch_raw)

-- Дедупликация по charge_id — линейный скан по запятой-разделённому
-- списку, тот же формат хранения (и то же O(n) поведение, включая уже
-- задокументированный unbounded growth — не исправлено здесь, см.
-- billing-service/README.md), что в текущей Java-реализации.
local function contains_charge_id(csv, id)
    if csv == nil or csv == '' then
        return false
    end
    for candidate in string.gmatch(csv, '([^,]+)') do
        if candidate == id then
            return true
        end
    end
    return false
end

if contains_charge_id(processed_raw, charge_id) then
    return {'ALREADY_PROCESSED', tostring(balance), state, tostring(epoch)}
end

if epoch ~= expected_epoch then
    return {'STALE_EPOCH', tostring(balance), state, tostring(epoch)}
end

if state == 'FROZEN' then
    return {'ACCOUNT_FROZEN', tostring(balance), state, tostring(epoch)}
end

local new_balance = balance - amount
local new_processed
if processed_raw == nil or processed_raw == '' then
    new_processed = charge_id
else
    new_processed = processed_raw .. ',' .. charge_id
end

redis.call('HSET', key,
    'state', state,
    'balance', tostring(new_balance),
    'epoch', tostring(epoch),
    'processed_charge_ids', new_processed)

return {'APPLIED', tostring(new_balance), state, tostring(epoch)}
