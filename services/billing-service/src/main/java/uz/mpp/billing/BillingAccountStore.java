package uz.mpp.billing;

import io.lettuce.core.RedisClient;
import io.lettuce.core.TransactionResult;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import uz.mpp.billing.BillingAccountState.Account;
import uz.mpp.billing.BillingAccountState.AccountState;
import uz.mpp.billing.BillingAccountState.ChargeOutcome;
import uz.mpp.billing.BillingAccountState.ChargeResult;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Billing Redis — {@code check_account_state}/{@code apply_atomic_charge}
 * (service_internal_methods.md §1.6).
 *
 * <p><b>Известное ограничение, сужено кодревью — исправлено, не только
 * задокументировано.</b> Первая версия делала read-then-write двумя
 * независимыми {@code fetch()}/{@code save()} вызовами — реальный TOCTOU race
 * между двумя репликами Billing Service: обычные ACTIVE-списания НЕ двигают
 * {@code epoch} (только freeze/unfreeze), значит epoch fencing НЕ защищает от
 * двух конкурентных charge к одному {@code account_id} — обе реплики читают
 * один и тот же баланс, обе проходят проверку, {@code save()} — plain
 * {@code hset}, не CAS, вторая запись затирает первую без следа (списание
 * теряется молча, charge_id никогда не попадает в processedChargeIds).
 *
 * <p>Настоящий production-фикс — один атомарный Lua/Redis Function
 * (development_plan.md 4.2, ещё не написан). До него — {@link #applyChargeAtomically}
 * использует Redis {@code WATCH}/{@code MULTI}/{@code EXEC} (оптимистичная
 * блокировка на стороне клиента Lettuce): если ключ счёta изменился между
 * {@code WATCH} и {@code EXEC} (та самая гонка), транзакция откатывается
 * ({@code EXEC} возвращает discarded), и попытка повторяется — race
 * обнаруживается и разрешается повтором, а не тихо проигрывается. Это не
 * полная замена Lua-скрипта (тот убирает round-trip'ы и сетевой race
 * WATCH-to-MULTI полностью), но устраняет именно ту потерю данных, на
 * которую указало кодревью.
 */
public final class BillingAccountStore {

    private static final int MAX_RETRIES = 5;

    private final RedisClient client;

    public BillingAccountStore(String redisUrl) {
        this.client = RedisClient.create(redisUrl);
    }

    public void close() {
        client.shutdown();
    }

    /**
     * Read-only просмотр состояния — используется вызывающей стороной только
     * чтобы определить {@code expectedEpoch} перед вызовом
     * {@link #applyChargeAtomically}. Сам по себе не защищает от гонки (это
     * всего лишь чтение) — атомарность обеспечивает WATCH/MULTI/EXEC внутри
     * {@link #applyChargeAtomically}, не этот метод.
     */
    public Account peek(String accountId) {
        try (StatefulRedisConnection<String, String> connection = client.connect()) {
            return readAccount(connection.sync(), "billing:account:" + accountId);
        }
    }

    /**
     * Атомарно (WATCH/MULTI/EXEC) читает счёт, применяет {@link BillingAccountState#applyCharge},
     * и если исход {@code APPLIED} — записывает результат в той же транзакции.
     * При обнаруженной гонке (конкурентная запись между WATCH и EXEC) —
     * повторяет попытку до {@value #MAX_RETRIES} раз.
     */
    public ChargeResult applyChargeAtomically(String accountId, String chargeId, long amount, long expectedEpoch) {
        String key = "billing:account:" + accountId;
        try (StatefulRedisConnection<String, String> connection = client.connect()) {
            RedisCommands<String, String> commands = connection.sync();
            for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
                commands.watch(key);
                Account current = readAccount(commands, key);
                ChargeResult result = BillingAccountState.applyCharge(current, chargeId, amount, expectedEpoch);

                if (result.outcome() != ChargeOutcome.APPLIED) {
                    commands.unwatch();
                    return result; // ALREADY_PROCESSED/ACCOUNT_FROZEN/STALE_EPOCH — ничего не пишем
                }

                commands.multi();
                writeAccount(commands, key, result.account());
                TransactionResult execResult = commands.exec();
                if (!execResult.wasDiscarded()) {
                    return result; // успешно закоммичено
                }
                // WATCH обнаружил конкурентное изменение ключа между WATCH и EXEC — повтор.
            }
        }
        throw new IllegalStateException(
            "не удалось применить charge после " + MAX_RETRIES + " попыток — высокая конкуренция за account_id=" + accountId
                + " (WATCH/MULTI/EXEC retry limit exceeded, не Lua — см. javadoc класса)");
    }

    private Account readAccount(RedisCommands<String, String> commands, String key) {
        Map<String, String> fields = commands.hgetall(key);
        if (fields.isEmpty()) {
            return Account.fresh(0);
        }
        AccountState state = AccountState.valueOf(fields.get("state"));
        long balance = Long.parseLong(fields.get("balance"));
        long epoch = Long.parseLong(fields.get("epoch"));
        Set<String> processedChargeIds = fields.containsKey("processed_charge_ids") && !fields.get("processed_charge_ids").isEmpty()
            ? Set.of(fields.get("processed_charge_ids").split(","))
            : Set.of();
        return new Account(balance, state, epoch, processedChargeIds);
    }

    private void writeAccount(RedisCommands<String, String> commands, String key, Account account) {
        commands.hset(key, Map.of(
            "state", account.state().name(),
            "balance", String.valueOf(account.balance()),
            "epoch", String.valueOf(account.epoch()),
            "processed_charge_ids", account.processedChargeIds().stream().collect(Collectors.joining(","))
        ));
    }
}
