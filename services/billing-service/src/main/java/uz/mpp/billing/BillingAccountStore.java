package uz.mpp.billing;

import io.lettuce.core.RedisClient;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import uz.mpp.billing.BillingAccountState.Account;
import uz.mpp.billing.BillingAccountState.AccountState;
import uz.mpp.billing.BillingAccountState.ChargeOutcome;
import uz.mpp.billing.BillingAccountState.ChargeResult;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Billing Redis — {@code check_account_state}/{@code apply_atomic_charge}
 * (service_internal_methods.md §1.6, development_plan.md 4.2).
 *
 * <p><b>Реальный production-фикс, не оптимистичная блокировка.</b> Первая
 * версия этого класса делала read-then-write (реальный TOCTOU race, найдено
 * кодревью), вторая — WATCH/MULTI/EXEC (обнаруживает гонку и повторяет,
 * задокументировано в истории коммитов). Эта версия — настоящий атомарный
 * Lua-скрипт ({@code apply_atomic_charge.lua}, ресурс classpath), 1:1 порт
 * {@link BillingAccountState#applyCharge} — вся проверка/списание выполняется
 * Redis'ом как один неделимый шаг на стороне сервера, конкурентная запись с
 * другой реплики физически не может вклиниться между чтением и записью (не
 * "race обнаруживается и разрешается повтором", а "race невозможен
 * структурно") — убирает и сетевой round-trip WATCH-to-MULTI, и сам retry-цикл.
 */
public final class BillingAccountStore {

    private static final String SCRIPT = loadScript();

    private final RedisClient client;

    public BillingAccountStore(String redisUrl) {
        this.client = RedisClient.create(redisUrl);
    }

    public void close() {
        client.shutdown();
    }

    private static String loadScript() {
        try (InputStream in = BillingAccountStore.class.getClassLoader().getResourceAsStream("apply_atomic_charge.lua")) {
            if (in == null) {
                throw new IllegalStateException("apply_atomic_charge.lua не найден в classpath");
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException("не удалось прочитать apply_atomic_charge.lua", e);
        }
    }

    /**
     * Read-only просмотр состояния — используется вызывающей стороной только
     * чтобы определить {@code expectedEpoch} перед вызовом
     * {@link #applyChargeAtomically}. Сам по себе не защищает от гонки (это
     * всего лишь чтение) — атомарность обеспечивает Lua-скрипт внутри
     * {@link #applyChargeAtomically}, не этот метод: между этим чтением и
     * вызовом {@link #applyChargeAtomically} epoch/баланс могут измениться,
     * для этого и существует сам {@code expectedEpoch}-параметр (STALE_EPOCH
     * отклонит устаревший вызов).
     */
    public Account peek(String accountId) {
        try (StatefulRedisConnection<String, String> connection = client.connect()) {
            return readAccount(connection.sync(), "billing:account:" + accountId);
        }
    }

    /**
     * Один атомарный Lua-вызов — {@code apply_atomic_charge.lua}, тот же
     * порядок проверок (charge_id dedup -> account_epoch -> account_state),
     * что {@link BillingAccountState#applyCharge}, который этот скрипт
     * обязан воспроизводить один-в-один (доказано
     * {@code BillingAccountStoreTest} против живого Redis).
     */
    @SuppressWarnings("unchecked")
    public ChargeResult applyChargeAtomically(String accountId, String chargeId, long amount, long expectedEpoch) {
        String key = "billing:account:" + accountId;
        try (StatefulRedisConnection<String, String> connection = client.connect()) {
            RedisCommands<String, String> commands = connection.sync();
            List<Object> result = (List<Object>) commands.eval(
                SCRIPT, ScriptOutputType.MULTI, new String[] {key}, chargeId, String.valueOf(amount), String.valueOf(expectedEpoch));

            String outcomeName = (String) result.get(0);
            long balance = Long.parseLong((String) result.get(1));
            AccountState state = AccountState.valueOf((String) result.get(2));
            long epoch = Long.parseLong((String) result.get(3));

            ChargeOutcome outcome = ChargeOutcome.valueOf(outcomeName);
            Set<String> processedChargeIds = outcome == ChargeOutcome.APPLIED
                ? peekProcessedChargeIds(commands, key)
                : Set.of();
            Account account = new Account(balance, state, epoch, processedChargeIds);
            return new ChargeResult(account, outcome);
        }
    }

    /**
     * Только для заполнения {@link Account#processedChargeIds()} в
     * возвращаемом результате (вызывающая сторона нигде не читает этот
     * набор из результата напрямую в проде — используется только в тестах
     * для проверки, что charge_id реально добавлен); отдельный HGET, не
     * часть атомарности — набор уже гарантированно обновлён Lua-скриптом
     * до этого чтения.
     */
    private Set<String> peekProcessedChargeIds(RedisCommands<String, String> commands, String key) {
        String raw = commands.hget(key, "processed_charge_ids");
        if (raw == null || raw.isEmpty()) {
            return Set.of();
        }
        return Set.of(raw.split(","));
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
}
