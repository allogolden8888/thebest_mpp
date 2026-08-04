package uz.mpp.billingreconciliation.redisio;

import io.lettuce.core.RedisClient;
import io.lettuce.core.TransactionResult;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import uz.mpp.billingreconciliation.core.Account;
import uz.mpp.billingreconciliation.core.AccountState;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * billing:balance:{account_id} HASH (data_infrastructure_spec.md §2.3) —
 * read + apply_fenced_cas (service_internal_methods.md §5.3).
 *
 * <p><b>Известное упрощение</b>: production-атомарность для {@code apply_charge}
 * — Billing Redis Lua/Redis Function (development_plan.md 4.2, владеет
 * Главный агент). Здесь для recovery (fenced unfreeze) используется
 * `WATCH`/`MULTI`/`EXEC` — легитимный нативный Redis-механизм оптимистичной
 * блокировки, не тот же Lua-скрипт, что понадобится Billing Service для
 * hot-path `apply_charge` (там нужна более высокая пропускная способность,
 * WATCH/MULTI не годится под 20 000 msg/s). Для низкочастотного recovery-пути
 * Reconciliation WATCH/MULTI/EXEC достаточно и корректно.
 */
public final class BillingRedisClient implements AutoCloseable {

    private final RedisClient client;
    private final StatefulRedisConnection<String, String> connection;
    private final RedisCommands<String, String> commands;

    public BillingRedisClient(String redisUri) {
        this.client = RedisClient.create(redisUri);
        this.connection = client.connect();
        this.commands = connection.sync();
    }

    BillingRedisClient(RedisCommands<String, String> commands) {
        this.client = null;
        this.connection = null;
        this.commands = commands;
    }

    private static String key(String accountId) {
        return "billing:balance:" + accountId;
    }

    /** Читает текущий Account из Redis. */
    public Account readAccount(String accountId) {
        Map<String, String> fields = commands.hgetall(key(accountId));
        if (fields.isEmpty()) {
            throw new IllegalStateException("billing:balance:" + accountId + " не найден");
        }
        AccountState state = "FROZEN".equals(fields.get("account_state")) ? AccountState.FROZEN : AccountState.ACTIVE;
        long balance = Long.parseLong(fields.get("balance"));
        long epoch = Long.parseLong(fields.get("account_epoch"));
        // processedChargeIds не хранится в billing:balance (это billing:charge:{charge_id}
        // отдельные ключи, data_infrastructure_spec.md §2.3) — не нужен для fenced CAS recovery.
        return new Account(balance, state, epoch, Set.of());
    }

    /**
     * apply_fenced_cas — WATCH текущего epoch, затем MULTI/EXEC, записывающий
     * ACTIVE/recomputedBalance/epoch+1, только если epoch не изменился между
     * WATCH и EXEC (optimistic concurrency — та же fencing-гарантия, что
     * unfreeze() в core/BillingAccountStateMachine, здесь — реальный round-trip
     * к Redis, не только in-memory модель).
     *
     * @return true, если CAS применён; false — конфликт (кто-то изменил epoch), retry нужен.
     */
    public boolean applyFencedUnfreeze(String accountId, long recomputedBalance, long expectedEpoch) {
        String k = key(accountId);
        commands.watch(k);

        String currentEpochStr = commands.hget(k, "account_epoch");
        long currentEpoch = currentEpochStr == null ? -1 : Long.parseLong(currentEpochStr);
        if (currentEpoch != expectedEpoch) {
            commands.unwatch();
            return false;
        }

        commands.multi();
        commands.hset(k, Map.of(
            "account_state", "ACTIVE",
            "balance", Long.toString(recomputedBalance),
            "account_epoch", Long.toString(expectedEpoch + 1)
        ));
        TransactionResult result = commands.exec();
        return !result.wasDiscarded();
    }

    /** trigger_freeze — идемпотентно, побеждает конкурентную гонку так же, как core/BillingAccountStateMachine.freeze. */
    public void freeze(String accountId) {
        String k = key(accountId);
        while (true) {
            commands.watch(k);
            Map<String, String> fields = commands.hgetall(k);
            if ("FROZEN".equals(fields.get("account_state"))) {
                commands.unwatch();
                return;
            }
            long epoch = Long.parseLong(fields.get("account_epoch"));
            commands.multi();
            commands.hset(k, Map.of("account_state", "FROZEN", "account_epoch", Long.toString(epoch + 1)));
            TransactionResult result = commands.exec();
            if (!result.wasDiscarded()) {
                return;
            }
            // WATCH конфликт — кто-то изменил счёт параллельно, повторяем.
        }
    }

    @Override
    public void close() {
        if (connection != null) {
            connection.close();
        }
        if (client != null) {
            client.shutdown();
        }
    }
}