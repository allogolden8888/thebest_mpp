package uz.mpp.billing;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import uz.mpp.billing.BillingAccountState.Account;
import uz.mpp.billing.BillingAccountState.AccountState;

import java.util.Set;
import java.util.stream.Collectors;

/**
 * Billing Redis — {@code check_account_state}/{@code apply_atomic_charge}
 * (service_internal_methods.md §1.6). Реальная реализация на Lettuce,
 * компилируется, live не проверена (см. README — та же оговорка, что у
 * {@code RedisMessageContextStore} в Policy Service).
 *
 * <p><b>Важно:</b> в реальном проде {@code apply_atomic_charge} — ОДИН
 * атомарный Lua/Redis Function вызов (development_plan.md 4.2, ещё не
 * написан), не read-then-write из Java, как здесь. Read-then-write из
 * приложения имеет TOCTOU race ровно того типа, который fencing по epoch
 * призван устранять НА СТОРОНЕ REDIS — в этой реализации гонка всё ещё
 * возможна между {@link #fetch} и {@link #save} двух параллельных
 * инстансов Billing Service. Это временная реализация для среза Фазы 2,
 * не production-корректная замена Lua-скрипта.
 */
public final class BillingAccountStore {

    private final RedisClient client;

    public BillingAccountStore(String redisUrl) {
        this.client = RedisClient.create(redisUrl);
    }

    public Account fetch(String accountId) {
        try (StatefulRedisConnection<String, String> connection = client.connect()) {
            RedisCommands<String, String> commands = connection.sync();
            String key = "billing:account:" + accountId;
            var fields = commands.hgetall(key);
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

    public void save(String accountId, Account account) {
        try (StatefulRedisConnection<String, String> connection = client.connect()) {
            RedisCommands<String, String> commands = connection.sync();
            String key = "billing:account:" + accountId;
            commands.hset(key, java.util.Map.of(
                "state", account.state().name(),
                "balance", String.valueOf(account.balance()),
                "epoch", String.valueOf(account.epoch()),
                "processed_charge_ids", account.processedChargeIds().stream().collect(Collectors.joining(","))
            ));
        }
    }
}
