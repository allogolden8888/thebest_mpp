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
    // Реальная находка (нагрузочный прогон): peek()/applyChargeAtomically()
    // раньше каждый открывали НОВОЕ соединение (client.connect() в
    // try-with-resources) — на КАЖДЫЙ charge это два полных TCP-хендшейка +
    // Redis AUTH вместо одного переиспользуемого соединения. Под
    // конкурентной обработкой (см. KafkaIo.run — теперь пул потоков, не
    // последовательный цикл) это стало бы ещё заметнее: тот же класс
    // находки, что уже был исправлен в delivery-service (MessageContextStore/
    // GatewayRegistry) — Lettuce StatefulRedisConnection документированно
    // потокобезопасно для конкурентного использования из многих потоков
    // (внутреннее мультиплексирование поверх одного сокета), держим его на
    // весь жизненный цикл сервиса, не на вызов.
    private final StatefulRedisConnection<String, String> connection;

    public BillingAccountStore(String redisUrl) {
        this.client = RedisClient.create(redisUrl);
        this.connection = client.connect();
    }

    public void close() {
        connection.close();
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
        return readAccount(connection.sync(), "billing:account:" + accountId);
    }

    private static String chargesKey(String accountId) {
        return "billing:account:" + accountId + ":charges";
    }

    /**
     * Облегчённая версия {@link #peek} только для {@code expectedEpoch} —
     * реальная находка (JFR-профилирование под нагрузкой): вызывающая
     * сторона (KafkaIo/RecurringBillingJob) до этого фикса дёргала
     * {@code peek(accountId).epoch()} на КАЖДОЕ сообщение, что тянуло за
     * собой {@link #readAccount} и его {@code Set.of(raw.split(","))} —
     * пересборку всей (растущей без ограничения) истории charge_id
     * аккаунта ради одного числа. Один точечный HGET вместо HGETALL +
     * парсинг всех полей.
     */
    public long peekEpoch(String accountId) {
        String raw = connection.sync().hget("billing:account:" + accountId, "epoch");
        return raw == null ? 0 : Long.parseLong(raw);
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
        RedisCommands<String, String> commands = connection.sync();
        List<Object> result = (List<Object>) commands.eval(
            SCRIPT, ScriptOutputType.MULTI, new String[] {key, chargesKey(accountId)},
            chargeId, String.valueOf(amount), String.valueOf(expectedEpoch));

        String outcomeName = (String) result.get(0);
        long balance = Long.parseLong((String) result.get(1));
        AccountState state = AccountState.valueOf((String) result.get(2));
        long epoch = Long.parseLong((String) result.get(3));

        ChargeOutcome outcome = ChargeOutcome.valueOf(outcomeName);
        // Реальная находка (JFR-профилирование под нагрузкой): раньше здесь
        // после каждого APPLIED делался ещё один HGET processed_charge_ids
        // + String.split(",") + Set.of(...) — пересборка ВСЕЙ (растущей без
        // ограничения, см. apply_atomic_charge.lua) истории charge_id
        // аккаунта на каждое единичное списание. `Set.of()` из этого
        // распада доминировал в CPU-профиле (java.util.ImmutableCollections
        // $SetN.probe — 62% всех ExecutionSample-сэмплов на реальном
        // прогоне). Ни один вызывающий код не читает
        // Account#processedChargeIds() из ВОЗВРАЩАЕМОГО результата этого
        // метода ни в проде (KafkaIo/BillingService.buildEvent используют
        // только outcome()), ни в тестах (BillingAccountStoreTest проверяет
        // dedup-набор через отдельный store.peek(), не через этот возврат) —
        // само поле здесь чистый мёртвый груз.
        Account account = new Account(balance, state, epoch, Set.of());
        return new ChargeResult(account, outcome);
    }

    private Account readAccount(RedisCommands<String, String> commands, String key) {
        Map<String, String> fields = commands.hgetall(key);
        if (fields.isEmpty()) {
            return Account.fresh(0);
        }
        AccountState state = AccountState.valueOf(fields.get("state"));
        long balance = Long.parseLong(fields.get("balance"));
        long epoch = Long.parseLong(fields.get("epoch"));
        // dedup-набор — отдельный Redis SET (billing:account:{id}:charges),
        // не поле этого хэша, см. apply_atomic_charge.lua. Ключ аккаунта —
        // "billing:account:{id}", отсюда и достаём accountId обратно для
        // SMEMBERS, не идеально красиво, но readAccount() — не hot path
        // (peek() используется только в тестах после fix'а peekEpoch()).
        String accountId = key.substring("billing:account:".length());
        Set<String> processedChargeIds = commands.smembers(chargesKey(accountId));
        return new Account(balance, state, epoch, processedChargeIds);
    }
}
