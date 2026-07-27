package uz.mpp.billing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import uz.mpp.billing.BillingAccountState.AccountState;
import uz.mpp.billing.BillingAccountState.ChargeOutcome;
import uz.mpp.billing.BillingAccountState.ChargeResult;

/**
 * Реальные round-trip'ы против локального Redis (brew, `redis-server`) —
 * не мок. Прямая проверка ЗАЧЕМ вообще написан {@code apply_atomic_charge.lua}:
 * {@link #concurrentChargesOnSameAccountNeverLoseAWrite} воспроизводит ровно
 * ту гонку, которую нашло кодревью (TOCTOU между read-then-write двумя
 * репликами) — с настоящим Lua-скриптом она структурно невозможна, не
 * "обнаружена и разрешена повтором".
 */
class BillingAccountStoreTest {

    private static final String REDIS_URL = System.getenv().getOrDefault("BILLING_ACCOUNT_STORE_TEST_REDIS_URL", "redis://localhost:6379/0");

    private BillingAccountStore store;
    private RedisClient rawClient;
    private String accountId;

    @BeforeEach
    void setUp() {
        store = new BillingAccountStore(REDIS_URL);
        rawClient = RedisClient.create(REDIS_URL);
        accountId = "test-account-" + UUID.randomUUID();
    }

    @AfterEach
    void tearDown() {
        try (StatefulRedisConnection<String, String> conn = rawClient.connect()) {
            conn.sync().del("billing:account:" + accountId);
        }
        store.close();
        rawClient.shutdown();
    }

    private void seedRaw(Map<String, String> fields) {
        try (StatefulRedisConnection<String, String> conn = rawClient.connect()) {
            conn.sync().hset("billing:account:" + accountId, fields);
        }
    }

    @Test
    void firstChargeOnFreshAccountIsApplied() {
        ChargeResult result = store.applyChargeAtomically(accountId, "charge-1", 500, 0);
        assertEquals(ChargeOutcome.APPLIED, result.outcome());
        assertEquals(-500, result.account().balance());
        assertEquals(AccountState.ACTIVE, result.account().state());
    }

    @Test
    void duplicateChargeIdIsAlreadyProcessedNotDoubleCharged() {
        ChargeResult first = store.applyChargeAtomically(accountId, "charge-1", 500, 0);
        assertEquals(ChargeOutcome.APPLIED, first.outcome());

        ChargeResult second = store.applyChargeAtomically(accountId, "charge-1", 500, 0);
        assertEquals(ChargeOutcome.ALREADY_PROCESSED, second.outcome());
        assertEquals(-500, second.account().balance(), "повтор того же charge_id не должен списывать второй раз");
    }

    @Test
    void wrongExpectedEpochIsRejectedAsStale() {
        seedRaw(Map.of("state", "ACTIVE", "balance", "0", "epoch", "5", "processed_charge_ids", ""));
        ChargeResult result = store.applyChargeAtomically(accountId, "charge-1", 500, 0 /* устаревший epoch */);
        assertEquals(ChargeOutcome.STALE_EPOCH, result.outcome());
        assertEquals(0, result.account().balance(), "STALE_EPOCH не должен изменять баланс");
    }

    @Test
    void frozenAccountRejectsCharge() {
        seedRaw(Map.of("state", "FROZEN", "balance", "1000", "epoch", "1", "processed_charge_ids", ""));
        ChargeResult result = store.applyChargeAtomically(accountId, "charge-1", 500, 1);
        assertEquals(ChargeOutcome.ACCOUNT_FROZEN, result.outcome());
        assertEquals(1000, result.account().balance(), "ACCOUNT_FROZEN не должен изменять баланс");
    }

    @Test
    void peekReflectsChargesAppliedThroughTheScript() {
        store.applyChargeAtomically(accountId, "charge-1", 300, 0);
        var account = store.peek(accountId);
        assertEquals(-300, account.balance());
        assertTrue(account.processedChargeIds().contains("charge-1"));
    }

    /**
     * Прямое доказательство того, зачем этот Lua-скрипт вообще написан:
     * N потоков одновременно списывают с ОДНОГО account_id (тот же epoch,
     * разные charge_id) — если бы это был read-then-write (первая версия
     * этого класса, найдено кодревью) или даже WATCH/MULTI/EXEC под высокой
     * конкуренцией мог потребовать ретраев — с настоящим атомарным
     * Lua-скриптом каждое списание гарантированно учтено ровно один раз,
     * итоговый баланс — точная сумма, не заниженная гонкой.
     */
    @Test
    void concurrentChargesOnSameAccountNeverLoseAWrite() throws InterruptedException {
        int threads = 20;
        long amountPerCharge = 10;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch go = new CountDownLatch(1);
        AtomicInteger appliedCount = new AtomicInteger(0);

        for (int i = 0; i < threads; i++) {
            int idx = i;
            pool.submit(() -> {
                ready.countDown();
                try {
                    go.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                ChargeResult result = store.applyChargeAtomically(accountId, "concurrent-charge-" + idx, amountPerCharge, 0);
                if (result.outcome() == ChargeOutcome.APPLIED) {
                    appliedCount.incrementAndGet();
                }
            });
        }

        ready.await();
        go.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS), "все потоки должны завершиться в пределах таймаута");

        assertEquals(threads, appliedCount.get(), "каждый уникальный charge_id должен быть применён ровно один раз");
        var finalAccount = store.peek(accountId);
        assertEquals(-(threads * amountPerCharge), finalAccount.balance(),
            "итоговый баланс должен точно равняться сумме всех списаний — ни одно не должно потеряться в гонке");
        assertEquals(threads, finalAccount.processedChargeIds().size());
    }
}
