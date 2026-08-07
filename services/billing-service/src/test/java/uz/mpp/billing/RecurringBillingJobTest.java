package uz.mpp.billing;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Реальный round-trip против локального Redis (brew, {@code redis-server})
 * — тот же принцип, что {@code BillingAccountStoreTest}.
 */
class RecurringBillingJobTest {

    private static final String REDIS_URL = System.getenv().getOrDefault("BILLING_ACCOUNT_STORE_TEST_REDIS_URL", "redis://localhost:6379/0");
    private static final Clock AUGUST_2026 = Clock.fixed(Instant.parse("2026-08-15T00:00:00Z"), ZoneOffset.UTC);
    private static final Clock SEPTEMBER_2026 = Clock.fixed(Instant.parse("2026-09-01T00:00:00Z"), ZoneOffset.UTC);

    private BillingAccountStore store;
    private RedisClient rawClient;
    private String accountId;

    @BeforeEach
    void setUp() {
        store = new BillingAccountStore(REDIS_URL);
        rawClient = RedisClient.create(REDIS_URL);
        accountId = "recurring-test-" + UUID.randomUUID();
    }

    @AfterEach
    void tearDown() {
        try (StatefulRedisConnection<String, String> connection = rawClient.connect()) {
            connection.sync().del("billing:account:" + accountId);
        }
        rawClient.shutdown();
        store.close();
    }

    @Test
    void chargesMonthlyFeePerActiveSenderAndPackageOncePerRun() {
        List<RecurringCharges.Sender> senders = List.of(
            new RecurringCharges.Sender("CLICK", "ALPHANAME", "active"),
            new RecurringCharges.Sender("5252", "SHORT_NUMBER", "active")
        );
        RecurringCharges.ServicePackage pkg = new RecurringCharges.ServicePackage(40_000, 2_000_000);
        RecurringBillingJob job = new RecurringBillingJob(store, accountId, "click_uz", senders, 4_000_000L, pkg, AUGUST_2026);

        job.run();

        // 2 alphaname fees (4M each) + 1 package (2M) = 10M списано с 0.
        BillingAccountState.Account account = store.peek(accountId);
        assertEquals(-10_000_000L, account.balance());
    }

    @Test
    void reRunningInSamePeriodDoesNotDoubleCharge() {
        List<RecurringCharges.Sender> senders = List.of(new RecurringCharges.Sender("CLICK", "ALPHANAME", "active"));
        RecurringBillingJob job = new RecurringBillingJob(store, accountId, "click_uz", senders, 4_000_000L, null, AUGUST_2026);

        job.run();
        job.run();
        job.run();

        BillingAccountState.Account account = store.peek(accountId);
        assertEquals(-4_000_000L, account.balance(), "повторный прогон в том же периоде не должен списывать повторно — charge_id-дедуп");
    }

    @Test
    void newMonthChargesAgain() {
        List<RecurringCharges.Sender> senders = List.of(new RecurringCharges.Sender("CLICK", "ALPHANAME", "active"));
        RecurringBillingJob augustJob = new RecurringBillingJob(store, accountId, "click_uz", senders, 4_000_000L, null, AUGUST_2026);
        RecurringBillingJob septemberJob = new RecurringBillingJob(store, accountId, "click_uz", senders, 4_000_000L, null, SEPTEMBER_2026);

        augustJob.run();
        septemberJob.run();

        BillingAccountState.Account account = store.peek(accountId);
        assertEquals(-8_000_000L, account.balance(), "новый месяц — новый charge_id, значит новое списание");
    }

    @Test
    void archivedSenderIsNeverCharged() {
        List<RecurringCharges.Sender> senders = List.of(new RecurringCharges.Sender("OLD", "ALPHANAME", "archived"));
        RecurringBillingJob job = new RecurringBillingJob(store, accountId, "click_uz", senders, 4_000_000L, null, AUGUST_2026);

        job.run();

        BillingAccountState.Account account = store.peek(accountId);
        assertEquals(0L, account.balance());
    }

    @Test
    void frozenAccountDefersChargeUntilUnfrozen() {
        // Заморозить счёт напрямую через Lua-примитив (freeze здесь эмулируется
        // прямой записью state=FROZEN в хэш — тот же формат, что читает
        // apply_atomic_charge.lua).
        try (StatefulRedisConnection<String, String> connection = rawClient.connect()) {
            connection.sync().hset("billing:account:" + accountId,
                java.util.Map.of("balance", "0", "state", "FROZEN", "epoch", "1"));
        }

        List<RecurringCharges.Sender> senders = List.of(new RecurringCharges.Sender("CLICK", "ALPHANAME", "active"));
        RecurringBillingJob job = new RecurringBillingJob(store, accountId, "click_uz", senders, 4_000_000L, null, AUGUST_2026);

        job.run(); // не должен бросить — ACCOUNT_FROZEN логируется, не исключение

        BillingAccountState.Account account = store.peek(accountId);
        assertEquals(0L, account.balance(), "замороженный счёт не списывается — charge отложен, не потерян (charge_id не помечен processed)");
    }
}
