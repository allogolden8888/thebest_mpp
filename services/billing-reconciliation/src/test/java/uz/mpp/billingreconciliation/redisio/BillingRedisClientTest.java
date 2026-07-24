package uz.mpp.billingreconciliation.redisio;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import uz.mpp.billingreconciliation.core.Account;
import uz.mpp.billingreconciliation.core.AccountState;

import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Реальный Redis (brew, локально) — WATCH/MULTI/EXEC реально исполняются, не симулируются. */
class BillingRedisClientTest {

    private RedisClient rawClient;
    private StatefulRedisConnection<String, String> connection;
    private RedisCommands<String, String> commands;
    private BillingRedisClient client;
    private String accountId;

    @BeforeEach
    void setUp() {
        String uri = System.getenv().getOrDefault("BILLING_RECONCILIATION_TEST_REDIS_URI", "redis://localhost:6379");
        try {
            rawClient = RedisClient.create(uri);
            connection = rawClient.connect();
        } catch (Exception e) {
            assumeTrue(false, "Redis недоступен на " + uri + " — пропуск");
            return;
        }
        commands = connection.sync();
        client = new BillingRedisClient(commands);
        accountId = "acc-" + UUID.randomUUID();

        commands.hset("billing:balance:" + accountId, Map.of(
            "balance", "100000", "currency", "UZS", "account_state", "ACTIVE", "account_epoch", "0"
        ));
    }

    @AfterEach
    void tearDown() {
        if (commands != null) {
            commands.del("billing:balance:" + accountId);
        }
        if (connection != null) {
            connection.close();
        }
        if (rawClient != null) {
            rawClient.shutdown();
        }
    }

    @Test
    void readAccountParsesRealHash() {
        Account account = client.readAccount(accountId);
        assertEquals(100000, account.balanceMinorUnits());
        assertEquals(AccountState.ACTIVE, account.state());
        assertEquals(0, account.epoch());
    }

    @Test
    void freezeBumpsEpochAndSetsFrozenState() {
        client.freeze(accountId);
        Account account = client.readAccount(accountId);
        assertEquals(AccountState.FROZEN, account.state());
        assertEquals(1, account.epoch());
    }

    @Test
    void freezeIsIdempotent() {
        client.freeze(accountId);
        client.freeze(accountId);
        Account account = client.readAccount(accountId);
        assertEquals(1, account.epoch(), "повторный freeze не должен двигать epoch дальше");
    }

    @Test
    void applyFencedUnfreezeSucceedsWithMatchingEpoch() {
        client.freeze(accountId); // epoch -> 1, FROZEN

        boolean applied = client.applyFencedUnfreeze(accountId, 97500, 1);
        assertTrue(applied);

        Account account = client.readAccount(accountId);
        assertEquals(AccountState.ACTIVE, account.state());
        assertEquals(97500, account.balanceMinorUnits());
        assertEquals(2, account.epoch());
    }

    @Test
    void applyFencedUnfreezeFailsWithStaleEpoch() {
        client.freeze(accountId); // epoch -> 1

        boolean applied = client.applyFencedUnfreeze(accountId, 97500, 0); // устаревший expectedEpoch
        assertFalse(applied, "CAS с устаревшим epoch должен провалиться");

        Account account = client.readAccount(accountId);
        assertEquals(AccountState.FROZEN, account.state(), "состояние не должно было измениться при отклонённом CAS");
    }
}