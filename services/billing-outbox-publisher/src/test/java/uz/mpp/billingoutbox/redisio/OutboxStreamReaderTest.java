package uz.mpp.billingoutbox.redisio;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import uz.mpp.billingoutbox.core.StreamEntry;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Реальный Redis (brew, локально). Пропускается, если Redis недоступен. */
class OutboxStreamReaderTest {

    private RedisClient client;
    private StatefulRedisConnection<String, String> connection;
    private RedisCommands<String, String> commands;
    private OutboxStreamReader reader;
    private String groupName;

    @BeforeEach
    void setUp() {
        String uri = System.getenv().getOrDefault("BILLING_OUTBOX_PUBLISHER_TEST_REDIS_URI", "redis://localhost:6379");
        try {
            client = RedisClient.create(uri);
            connection = client.connect();
        } catch (Exception e) {
            assumeTrue(false, "Redis недоступен на " + uri + " — пропуск");
            return;
        }
        commands = connection.sync();
        commands.del("billing:outbox:0"); // изоляция от предыдущих прогонов
        groupName = "test-group-" + UUID.randomUUID();
        reader = new OutboxStreamReader(commands, groupName, "test-consumer", 1);
    }

    @AfterEach
    void tearDown() {
        if (commands != null) {
            commands.del("billing:outbox:0");
        }
        if (connection != null) {
            connection.close();
        }
        if (client != null) {
            client.shutdown();
        }
    }

    @Test
    void pollAllShardsReadsRealStreamEntry() {
        String id = commands.xadd("billing:outbox:0", Map.of(
            "charge_id", "charge-1",
            "account_id", "acc-1",
            "partner_id", "acme",
            "amount_minor_units", "150000",
            "currency_code", "UZS",
            "entry_type", "charge",
            "created_at_epoch_ms", "1700000000000"
        ));

        List<StreamEntry> entries = reader.pollAllShards(10);

        assertEquals(1, entries.size());
        StreamEntry entry = entries.get(0);
        assertEquals(id, entry.redisEntryId());
        assertEquals("charge-1", entry.chargeId());
        assertEquals("acc-1", entry.accountId());
        assertEquals(150000L, entry.amountMinorUnits());
        assertEquals("charge", entry.entryType());
    }

    @Test
    void ackRemovesEntryFromPending() {
        commands.xadd("billing:outbox:0", Map.of(
            "charge_id", "charge-1", "account_id", "acc-1", "partner_id", "acme",
            "amount_minor_units", "1", "currency_code", "UZS", "entry_type", "charge",
            "created_at_epoch_ms", "1"
        ));
        List<StreamEntry> entries = reader.pollAllShards(10);
        assertEquals(1, entries.size());

        long pendingBefore = commands.xpending("billing:outbox:0", groupName).getCount();
        assertEquals(1, pendingBefore);

        reader.ack(0, entries.get(0).redisEntryId());

        long pendingAfter = commands.xpending("billing:outbox:0", groupName).getCount();
        assertEquals(0, pendingAfter);
    }
}