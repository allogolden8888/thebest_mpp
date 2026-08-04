package uz.mpp.billingoutbox.redisio;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import uz.mpp.billingoutbox.core.StreamEntry;

import java.time.Duration;
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

    /**
     * Прямое доказательство исправления CRITICAL находки кодревью #1: запись,
     * прочитанная через {@code XREADGROUP} (значит уже в PEL), но НИКОГДА не
     * подтверждённая {@code XACK} (та же ситуация, что publish в Kafka упал
     * или процесс упал между чтением и ack) — раньше не существовало НИ
     * ОДНОГО пути, которым эта запись когда-либо была бы перечитана
     * {@code pollAllShards} (он читает только {@code >}, никогда не
     * доставленные). {@link OutboxStreamReader#reclaimStalePending} должен её
     * найти и вернуть, как только она достаточно долго простаивает в PEL.
     */
    @Test
    void reclaimStalePendingRecoversEntryThatWasNeverAcked() throws InterruptedException {
        commands.xadd("billing:outbox:0", Map.of(
            "charge_id", "charge-stuck", "account_id", "acc-1", "partner_id", "acme",
            "amount_minor_units", "1", "currency_code", "UZS", "entry_type", "charge",
            "created_at_epoch_ms", "1"
        ));

        // Читаем (попадает в PEL), но намеренно НЕ ack'аем — симулирует сбой
        // publish/краш процесса между XREADGROUP и XACK.
        List<StreamEntry> firstRead = reader.pollAllShards(10);
        assertEquals(1, firstRead.size());

        // pollAllShards (только >) не должен видеть эту запись повторно —
        // подтверждает, что без reclaimStalePending она бы застряла навсегда.
        assertTrue(reader.pollAllShards(10).isEmpty(),
            "pollAllShards не должен повторно вернуть уже доставленную (хоть и не acked) запись");

        Thread.sleep(20); // чтобы гарантированно превысить minIdleTime ниже

        List<StreamEntry> reclaimed = reader.reclaimStalePending(0, Duration.ofMillis(1), 10);

        assertEquals(1, reclaimed.size(), "запись, простаивающая в PEL, должна быть заявлена заново");
        assertEquals("charge-stuck", reclaimed.get(0).chargeId());
        assertEquals(firstRead.get(0).redisEntryId(), reclaimed.get(0).redisEntryId(), "тот же Redis entry ID, не новая запись");

        // Реклейм не подтверждает сам — тот же publish+ack путь, что обычные записи.
        long pendingBeforeAck = commands.xpending("billing:outbox:0", groupName).getCount();
        assertEquals(1, pendingBeforeAck, "reclaim сам по себе не должен ack'ать — только заново заявляет владение");

        reader.ack(0, reclaimed.get(0).redisEntryId());
        long pendingAfterAck = commands.xpending("billing:outbox:0", groupName).getCount();
        assertEquals(0, pendingAfterAck);
    }

    @Test
    void reclaimStalePendingIgnoresEntryStillWithinMinIdleTime() {
        commands.xadd("billing:outbox:0", Map.of(
            "charge_id", "charge-fresh", "account_id", "acc-1", "partner_id", "acme",
            "amount_minor_units", "1", "currency_code", "UZS", "entry_type", "charge",
            "created_at_epoch_ms", "1"
        ));
        reader.pollAllShards(10); // попадает в PEL, только что доставлена

        List<StreamEntry> reclaimed = reader.reclaimStalePending(0, Duration.ofHours(1), 10);

        assertTrue(reclaimed.isEmpty(), "недавно доставленная (< minIdleTime) запись не должна быть реклеймлена — она может ещё обрабатываться нормально");
    }
}