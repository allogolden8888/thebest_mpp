package uz.mpp.billingoutbox;

import uz.mpp.billingoutbox.core.StreamEntry;
import uz.mpp.billingoutbox.health.HealthServer;
import uz.mpp.billingoutbox.kafkaio.LedgerEventBuilder;
import uz.mpp.billingoutbox.kafkaio.LedgerEventPublisher;
import uz.mpp.billingoutbox.redisio.OutboxStreamReader;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Billing Outbox Publisher (services_specifictaion.md §6.1): Billing Redis
 * Stream → billing.ledger.
 */
public final class Main {

    private static final int RECLAIM_INTERVAL_SECONDS = 10;
    /** См. javadoc {@link OutboxStreamReader#reclaimStalePending} — запас над обычным 500мс poll-циклом. */
    private static final Duration RECLAIM_MIN_IDLE = Duration.ofSeconds(30);

    public static void main(String[] args) throws Exception {
        HealthServer health = new HealthServer();
        health.start();

        // Раньше URI собирался здесь вручную и БЕЗ пароля — сервис не мог
        // подключиться к защищённому requirepass Redis (а он защищён и
        // локально, и в проде), поэтому биллинговый outbox не вычитывался
        // вообще. См. RedisUrl javadoc.
        String redisUri = RedisUrl.buildBillingUrl();
        int numShards = Integer.parseInt(env("OUTBOX_NUM_SHARDS", "4"));
        OutboxStreamReader streamReader = new OutboxStreamReader(redisUri, "billing-outbox-publisher", env("HOSTNAME", "billing-outbox-publisher-0"), numShards);

        LedgerEventPublisher publisher = new LedgerEventPublisher(env("KAFKA_BOOTSTRAP_SERVERS", "kafka-bootstrap.mpp.svc:9092"));

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleWithFixedDelay(() -> pollAndPublish(streamReader, publisher, numShards), 0, 500, TimeUnit.MILLISECONDS);
        // CODE_REVIEW.md Critical #1 — reclaim_stale_pending, отдельный,
        // более редкий цикл (не каждые 500мс — XAUTOCLAIM сканирует весь PEL
        // на каждый вызов, нет смысла делать это так же часто, как обычный
        // poll свежих записей). RECLAIM_MIN_IDLE — насколько застрявшей
        // должна быть запись, прежде чем considered "точно не в процессе
        // обычной обработки" — с запасом над обычным циклом poll.
        scheduler.scheduleWithFixedDelay(() -> reclaimAndPublish(streamReader, publisher, numShards), RECLAIM_INTERVAL_SECONDS, RECLAIM_INTERVAL_SECONDS, TimeUnit.SECONDS);

        health.setReady(true);
        System.out.println("billing-outbox-publisher готов");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            scheduler.shutdown();
            streamReader.close();
            publisher.close();
            health.stop();
        }));

        Thread.currentThread().join();
    }

    private static void pollAndPublish(OutboxStreamReader streamReader, LedgerEventPublisher publisher, int numShards) {
        try {
            List<StreamEntry> entries = streamReader.pollAllShards(100);
            publishAndAck(streamReader, publisher, entries);
        } catch (Exception e) {
            System.err.println("poll_redis_stream failed: " + e.getMessage());
        }
    }

    /**
     * CODE_REVIEW.md Critical #1 — reclaim_stale_pending, отдельный цикл поверх
     * {@link OutboxStreamReader#reclaimStalePending}. Переиспользует тот же
     * {@link #publishAndAck} путь, что обычные свежие записи — reclaim
     * отличается только ИСТОЧНИКОМ записей (PEL вместо {@code >}), не тем,
     * что с ними делать после.
     */
    private static void reclaimAndPublish(OutboxStreamReader streamReader, LedgerEventPublisher publisher, int numShards) {
        for (int shard = 0; shard < numShards; shard++) {
            try {
                List<StreamEntry> reclaimed = streamReader.reclaimStalePending(shard, RECLAIM_MIN_IDLE, 100);
                if (!reclaimed.isEmpty()) {
                    System.err.println("reclaim_stale_pending: " + reclaimed.size() + " запись(ей) заявлены заново на shard=" + shard);
                }
                publishAndAck(streamReader, publisher, reclaimed);
            } catch (Exception e) {
                System.err.println("reclaim_stale_pending failed for shard=" + shard + ": " + e.getMessage());
            }
        }
    }

    private static void publishAndAck(OutboxStreamReader streamReader, LedgerEventPublisher publisher, List<StreamEntry> entries) {
        for (StreamEntry entry : entries) {
            try {
                var event = LedgerEventBuilder.build(entry);
                publisher.publish(event).get();
                streamReader.ack(entry.shard(), entry.redisEntryId());
            } catch (Exception e) {
                System.err.println("publish_ledger_event failed for charge_id=" + entry.chargeId() + ": " + e.getMessage());
            }
        }
    }

    private static String env(String key, String fallback) {
        String v = System.getenv(key);
        return (v == null || v.isEmpty()) ? fallback : v;
    }
}