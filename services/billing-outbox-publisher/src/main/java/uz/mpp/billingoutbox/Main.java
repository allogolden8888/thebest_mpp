package uz.mpp.billingoutbox;

import uz.mpp.billingoutbox.core.StreamEntry;
import uz.mpp.billingoutbox.health.HealthServer;
import uz.mpp.billingoutbox.kafkaio.LedgerEventBuilder;
import uz.mpp.billingoutbox.kafkaio.LedgerEventPublisher;
import uz.mpp.billingoutbox.redisio.OutboxStreamReader;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Billing Outbox Publisher (services_specifictaion.md §6.1): Billing Redis
 * Stream → billing.ledger.
 */
public final class Main {

    public static void main(String[] args) throws Exception {
        HealthServer health = new HealthServer();
        health.start();

        String redisUri = "redis://" + env("REDIS_BILLING_HOST", "localhost") + ":" + env("REDIS_BILLING_PORT", "6379");
        int numShards = Integer.parseInt(env("OUTBOX_NUM_SHARDS", "4"));
        OutboxStreamReader streamReader = new OutboxStreamReader(redisUri, "billing-outbox-publisher", env("HOSTNAME", "billing-outbox-publisher-0"), numShards);

        LedgerEventPublisher publisher = new LedgerEventPublisher(env("KAFKA_BOOTSTRAP_SERVERS", "kafka-bootstrap.mpp.svc:9092"));

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleWithFixedDelay(() -> pollAndPublish(streamReader, publisher, numShards), 0, 500, TimeUnit.MILLISECONDS);

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
            for (StreamEntry entry : entries) {
                try {
                    var event = LedgerEventBuilder.build(entry);
                    publisher.publish(event).get();
                    streamReader.ack(entry.shard(), entry.redisEntryId());
                } catch (Exception e) {
                    System.err.println("publish_ledger_event failed for charge_id=" + entry.chargeId() + ": " + e.getMessage());
                }
            }
        } catch (Exception e) {
            System.err.println("poll_redis_stream failed: " + e.getMessage());
        }
    }

    private static String env(String key, String fallback) {
        String v = System.getenv(key);
        return (v == null || v.isEmpty()) ? fallback : v;
    }
}