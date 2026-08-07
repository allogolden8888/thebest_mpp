package uz.mpp.scheduler.standard.core;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Реальные round-trip'ы против локального Redis (brew, {@code redis-server})
 * — не мок, тот же принцип, что {@code BillingAccountStoreTest} в
 * billing-service. Прямая проверка ЗАЧЕМ написан
 * {@code token_bucket_acquire.lua} (CODE_REVIEW.md HIGH #5): один per-stage
 * Redis-ключ, разделяемый всеми "партициями" (симулируются здесь
 * несколькими {@link RedisTokenBucket} с одинаковым stageName, как было бы
 * у нескольких {@code HoldCommandProcessor}-инстансов на разных партициях/
 * репликах в проде) — суммарно никогда не выдаёт больше токенов, чем
 * реально сконфигурировано, в отличие от старого per-partition
 * {@link TokenBucket}, который на этом сценарии выдал бы кратно больше.
 */
class RedisTokenBucketTest {

    private static final String REDIS_URL = System.getenv().getOrDefault("SCHEDULER_STANDARD_LANE_TEST_REDIS_URL", "redis://localhost:6379/0");

    private RedisClient client;
    private String stageName;

    @BeforeEach
    void setUp() {
        client = RedisClient.create(REDIS_URL);
        stageName = "TEST_STAGE_" + System.nanoTime();
    }

    @AfterEach
    void tearDown() {
        try (StatefulRedisConnection<String, String> connection = client.connect()) {
            connection.sync().del("scheduler:standard:token_bucket:" + stageName);
        }
        client.shutdown();
    }

    @Test
    void acquireUpToGrantsNoMoreThanAvailableTokens() {
        RedisTokenBucket bucket = new RedisTokenBucket(client, stageName, 5, 0);
        int granted = bucket.acquireUpTo(10, 0L);
        assertEquals(5, granted, "capacity=5, no refill — не может выдать больше 5 сразу");
    }

    @Test
    void secondAcquireSeesDrainedState() {
        RedisTokenBucket bucket = new RedisTokenBucket(client, stageName, 5, 0);
        assertEquals(3, bucket.acquireUpTo(3, 0L));
        assertEquals(2, bucket.acquireUpTo(10, 0L), "5 - 3 = 2 осталось");
        assertEquals(0, bucket.acquireUpTo(1, 0L), "пусто — новых токенов без refill не появляется");
    }

    @Test
    void refillsOverTime() {
        RedisTokenBucket bucket = new RedisTokenBucket(client, stageName, 10, 10); // 10/сек
        assertEquals(10, bucket.acquireUpTo(10, 0L));
        assertEquals(0, bucket.acquireUpTo(10, 0L), "сразу же — рефилла ещё не было");
        // +500мс при 10/сек = +5 токенов
        assertEquals(5, bucket.acquireUpTo(10, 500L));
    }

    @Test
    void neverExceedsCapacityEvenAfterLongIdle() {
        RedisTokenBucket bucket = new RedisTokenBucket(client, stageName, 5, 100);
        bucket.acquireUpTo(5, 0L);
        // Огромный простой — рефилл должен упереться в capacity, не расти безгранично.
        assertEquals(5, bucket.acquireUpTo(1000, 1_000_000L));
    }

    /**
     * Ключевой регрессионный тест на саму находку кодревью: несколько
     * {@link RedisTokenBucket}, указывающих на ОДИН stageName (симулирует
     * несколько партиций/реплик {@code HoldCommandProcessor}, каждая с
     * собственным Java-объектом, но одним и тем же Redis-ключом), суммарно
     * никогда не выдают больше capacity токенов при конкурентных вызовах —
     * старый per-partition {@link TokenBucket} на этом сценарии выдал бы
     * {@code partitions * capacity}.
     */
    @Test
    void concurrentBucketsForSameStageShareOneLimit() throws InterruptedException {
        int partitions = 8;
        int capacity = 20;
        List<RedisTokenBucket> perPartitionBuckets = new java.util.ArrayList<>();
        for (int i = 0; i < partitions; i++) {
            perPartitionBuckets.add(new RedisTokenBucket(client, stageName, capacity, 0));
        }

        ExecutorService pool = Executors.newFixedThreadPool(partitions);
        CountDownLatch startLatch = new CountDownLatch(1);
        AtomicInteger totalGranted = new AtomicInteger(0);

        for (RedisTokenBucket bucket : perPartitionBuckets) {
            pool.submit(() -> {
                try {
                    startLatch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                // Каждая "партиция" пытается забрать capacity токенов —
                // если бы лимит не разделялся, суммарно ушло бы
                // partitions*capacity.
                totalGranted.addAndGet(bucket.acquireUpTo(capacity, 0L));
            });
        }
        startLatch.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));

        assertEquals(capacity, totalGranted.get(),
            "общий per-stage лимит должен делиться между \"партициями\", не умножаться на их число");
    }
}
