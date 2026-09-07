package uz.mpp.delivery;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import uz.mpp.delivery.DeliveryService.SubmitOutcome;
import uz.mpp.platformcontracts.common.v1.Outcome;

/**
 * Реальные round-trip'ы против локального Redis — прямая проверка исправления
 * CRITICAL находки кодревью (PART 2, delivery-service #1): дубль реального
 * submit'а оператору при редеставке того же {@code stage_execution_id}.
 * {@link #concurrentClaimsOnSameStageExecutionIdOnlyOneWinner} воспроизводит
 * ровно ту гонку (два "реплики"/редеставки одновременно пытаются выполнить
 * submit для одного и того же stage_execution_id) — только одна должна
 * реально дойти до вызова submitClient.submit(...) (здесь — до "Won").
 */
class SubmitIdempotencyStoreTest {

    private static final String REDIS_URL = System.getenv().getOrDefault("DELIVERY_IDEMPOTENCY_TEST_REDIS_URL", "redis://localhost:6379/0");

    private SubmitIdempotencyStore store;
    private RedisClient rawClient;
    private String stageExecutionId;
    private String correlationKey;

    @BeforeEach
    void setUp() {
        store = new SubmitIdempotencyStore(REDIS_URL);
        rawClient = RedisClient.create(REDIS_URL);
        stageExecutionId = "test-se-" + UUID.randomUUID();
    }

    @AfterEach
    void tearDown() {
        try (StatefulRedisConnection<String, String> conn = rawClient.connect()) {
            conn.sync().del("dlvsubmit:" + stageExecutionId);
            if (correlationKey != null) {
                conn.sync().del(correlationKey);
            }
        }
        store.close();
        rawClient.shutdown();
    }

    @Test
    void firstClaimOnFreshStageExecutionIdWins() {
        SubmitIdempotencyStore.ClaimResult result = store.claim(stageExecutionId, "dlv-" + stageExecutionId);
        SubmitIdempotencyStore.ClaimResult.Won won = assertInstanceOf(SubmitIdempotencyStore.ClaimResult.Won.class, result);
        assertEquals("dlv-" + stageExecutionId, won.queueMsgId());
    }

    @Test
    void secondClaimBeforeOutcomeRecordedIsAmbiguousNotResubmitted() {
        store.claim(stageExecutionId, "dlv-" + stageExecutionId);

        // Симулируем крэш между реальным submit'ом и recordOutcome — вторая
        // (редеставленная) попытка не должна получить "Won" повторно.
        SubmitIdempotencyStore.ClaimResult second = store.claim(stageExecutionId, "dlv-" + stageExecutionId);
        SubmitIdempotencyStore.ClaimResult.AmbiguousInFlight ambiguous =
            assertInstanceOf(SubmitIdempotencyStore.ClaimResult.AmbiguousInFlight.class, second);
        assertEquals("dlv-" + stageExecutionId, ambiguous.queueMsgId());
    }

    @Test
    void claimAfterOutcomeRecordedReturnsCachedOutcomeNotWon() {
        store.claim(stageExecutionId, "dlv-" + stageExecutionId);
        SubmitOutcome recorded = new SubmitOutcome(Outcome.OUTCOME_SUCCEEDED, "", "smsc-123");
        store.recordOutcome(stageExecutionId, recorded);

        SubmitIdempotencyStore.ClaimResult second = store.claim(stageExecutionId, "dlv-" + stageExecutionId);
        SubmitIdempotencyStore.ClaimResult.AlreadyDone done =
            assertInstanceOf(SubmitIdempotencyStore.ClaimResult.AlreadyDone.class, second);
        assertEquals(Outcome.OUTCOME_SUCCEEDED, done.outcome().outcome());
        assertEquals("smsc-123", done.outcome().smscMessageId());
        assertEquals("dlv-" + stageExecutionId, done.queueMsgId());
    }

    /**
     * ГЛАВНАЯ проверка правки быстрого пути корреляции DLR: {@code recordOutcome}
     * с {@link SubmitIdempotencyStore.CorrelationHint} обязан положить в Runtime
     * Redis ключ РОВНО того формата, который читает Go-сторона
     * ({@code dlr-manager/internal/correlation.FastPathKey} и
     * {@code parseFastPathValue}). Формат сверяется здесь дословно, потому что
     * рассинхрон между двумя языками не поймает ни один компилятор — он
     * проявился бы только как «корреляция снова не находится».
     */
    @Test
    void recordOutcomeWithHintWritesFastPathCorrelationEntryInGoReadableFormat() {
        String operatorID = "op-" + UUID.randomUUID().toString().substring(0, 8);
        String smscMessageId = "smsc-" + UUID.randomUUID();
        String messageId = UUID.randomUUID().toString();
        Instant submittedAt = Instant.now();
        correlationKey = "dlrcorr:" + operatorID + ":" + smscMessageId + ":1";

        store.claim(stageExecutionId, "dlv-" + stageExecutionId);
        store.recordOutcome(
            stageExecutionId,
            new SubmitOutcome(Outcome.OUTCOME_SUCCEEDED, "", smscMessageId),
            new SubmitIdempotencyStore.CorrelationHint(operatorID, messageId, 1, submittedAt));

        try (StatefulRedisConnection<String, String> conn = rawClient.connect()) {
            String value = conn.sync().get(correlationKey);
            assertNotNull(value, "быстрый путь не записан по ключу " + correlationKey);
            assertEquals(
                submittedAt.toEpochMilli() + "|" + messageId + "|" + stageExecutionId,
                value);
            long ttl = conn.sync().ttl(correlationKey);
            assertTrue(ttl > 0 && ttl <= SubmitIdempotencyStore.DEFAULT_FAST_PATH_TTL.getSeconds(),
                "TTL быстрого пути = " + ttl + "с, ожидали (0; " + SubmitIdempotencyStore.DEFAULT_FAST_PATH_TTL.getSeconds() + "]");
        }
    }

    /**
     * Оператор не вернул {@code smsc_message_id} синхронно (документированно
     * допустимо) — коррелировать не по чему, писать быстрый путь нечем и
     * незачем. Ключ не должен появиться вообще: пустой {@code smsc_message_id}
     * в ключе склеил бы разные сообщения в одну запись.
     */
    @Test
    void recordOutcomeWithoutSmscMessageIdWritesNoFastPathEntry() {
        String operatorID = "op-" + UUID.randomUUID().toString().substring(0, 8);
        correlationKey = "dlrcorr:" + operatorID + "::1";

        store.claim(stageExecutionId, "dlv-" + stageExecutionId);
        store.recordOutcome(
            stageExecutionId,
            new SubmitOutcome(Outcome.OUTCOME_SUBMISSION_OUTCOME_UNKNOWN, "SUBMIT_TIMEOUT", ""),
            new SubmitIdempotencyStore.CorrelationHint(operatorID, UUID.randomUUID().toString(), 1, Instant.now()));

        try (StatefulRedisConnection<String, String> conn = rawClient.connect()) {
            assertNull(conn.sync().get(correlationKey));
        }
    }

    /**
     * Обратная совместимость: {@code recordOutcome} без подсказки (путь
     * AmbiguousInFlight в KafkaIo) не пишет быстрый путь и по-прежнему
     * корректно записывает сам исход.
     */
    @Test
    void recordOutcomeWithoutHintStillRecordsOutcome() {
        store.claim(stageExecutionId, "dlv-" + stageExecutionId);
        store.recordOutcome(stageExecutionId, new SubmitOutcome(Outcome.OUTCOME_SUCCEEDED, "", "smsc-nohint"));

        SubmitIdempotencyStore.ClaimResult second = store.claim(stageExecutionId, "dlv-" + stageExecutionId);
        SubmitIdempotencyStore.ClaimResult.AlreadyDone done =
            assertInstanceOf(SubmitIdempotencyStore.ClaimResult.AlreadyDone.class, second);
        assertEquals("smsc-nohint", done.outcome().smscMessageId());
    }

    /**
     * Прямое доказательство того, зачем нужен именно атомарный HSETNX-claim,
     * а не "прочитать, потом решить": N потоков одновременно пытаются
     * заклеймить один и тот же stage_execution_id (та же гонка, что вызвала
     * бы дубль реального SMS submit'а в KafkaIo.processRecord) — ровно один
     * должен получить Won.
     */
    @Test
    void concurrentClaimsOnSameStageExecutionIdOnlyOneWinner() throws InterruptedException {
        int threads = 20;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch go = new CountDownLatch(1);
        AtomicInteger wonCount = new AtomicInteger(0);

        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                ready.countDown();
                try {
                    go.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                SubmitIdempotencyStore.ClaimResult result = store.claim(stageExecutionId, "dlv-" + stageExecutionId);
                if (result instanceof SubmitIdempotencyStore.ClaimResult.Won) {
                    wonCount.incrementAndGet();
                }
            });
        }

        ready.await();
        go.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS), "все потоки должны завершиться в пределах таймаута");
        assertEquals(1, wonCount.get(), "ровно одна попытка должна выиграть claim — иначе submitClient.submit(...) вызвался бы дважды");
    }
}
