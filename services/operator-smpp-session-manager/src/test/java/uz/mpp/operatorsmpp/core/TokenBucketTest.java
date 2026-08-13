package uz.mpp.operatorsmpp.core;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TokenBucketTest {

    @Test
    void allowsUpToCapacity() {
        TokenBucket bucket = new TokenBucket(3, 1, 0L);
        assertTrue(bucket.tryAcquire(0L));
        assertTrue(bucket.tryAcquire(0L));
        assertTrue(bucket.tryAcquire(0L));
        assertFalse(bucket.tryAcquire(0L));
    }

    @Test
    void refillsOverTime() {
        TokenBucket bucket = new TokenBucket(1, 1, 0L);
        assertTrue(bucket.tryAcquire(0L));
        assertFalse(bucket.tryAcquire(500L));
        assertTrue(bucket.tryAcquire(1000L));
    }

    // withBurstHeadroom — см. TokenBucket.BURST_HEADROOM_FACTOR, то же
    // исправление, что и rate_limit.rs::Bucket::new_message_bucket_at
    // сегодня, та же техника подбора тестовых rate под круглые числа.

    @Test
    void burstHeadroomCapacityIs15PercentOfRateNotFullRate() {
        // rate=20 -> capacity = 20 * BURST_HEADROOM_FACTOR(0.15) = 3.
        TokenBucket bucket = TokenBucket.withBurstHeadroom(20, 0L);
        for (int i = 0; i < 3; i++) {
            assertTrue(bucket.tryAcquire(0L), "токен " + i + " из 3 (=20*0.15) должен быть доступен");
        }
        assertFalse(bucket.tryAcquire(0L), "4-й токен мгновенно уже не должен проходить — старое поведение (capacity=20) пропустило бы");
    }

    @Test
    void burstHeadroomRefillPerSecondStaysFullRateOnlyCapacityIsCut() {
        // rate=40 -> capacity=6, но refillPerSecond ОСТАЁТСЯ полным rate (40) —
        // урезана только мгновенная ёмкость всплеска, не средняя пропускная способность.
        TokenBucket bucket = TokenBucket.withBurstHeadroom(40, 0L);
        for (int i = 0; i < 6; i++) {
            assertTrue(bucket.tryAcquire(0L));
        }
        assertFalse(bucket.tryAcquire(0L), "bucket пуст сразу после исчерпания capacity=6");

        long later = 100L; // refillPerSecond=40 * 0.1с = 4 токена
        for (int i = 0; i < 4; i++) {
            assertTrue(bucket.tryAcquire(later));
        }
        assertFalse(bucket.tryAcquire(later), "только 4 токена накопилось за 100мс при refillPerSecond=40");
    }

    @Test
    void burstHeadroomCapacityFloorIsOneTokenForLowRate() {
        // Без пола rate=3 дало бы capacity=0.45 — токен никогда не набрался
        // бы до 1.0, bucket не пропустил бы НИ ОДНОГО сообщения. capacity =
        // max(rate*0.15, 1.0) — пол в 1 токен.
        TokenBucket bucket = TokenBucket.withBurstHeadroom(3, 0L);
        assertTrue(bucket.tryAcquire(0L), "низкий rate всё равно должен пропускать хотя бы 1 сообщение мгновенно");
        assertFalse(bucket.tryAcquire(0L), "capacity=1 (пол) исчерпан после первого сообщения");
    }

    @Test
    void burstHeadroomCapacityDoesNotExceedConfiguredRateEvenAfterLongIdle() {
        // Регрессия на саму находку: rate=20 -> capacity=3. Долгий простой не
        // должен позволить накопить 3600*20 токенов.
        TokenBucket bucket = TokenBucket.withBurstHeadroom(20, 0L);
        bucket.tryAcquire(0L);
        long muchLater = 3600_000L;
        assertTrue(bucket.tryAcquire(muchLater));
        assertTrue(bucket.tryAcquire(muchLater));
        assertTrue(bucket.tryAcquire(muchLater));
        assertFalse(bucket.tryAcquire(muchLater), "capacity=3 не должен быть превышен долгим простоем");
    }
}