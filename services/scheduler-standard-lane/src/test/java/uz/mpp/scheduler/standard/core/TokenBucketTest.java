package uz.mpp.scheduler.standard.core;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TokenBucketTest {

    @Test
    void allowsUpToCapacityImmediately() {
        TokenBucket bucket = new TokenBucket(5, 1, 0L);
        assertTrue(bucket.tryAcquire(5, 0L));
    }

    @Test
    void deniesBeyondCapacity() {
        TokenBucket bucket = new TokenBucket(5, 1, 0L);
        assertFalse(bucket.tryAcquire(6, 0L));
    }

    @Test
    void refillsOverTime() {
        TokenBucket bucket = new TokenBucket(5, 1, 0L); // 1 token/sec
        assertTrue(bucket.tryAcquire(5, 0L)); // drain fully
        assertFalse(bucket.tryAcquire(1, 500L)); // 0.5s later, not enough
        assertTrue(bucket.tryAcquire(1, 1000L)); // 1s later, 1 token refilled
    }

    @Test
    void doesNotExceedCapacityOnRefill() {
        TokenBucket bucket = new TokenBucket(5, 100, 0L); // fast refill
        assertTrue(bucket.tryAcquire(5, 0L));
        int available = bucket.available(100_000L); // huge elapsed time, should cap at capacity
        assertEquals(5, available);
    }
}
