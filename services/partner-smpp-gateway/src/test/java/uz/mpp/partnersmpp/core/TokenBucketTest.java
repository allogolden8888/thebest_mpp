package uz.mpp.partnersmpp.core;

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
}