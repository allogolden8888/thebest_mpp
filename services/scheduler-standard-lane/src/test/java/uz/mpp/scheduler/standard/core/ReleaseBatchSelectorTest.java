package uz.mpp.scheduler.standard.core;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReleaseBatchSelectorTest {

    private static HeldItem item(String id) {
        return new HeldItem("msg-" + id, id, "PARTNER", "acme", "BILLING", 0L);
    }

    @Test
    void emptyBacklogReleasesNothing() {
        TokenBucket bucket = new TokenBucket(100, 100, 0L);
        assertEquals(List.of(), ReleaseBatchSelector.selectBatch(List.of(), 1.0, bucket, 0L));
    }

    @Test
    void zeroRampRateReleasesNothingEvenWithTokens() {
        List<HeldItem> held = List.of(item("a1"), item("a2"));
        TokenBucket bucket = new TokenBucket(100, 100, 0L);
        assertEquals(List.of(), ReleaseBatchSelector.selectBatch(held, 0.0, bucket, 0L));
    }

    @Test
    void rampRateLimitsBatchSizeAsFractionOfBacklog() {
        List<HeldItem> held = List.of(item("a1"), item("a2"), item("a3"), item("a4"));
        TokenBucket bucket = new TokenBucket(100, 100, 0L); // plenty of tokens
        List<HeldItem> batch = ReleaseBatchSelector.selectBatch(held, 0.25, bucket, 0L); // ramp step 25%
        assertEquals(1, batch.size(), "ceil(4 * 0.25) = 1");
    }

    @Test
    void tokenBucketCapsBatchBelowRampBudget() {
        List<HeldItem> held = List.of(item("a1"), item("a2"), item("a3"), item("a4"));
        TokenBucket bucket = new TokenBucket(2, 0, 0L); // only 2 tokens, no refill
        List<HeldItem> batch = ReleaseBatchSelector.selectBatch(held, 1.0, bucket, 0L); // ramp allows all 4
        assertEquals(2, batch.size(), "token bucket should cap batch at 2 even though ramp allows 4");
    }

    @Test
    void fullRampAndFullTokensReleasesEntireBacklog() {
        List<HeldItem> held = List.of(item("a1"), item("a2"), item("a3"));
        TokenBucket bucket = new TokenBucket(100, 100, 0L);
        List<HeldItem> batch = ReleaseBatchSelector.selectBatch(held, 1.0, bucket, 0L);
        assertEquals(3, batch.size());
    }

    @Test
    void selectingBatchActuallyDrainsTokenBucket() {
        List<HeldItem> held = List.of(item("a1"), item("a2"));
        TokenBucket bucket = new TokenBucket(5, 0, 0L);
        ReleaseBatchSelector.selectBatch(held, 1.0, bucket, 0L);
        assertEquals(3, bucket.available(0L), "5 - 2 released = 3 remaining");
    }

    @Test
    void fairSchedulingAppliedAcrossPartnersWithinBatch() {
        List<HeldItem> held = List.of(
            new HeldItem("m1", "a1", "PARTNER", "acme", "BILLING", 0L),
            new HeldItem("m2", "a2", "PARTNER", "acme", "BILLING", 0L),
            new HeldItem("m3", "b1", "PARTNER", "beta", "BILLING", 0L)
        );
        TokenBucket bucket = new TokenBucket(2, 0, 0L);
        List<HeldItem> batch = ReleaseBatchSelector.selectBatch(held, 1.0, bucket, 0L);
        assertEquals(2, batch.size());
        assertTrue(batch.stream().anyMatch(i -> i.scopeId().equals("beta")),
            "batch should include beta's item, not just acme's first two, due to fair scheduling");
    }
}
