package uz.mpp.scheduler.standard.core;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FairSchedulerTest {

    private static HeldItem item(String scopeId, String stageExecId) {
        return new HeldItem("msg-" + stageExecId, stageExecId, "PARTNER", scopeId, "BILLING", 0L);
    }

    @Test
    void interleavesAcrossPartnersRoundRobin() {
        // acme has 3 backlog items, beta has 1 — round-robin must not let
        // acme monopolize the front of the batch.
        List<HeldItem> candidates = List.of(
            item("acme", "a1"), item("acme", "a2"), item("acme", "a3"),
            item("beta", "b1")
        );

        List<HeldItem> ordered = FairScheduler.applyFairScheduling(candidates);

        assertEquals(4, ordered.size());
        assertEquals("a1", ordered.get(0).stageExecutionId());
        assertEquals("b1", ordered.get(1).stageExecutionId());
        assertEquals("a2", ordered.get(2).stageExecutionId());
        assertEquals("a3", ordered.get(3).stageExecutionId());
    }

    @Test
    void singlePartnerPreservesFifoOrder() {
        List<HeldItem> candidates = List.of(item("acme", "a1"), item("acme", "a2"));
        List<HeldItem> ordered = FairScheduler.applyFairScheduling(candidates);
        assertEquals(List.of("a1", "a2"), ordered.stream().map(HeldItem::stageExecutionId).toList());
    }

    @Test
    void emptyInputProducesEmptyOutput() {
        assertEquals(List.of(), FairScheduler.applyFairScheduling(List.of()));
    }
}
