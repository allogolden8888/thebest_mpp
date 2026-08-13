package uz.mpp.operatorsmpp.core;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PacerMetricsTest {

    @Test
    void dispatchedAndRejectedCountersAccumulatePerTierAndReason() {
        PacerMetrics metrics = new PacerMetrics();
        metrics.recordDispatched(PriorityTier.HIGH);
        metrics.recordDispatched(PriorityTier.HIGH);
        metrics.recordDispatched(PriorityTier.LOW);
        metrics.recordRejected(PriorityTier.LOW, "PACER_QUEUE_FULL");
        metrics.recordRejected(PriorityTier.LOW, "PACER_QUEUE_FULL");
        metrics.recordRejected(PriorityTier.MEDIUM, "PACER_QUEUE_TIMEOUT");

        assertEquals(2, metrics.dispatchedTotalForTest(PriorityTier.HIGH));
        assertEquals(0, metrics.dispatchedTotalForTest(PriorityTier.MEDIUM));
        assertEquals(1, metrics.dispatchedTotalForTest(PriorityTier.LOW));
        assertEquals(2, metrics.rejectedTotalForTest(PriorityTier.LOW, "PACER_QUEUE_FULL"));
        assertEquals(1, metrics.rejectedTotalForTest(PriorityTier.MEDIUM, "PACER_QUEUE_TIMEOUT"));
        assertEquals(0, metrics.rejectedTotalForTest(PriorityTier.HIGH, "PACER_QUEUE_FULL"), "не должно быть перекрёстного загрязнения между tier'ами/причинами");
    }

    @Test
    void queueDepthReadsLiveSupplierAtScrapeTimeNotACachedSnapshot() {
        PacerMetrics metrics = new PacerMetrics();
        int[] depth = {0};
        metrics.bindQueueDepth(PriorityTier.HIGH, () -> depth[0]);

        assertTrue(metrics.renderPrometheusText().contains("operator_smpp_pacer_queue_depth{tier=\"high\"} 0"));
        depth[0] = 42;
        assertTrue(metrics.renderPrometheusText().contains("operator_smpp_pacer_queue_depth{tier=\"high\"} 42"),
            "queue_depth должен отражать состояние очереди В МОМЕНТ scrape, не значение при bindQueueDepth");
    }

    @Test
    void achievedTpsIsDeltaSinceLastScrapeThenResets() {
        PacerMetrics metrics = new PacerMetrics();
        metrics.recordDispatched(PriorityTier.HIGH);
        metrics.recordDispatched(PriorityTier.HIGH);
        metrics.recordDispatched(PriorityTier.HIGH);

        assertTrue(metrics.renderPrometheusText().contains("operator_smpp_pacer_achieved_tps{tier=\"high\"} 3"));
        // Второй scrape сразу же, без новых dispatched -> дельта 0.
        assertTrue(metrics.renderPrometheusText().contains("operator_smpp_pacer_achieved_tps{tier=\"high\"} 0"));

        metrics.recordDispatched(PriorityTier.HIGH);
        assertTrue(metrics.renderPrometheusText().contains("operator_smpp_pacer_achieved_tps{tier=\"high\"} 1"));
    }

    @Test
    void renderedTextFollowsHelpTypeConvention() {
        PacerMetrics metrics = new PacerMetrics();
        String text = metrics.renderPrometheusText();
        assertTrue(text.contains("# HELP operator_smpp_pacer_dispatched_total"));
        assertTrue(text.contains("# TYPE operator_smpp_pacer_dispatched_total counter"));
        assertTrue(text.contains("# TYPE operator_smpp_pacer_queue_depth gauge"));
        assertTrue(text.contains("# TYPE operator_smpp_pacer_achieved_tps gauge"));
    }
}
