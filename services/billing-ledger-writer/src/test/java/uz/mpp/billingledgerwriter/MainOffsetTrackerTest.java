package uz.mpp.billingledgerwriter;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link Main.OffsetTracker} — тот же паттерн, что уже найден и исправлен
 * кодревью в billing-service/delivery-service этой сессии
 * (commitAsync()/commitSync() без явных offset'ов коммитит позицию всего
 * фетча, не оффсет только что обработанной записи) — закрывает CODE_REVIEW.md
 * Critical #3 для billing-ledger-writer.
 */
class MainOffsetTrackerTest {

    private static final TopicPartition TP = new TopicPartition("billing.ledger", 0);

    @Test
    void allSuccessfulCommitsPastLastRecord() {
        Main.OffsetTracker tracker = new Main.OffsetTracker();
        tracker.recordSuccess(TP, 10);
        tracker.recordSuccess(TP, 11);
        tracker.recordSuccess(TP, 12);
        assertEquals(Map.of(TP, 13L), tracker.committableOffsets());
    }

    @Test
    void failureInMiddleOfBatchDoesNotSkipPastFailedRecord() {
        Main.OffsetTracker tracker = new Main.OffsetTracker();
        tracker.recordSuccess(TP, 10);
        tracker.recordFailure(TP);
        assertTrue(tracker.isSuspended(TP));
        // offset 10 успешно обработан — коммит должен дойти до 11 (после
        // record 10), НЕ дальше: следующая запись (offset 11, та, что
        // провалилась) должна быть передоставлена, а не молча пропущена.
        assertEquals(Map.of(TP, 11L), tracker.committableOffsets());
    }

    @Test
    void independentPartitionsTrackedSeparately() {
        TopicPartition tp0 = new TopicPartition("billing.ledger", 0);
        TopicPartition tp1 = new TopicPartition("billing.ledger", 1);
        Main.OffsetTracker tracker = new Main.OffsetTracker();

        tracker.recordSuccess(tp0, 5);
        tracker.recordFailure(tp1);
        tracker.recordSuccess(tp0, 6);

        assertEquals(Map.of(tp0, 7L), tracker.committableOffsets());
        assertTrue(tracker.isSuspended(tp1));
    }

    @Test
    void noSuccessMeansNothingToCommit() {
        Main.OffsetTracker tracker = new Main.OffsetTracker();
        tracker.recordFailure(TP);
        assertEquals(Map.of(), tracker.committableOffsets());
    }
}
