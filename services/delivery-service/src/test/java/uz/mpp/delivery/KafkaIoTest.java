package uz.mpp.delivery;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import uz.mpp.delivery.KafkaIo.OffsetTracker;

/**
 * {@link OffsetTracker} — тот же паттерн, что уже найден и исправлен
 * кодревью в billing-service (commitAsync() без аргументов коммитит позицию
 * всего фетча, не оффсет обработанной записи) — применён здесь с самого
 * начала, тестируется тем же способом.
 */
class KafkaIoTest {

    private static final TopicPartition TP = new TopicPartition("stage.delivery", 0);

    @Test
    void allSuccessfulCommitsPastLastRecord() {
        OffsetTracker tracker = new OffsetTracker();
        tracker.recordSuccess(TP, 10);
        tracker.recordSuccess(TP, 11);
        tracker.recordSuccess(TP, 12);
        assertEquals(Map.of(TP, 13L), tracker.committableOffsets());
    }

    @Test
    void failureInMiddleOfBatchDoesNotSkipPastFailedRecord() {
        OffsetTracker tracker = new OffsetTracker();
        tracker.recordSuccess(TP, 10);
        tracker.recordFailure(TP);
        assertTrue(tracker.isSuspended(TP));
        assertEquals(Map.of(TP, 11L), tracker.committableOffsets());
    }

    @Test
    void independentPartitionsTrackedSeparately() {
        TopicPartition tp0 = new TopicPartition("stage.delivery", 0);
        TopicPartition tp1 = new TopicPartition("stage.delivery", 1);
        OffsetTracker tracker = new OffsetTracker();

        tracker.recordSuccess(tp0, 5);
        tracker.recordFailure(tp1);
        tracker.recordSuccess(tp0, 6);

        assertEquals(Map.of(tp0, 7L), tracker.committableOffsets());
        assertTrue(tracker.isSuspended(tp1));
    }

    @Test
    void noSuccessMeansNothingToCommit() {
        OffsetTracker tracker = new OffsetTracker();
        tracker.recordFailure(TP);
        assertEquals(Map.of(), tracker.committableOffsets());
    }
}
