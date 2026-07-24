package uz.mpp.billing;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import uz.mpp.billing.KafkaIo.OffsetTracker;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Прямая проверка находки кодревью #1: {@code consumer.commitAsync()} без
 * аргументов коммитил позицию всего фетча, не оффсет обработанной записи —
 * если запись B в батче [A, B, C] падала, а C после неё успевала обработаться,
 * коммит после C навсегда терял B (при рестарте она не переобрабатывалась).
 * {@link OffsetTracker} — вынесенная чистая логика "что коммитить", теперь
 * тестируется без единого живого {@code KafkaConsumer}.
 */
class KafkaIoTest {

    private static final TopicPartition TP = new TopicPartition("stage.billing", 0);

    @Test
    void allSuccessfulCommitsPastLastRecord() {
        OffsetTracker tracker = new OffsetTracker();
        tracker.recordSuccess(TP, 10);
        tracker.recordSuccess(TP, 11);
        tracker.recordSuccess(TP, 12);
        assertEquals(Map.of(TP, 13L), tracker.committableOffsets());
    }

    @Test
    void failureInMiddleOfBatchStopsCommitBeforeIt_doesNotSkipPastFailedRecord() {
        // Ровно сценарий из кодревью: batch [A=10, B=11(падает), C=12].
        OffsetTracker tracker = new OffsetTracker();
        tracker.recordSuccess(TP, 10); // A

        tracker.recordFailure(TP); // B падает — партишен приостановлен

        assertTrue(tracker.isSuspended(TP), "после ошибки партишен должен быть приостановлен для этого поллинга");
        // C (offset=12) в реальном run() не будет даже передан в recordSuccess,
        // потому что isSuspended(TP) уже true — но даже если бы был вызван по ошибке,
        // коммит не должен перепрыгнуть через B:
        assertEquals(Map.of(TP, 11L), tracker.committableOffsets(),
            "коммитить нужно РОВНО до B (offset 10+1=11), не дальше — C, если бы обработался, не должен был бы сдвинуть коммит вперёд B");
    }

    @Test
    void independentPartitionsTrackedSeparately() {
        TopicPartition tp0 = new TopicPartition("stage.billing", 0);
        TopicPartition tp1 = new TopicPartition("stage.billing", 1);
        OffsetTracker tracker = new OffsetTracker();

        tracker.recordSuccess(tp0, 5);
        tracker.recordFailure(tp1); // ошибка в partition 1 не должна влиять на partition 0
        tracker.recordSuccess(tp0, 6);

        assertEquals(Map.of(tp0, 7L), tracker.committableOffsets(), "partition 0 коммитится нормально несмотря на ошибку в partition 1");
        assertTrue(tracker.isSuspended(tp1));
    }

    @Test
    void noSuccessMeansNothingToCommit() {
        OffsetTracker tracker = new OffsetTracker();
        tracker.recordFailure(TP);
        assertEquals(Map.of(), tracker.committableOffsets(), "ни одной успешной записи — нечего коммитить, не 'коммитить всё равно текущую позицию'");
    }
}
