package uz.mpp.partnersmpp.admission;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExecutionControlConsumerTest {

    @Test
    void bootstrapCompletesOnlyAfterEveryPartitionReachesCapturedEndOffset() {
        TopicPartition first = new TopicPartition(ExecutionControlConsumer.TOPIC, 0);
        TopicPartition second = new TopicPartition(ExecutionControlConsumer.TOPIC, 1);
        Map<TopicPartition, Long> ends = Map.of(first, 5L, second, 9L);

        assertFalse(ExecutionControlConsumer.caughtUp(ends, Map.of(first, 5L)));
        assertFalse(ExecutionControlConsumer.caughtUp(ends, Map.of(first, 5L, second, 8L)));
        assertTrue(ExecutionControlConsumer.caughtUp(ends, Map.of(first, 5L, second, 9L)));
        assertTrue(ExecutionControlConsumer.caughtUp(ends, Map.of(first, 7L, second, 11L)));
    }
}
