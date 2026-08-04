package uz.mpp.deliveryreconciliation.kafkaio;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import uz.mpp.platformcontracts.common.v1.ReconciliationOutcome;
import uz.mpp.platformcontracts.common.v1.StageCompletedEvent;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

class StageCompletedPublisherTest {

    @Test
    void publishSendsKeyedByMessageId() throws Exception {
        MockProducer<String, byte[]> mock = new MockProducer<String, byte[]>(true, null, new StringSerializer(), new ByteArraySerializer());
        StageCompletedPublisher publisher = new StageCompletedPublisher(mock);

        UUID messageId = UUID.randomUUID();
        StageCompletedEvent event = StageCompletedBuilder.build(messageId, UUID.randomUUID(), 1,
            ReconciliationOutcome.RECONCILIATION_OUTCOME_DELIVERY_CONFIRMED, Instant.now());

        publisher.publish(event).get();

        List<ProducerRecord<String, byte[]>> history = mock.history();
        assertEquals(1, history.size());
        assertEquals("stage.completed", history.get(0).topic());
        assertEquals(messageId.toString(), history.get(0).key());
    }
}
