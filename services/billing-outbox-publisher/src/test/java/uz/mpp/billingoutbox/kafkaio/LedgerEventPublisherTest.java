package uz.mpp.billingoutbox.kafkaio;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import uz.mpp.billingoutbox.core.StreamEntry;
import uz.mpp.platformcontracts.events.v1.LedgerEvent;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class LedgerEventPublisherTest {

    @Test
    void publishKeyedByAccountId() throws Exception {
        MockProducer<String, byte[]> mock = new MockProducer<String, byte[]>(true, null, new StringSerializer(), new ByteArraySerializer());
        LedgerEventPublisher publisher = new LedgerEventPublisher(mock);

        StreamEntry entry = new StreamEntry(0, "1-0", "charge-1", "acc-1", "acme", 1000, "UZS", "charge", null, "", System.currentTimeMillis());
        LedgerEvent event = LedgerEventBuilder.build(entry);
        publisher.publish(event).get();

        List<ProducerRecord<String, byte[]>> history = mock.history();
        assertEquals(1, history.size());
        assertEquals("billing.ledger", history.get(0).topic());
        assertEquals("acc-1", history.get(0).key());
    }
}