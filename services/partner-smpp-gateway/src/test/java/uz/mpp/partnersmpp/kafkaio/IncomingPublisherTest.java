package uz.mpp.partnersmpp.kafkaio;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import uz.mpp.platformcontracts.common.v1.Channel;
import uz.mpp.platformcontracts.events.v1.IncomingMessage;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * MockProducer — официальный in-memory test double из kafka-clients (не
 * самодельный мок) — реально прогоняет ProducerRecord через сериализацию.
 */
class IncomingPublisherTest {

    @Test
    void publishSendsRecordWithMessageIdAsKey() throws Exception {
        MockProducer<String, byte[]> mockProducer =
            new MockProducer<String, byte[]>(true, null, new StringSerializer(), new ByteArraySerializer());
        IncomingPublisher publisher = new IncomingPublisher(mockProducer);

        IncomingMessage msg = IncomingMessage.newBuilder()
            .setMessageId("msg-1")
            .setPartnerId("acme")
            .setChannel(Channel.CHANNEL_SMS)
            .build();

        publisher.publish(msg).get();

        List<ProducerRecord<String, byte[]>> history = mockProducer.history();
        assertEquals(1, history.size());
        assertEquals("incoming.messages", history.get(0).topic());
        assertEquals("msg-1", history.get(0).key());
        assertEquals(msg, IncomingMessage.parseFrom(history.get(0).value()));
    }
}