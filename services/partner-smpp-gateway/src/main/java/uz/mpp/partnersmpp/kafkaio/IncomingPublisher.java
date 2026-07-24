package uz.mpp.partnersmpp.kafkaio;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import uz.mpp.platformcontracts.events.v1.IncomingMessage;

import java.util.Properties;
import java.util.concurrent.Future;

/**
 * publish_incoming (service_internal_methods.md §1.2): KafkaAck на
 * incoming.messages. Реальный Kafka-клиент (kafka-clients, стандартный —
 * services_specifictaion.md §2.2 "Kafka Client", не Streams), не проверялся
 * против живого брокера в этой песочнице (см. README).
 */
public final class IncomingPublisher {

    private static final String TOPIC = "incoming.messages";

    private final Producer<String, byte[]> producer;

    public IncomingPublisher(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        this.producer = new KafkaProducer<>(props);
    }

    IncomingPublisher(Producer<String, byte[]> producer) {
        this.producer = producer;
    }

    public Future<?> publish(IncomingMessage message) {
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(TOPIC, message.getMessageId(), message.toByteArray());
        return producer.send(record);
    }

    public void close() {
        producer.close();
    }
}