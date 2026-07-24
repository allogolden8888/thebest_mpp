package uz.mpp.deliveryreconciliation.kafkaio;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import uz.mpp.platformcontracts.common.v1.StageCompletedEvent;

import java.util.Properties;
import java.util.concurrent.Future;

/**
 * publish_stage_completed (service_internal_methods.md §1.9) — реальный
 * kafka-clients Producer (plain, не Streams — services_specifictaion.md §2.9
 * "Kafka Client"), не проверялся против живого брокера в этой песочнице.
 */
public final class StageCompletedPublisher {

    private static final String TOPIC = "stage.completed";

    private final Producer<String, byte[]> producer;

    public StageCompletedPublisher(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        this.producer = new KafkaProducer<>(props);
    }

    public StageCompletedPublisher(Producer<String, byte[]> producer) {
        this.producer = producer;
    }

    public Future<?> publish(StageCompletedEvent event) {
        return producer.send(new ProducerRecord<>(TOPIC, event.getMessageId(), event.toByteArray()));
    }

    public void close() {
        producer.close();
    }
}
