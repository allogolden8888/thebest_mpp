package uz.mpp.deliveryreconciliation.kafkaio;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import uz.mpp.platformcontracts.common.v1.StageCompletedEvent;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.Future;

/**
 * publish_stage_completed (service_internal_methods.md §1.9) — реальный
 * kafka-clients Producer (plain, не Streams — services_specifictaion.md §2.9
 * "Kafka Client"), не проверялся против живого брокера в этой песочнице.
 */
public final class StageCompletedPublisher {

    private static final String TOPIC = "stage.completed";

    // 10с — как и в billing-service (тот же класс риска, см. ниже).
    private static final Duration PRODUCER_SEND_TIMEOUT = Duration.ofSeconds(10);

    private final Producer<String, byte[]> producer;

    public StageCompletedPublisher(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        // Тот же баг, что нашли и исправили в billing-service/KafkaIo
        // (buildProducer) и partner-smpp-gateway/IncomingPublisher: без явной
        // настройки KafkaProducer работает на дефолтах — buffer.memory=32MB,
        // max.block.ms=60000мс, из-за чего producer.send() может молча
        // заблокировать вызывающий поток на до 60с под backpressure, без
        // какой-либо защиты по таймауту. Фикс: 1) buffer.memory поднят; 2)
        // max.block.ms снижен до PRODUCER_SEND_TIMEOUT — под backpressure
        // send() бросает исключение за 10с, не блокирует поток на минуту.
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 67_108_864L); // 64MB (было 32MB по умолчанию)
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, PRODUCER_SEND_TIMEOUT.toMillis()); // 10s (было 60s по умолчанию)
        // linger.ms=0 по умолчанию — каждый send() уходит брокеру отдельным
        // запросом. 5мс даёт клиенту собрать пачку без заметного вклада в
        // латентность.
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
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
