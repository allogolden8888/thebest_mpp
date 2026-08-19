package uz.mpp.billingoutbox.kafkaio;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import uz.mpp.platformcontracts.events.v1.LedgerEvent;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.Future;

/**
 * publish_ledger_event (service_internal_methods.md §5.1) — реальный
 * kafka-clients Producer, ключ = account_id (service_io_contracts.md
 * "Ключевание": "billing.ledger — по account_id, чтобы Ledger Writer видел
 * все операции одного счёта по порядку"). Не проверялся против живого
 * брокера в этой песочнице.
 */
public final class LedgerEventPublisher {

    private static final String TOPIC = "billing.ledger";

    // 10с — как и в billing-service (тот же класс риска, см. ниже).
    private static final Duration PRODUCER_SEND_TIMEOUT = Duration.ofSeconds(10);

    private final Producer<String, byte[]> producer;

    public LedgerEventPublisher(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        // Тот же баг, что нашли и исправили в billing-service/KafkaIo
        // (buildProducer): без явной настройки KafkaProducer работает на
        // дефолтах — buffer.memory=32MB, max.block.ms=60000мс — send()
        // молча БЛОКИРУЕТ вызывающий поток до 60с в ожидании места в буфере
        // под backpressure, ДО того как вернуть Future, то есть до
        // существующего .get() таймаута дело даже не доходит. Фикс: 1)
        // buffer.memory поднят; 2) max.block.ms снижен до
        // PRODUCER_SEND_TIMEOUT — под backpressure send() бросает
        // исключение за 10с, не зависает молча на минуту.
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 67_108_864L); // 64MB (было 32MB по умолчанию)
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, PRODUCER_SEND_TIMEOUT.toMillis()); // 10s (было 60s по умолчанию)
        // linger.ms=0 по умолчанию — каждый send() уходит брокеру отдельным
        // запросом. 5мс даёт клиенту собрать пачку без заметного вклада в
        // латентность.
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        this.producer = new KafkaProducer<>(props);
    }

    public LedgerEventPublisher(Producer<String, byte[]> producer) {
        this.producer = producer;
    }

    public Future<?> publish(LedgerEvent event) {
        return producer.send(new ProducerRecord<>(TOPIC, event.getAccountId(), event.toByteArray()));
    }

    public void close() {
        producer.close();
    }
}