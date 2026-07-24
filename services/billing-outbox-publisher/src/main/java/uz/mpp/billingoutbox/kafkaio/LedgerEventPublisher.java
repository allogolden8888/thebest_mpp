package uz.mpp.billingoutbox.kafkaio;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import uz.mpp.platformcontracts.events.v1.LedgerEvent;

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

    private final Producer<String, byte[]> producer;

    public LedgerEventPublisher(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
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