package uz.mpp.operatorsmpp.kafkaio;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import uz.mpp.platformcontracts.events.v1.OperatorDlr;
import uz.mpp.platformcontracts.events.v1.OperatorSubmitAccepted;

import java.util.Properties;
import java.util.concurrent.Future;

/**
 * publish_submit_accepted + publish_operator_dlr (service_internal_methods.md
 * §1.3) — ключевание по operator_id (service_io_contracts.md "Ключевание":
 * "operator.* — по operator_id, чтобы DLR Manager мог параллелить
 * корреляцию по оператору без потери упорядоченности внутри одного
 * оператора"). Реальный kafka-clients Producer, не проверялся против
 * живого брокера в этой песочнице.
 */
public final class OperatorEventPublisher {

    private static final String SUBMIT_ACCEPTED_TOPIC = "operator.submit.accepted";
    private static final String DLR_TOPIC = "operator.dlr";

    private final Producer<String, byte[]> producer;

    public OperatorEventPublisher(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        this.producer = new KafkaProducer<>(props);
    }

    public OperatorEventPublisher(Producer<String, byte[]> producer) {
        this.producer = producer;
    }

    public Future<?> publishSubmitAccepted(OperatorSubmitAccepted event) {
        return producer.send(new ProducerRecord<>(SUBMIT_ACCEPTED_TOPIC, event.getOperatorId(), event.toByteArray()));
    }

    public Future<?> publishDlr(OperatorDlr event) {
        return producer.send(new ProducerRecord<>(DLR_TOPIC, event.getOperatorId(), event.toByteArray()));
    }

    public void close() {
        producer.close();
    }
}