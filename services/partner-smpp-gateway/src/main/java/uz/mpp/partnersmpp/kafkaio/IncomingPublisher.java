package uz.mpp.partnersmpp.kafkaio;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import uz.mpp.platformcontracts.events.v1.IncomingMessage;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;

/**
 * publish_incoming (service_internal_methods.md §1.2): KafkaAck на
 * incoming.messages. Реальный Kafka-клиент (kafka-clients, стандартный —
 * services_specifictaion.md §2.2 "Kafka Client", не Streams), не проверялся
 * против живого брокера в этой песочнице (см. README).
 */
public final class IncomingPublisher {

    private static final String TOPIC = "incoming.messages";

    // 10с — как и в billing-service (тот же класс риска, см. ниже).
    private static final Duration PRODUCER_SEND_TIMEOUT = Duration.ofSeconds(10);

    private final Producer<String, byte[]> producer;

    public IncomingPublisher(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        // Тот же баг, что нашли и исправили в billing-service/KafkaIo
        // (buildProducer): без явной настройки KafkaProducer работает на
        // дефолтах — buffer.memory=32MB, max.block.ms=60000мс. Здесь риск
        // ОСТРЕЕ, не такой же: publish() вызывается синхронно прямо из
        // SmppServerHandler.channelRead0 — общего Netty I/O event-loop
        // потока, который делит несколько SMPP-сессий одновременно. Если
        // send() молча заблокирует ЭТОТ поток на до 60с в ожидании места в
        // буфере под backpressure, зависает не один воркер — зависают ВСЕ
        // SMPP-сессии, приписанные к этому event-loop потоку. Фикс: 1)
        // buffer.memory поднят; 2) max.block.ms снижен до
        // PRODUCER_SEND_TIMEOUT — под backpressure send() бросает
        // исключение за 10с, не блокирует event-loop поток на минуту.
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 67_108_864L); // 64MB (было 32MB по умолчанию)
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, PRODUCER_SEND_TIMEOUT.toMillis()); // 10s (было 60s по умолчанию)
        // linger.ms=0 по умолчанию — каждый send() уходит брокеру отдельным
        // запросом. 5мс даёт клиенту собрать пачку без заметного вклада в
        // латентность одиночного SMPP submit_sm.
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        this.producer = new KafkaProducer<>(props);
    }

    IncomingPublisher(Producer<String, byte[]> producer) {
        this.producer = producer;
    }

    public Future<?> publish(IncomingMessage message) {
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(TOPIC, message.getMessageId(), message.toByteArray());
        return producer.send(record);
    }

    /**
     * HIGH находка кодревью (CODE_REVIEW.md, "partner-smpp-gateway" #2):
     * {@code publish(IncomingMessage)} выше — fire-and-forget, {@link Future}
     * результата отбрасывался вызывающей стороной, и {@code submit_sm_resp
     * ESME_ROK} отправлялся партнёру немедленно, до подтверждения публикации.
     * Этот overload — с callback, вызываемым ПОСЛЕ реального ack от Kafka
     * (или ошибки) — {@code SmppServerHandler} теперь отвечает партнёру
     * только после этого callback'а, не раньше.
     */
    public void publish(IncomingMessage message, BiConsumer<Void, Throwable> callback) {
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(TOPIC, message.getMessageId(), message.toByteArray());
        producer.send(record, (metadata, exception) -> callback.accept(null, exception));
    }

    public void close() {
        producer.close();
    }
}