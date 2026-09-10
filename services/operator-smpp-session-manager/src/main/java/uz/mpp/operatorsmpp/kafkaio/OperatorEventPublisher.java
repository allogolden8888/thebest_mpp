package uz.mpp.operatorsmpp.kafkaio;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import uz.mpp.platformcontracts.events.v1.OperatorDlr;
import uz.mpp.platformcontracts.events.v1.OperatorPduLog;
import uz.mpp.platformcontracts.events.v1.OperatorSubmitAccepted;

import java.time.Duration;
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
    // per-PDU диагностический след (BACKOFFICE_DESIGN_SPEC.md Экраны 38-40)
    // — отдельный топик от operator.submit.accepted/operator.dlr: те несут
    // бизнес-события корреляции (ровно одно на сообщение/сегмент), этот —
    // КАЖДЫЙ PDU, реально пересечённый по проводу (submit_sm, submit_sm_resp,
    // deliver_sm, deliver_sm_resp), на порядок выше объём.
    private static final String PDU_LOG_TOPIC = "operator.pdu.log";

    // 10с — как и в billing-service/partner-smpp-gateway (тот же класс риска,
    // см. ниже).
    private static final Duration PRODUCER_SEND_TIMEOUT = Duration.ofSeconds(10);

    private final Producer<String, byte[]> producer;

    public OperatorEventPublisher(String bootstrapServers) {
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
        // NEXT_STEPS_1500TPS.md 1.1: linger.ms=0 по умолчанию — 5мс даёт
        // клиенту собрать пачку без заметного вклада в p50/p95.
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
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

    // Ключевание по operator_id — тот же принцип, что publishSubmitAccepted/
    // publishDlr выше (service_io_contracts.md "Ключевание").
    public Future<?> publishPduLog(OperatorPduLog event) {
        return producer.send(new ProducerRecord<>(PDU_LOG_TOPIC, event.getOperatorId(), event.toByteArray()));
    }

    public void close() {
        producer.close();
    }
}