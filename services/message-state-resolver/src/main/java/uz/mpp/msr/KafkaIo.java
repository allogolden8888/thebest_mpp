package uz.mpp.msr;

import com.google.protobuf.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import uz.mpp.msr.TransitionValidator.ApplyResult;
import uz.mpp.msr.TransitionValidator.Verdict;
import uz.mpp.platformcontracts.common.v1.StageCompletedEvent;
import uz.mpp.platformcontracts.events.v1.DeliveryStatusEvent;
import uz.mpp.platformcontracts.events.v1.MessageLifecycleEvent;

/**
 * {@code commit_transaction} (service_internal_methods.md §2.4, HLD §10.1):
 * одна Kafka-транзакция — запись в {@code message-state.changelog} +
 * {@code message.lifecycle} + commit input offset. Реальный transactional
 * {@link KafkaProducer} (initTransactions/beginTransaction/
 * sendOffsetsToTransaction/commitTransaction) — тот же API, на котором
 * построен exactly_once_v2 в Kafka Streams, здесь используется напрямую,
 * без Streams DSL поверх него (см. README "Что упрощено").
 */
public final class KafkaIo {

    private static final Logger LOG = Logger.getLogger(KafkaIo.class.getName());
    public static final String TOPIC_STAGE_COMPLETED = "stage.completed";
    public static final String TOPIC_DELIVERY_STATUS = "delivery.status";
    public static final String TOPIC_LIFECYCLE = "message.lifecycle";
    public static final String TOPIC_CHANGELOG = "message-state.changelog";

    public static KafkaConsumer<String, byte[]> buildConsumer(String bootstrapServers, String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        return new KafkaConsumer<>(props);
    }

    /**
     * {@code transactionalId} — найдено при реализации, не решено здесь
     * полностью: правильная привязка {@code transactional.id} к владению
     * партицией под ребалансировкой (то, что Kafka Streams EOS решает
     * автоматически через group-instance fencing) НЕ реализована в этом
     * срезе — {@code transactionalId} передаётся как стабильный
     * per-instance идентификатор (см. Main.java), корректно для одной
     * реплики или ручного статичного распределения партиций, не
     * гарантированно safe при произвольном rolling restart с несколькими
     * репликами. См. README "Что НЕ реализовано".
     */
    public static KafkaProducer<String, byte[]> buildTransactionalProducer(String bootstrapServers, String transactionalId) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props);
        producer.initTransactions();
        return producer;
    }

    public static void run(
        KafkaConsumer<String, byte[]> consumer,
        KafkaProducer<String, byte[]> producer,
        MessageStateStore store,
        AtomicBoolean running
    ) {
        consumer.subscribe(List.of(TOPIC_STAGE_COMPLETED, TOPIC_DELIVERY_STATUS));
        try {
            while (running.get()) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofSeconds(1));
                if (records.isEmpty()) {
                    continue;
                }

                producer.beginTransaction();
                try {
                    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                    for (ConsumerRecord<String, byte[]> record : records) {
                        processRecord(record, store, producer);
                        TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                        offsets.merge(tp, new OffsetAndMetadata(record.offset() + 1),
                            (existing, next) -> next.offset() > existing.offset() ? next : existing);
                    }
                    producer.sendOffsetsToTransaction(offsets, consumer.groupMetadata());
                    producer.commitTransaction();
                } catch (Exception e) {
                    LOG.log(Level.SEVERE, "транзакция прервана, батч будет переобработан", e);
                    producer.abortTransaction();
                }
            }
        } catch (org.apache.kafka.common.errors.WakeupException e) {
            if (running.get()) {
                throw e;
            }
        }
    }

    private static void processRecord(ConsumerRecord<String, byte[]> record, MessageStateStore store, KafkaProducer<String, byte[]> producer) throws Exception {
        String messageId;
        String eventId;
        LifecycleStatus candidate;

        if (record.topic().equals(TOPIC_STAGE_COMPLETED)) {
            StageCompletedEvent event = StageCompletedEvent.parseFrom(record.value());
            var mapped = CandidateTransitionResolver.fromStageCompleted(event);
            if (mapped.isEmpty()) {
                return;
            }
            messageId = event.getMessageId();
            eventId = event.getEventId();
            candidate = mapped.get();
        } else {
            DeliveryStatusEvent event = DeliveryStatusEvent.parseFrom(record.value());
            var mapped = CandidateTransitionResolver.fromDeliveryStatus(event);
            if (mapped.isEmpty()) {
                LOG.warning("delivery.status с нераспознанным normalized_status=" + event.getNormalizedStatus() + " для message_id=" + event.getMessageId());
                return;
            }
            messageId = event.getMessageId();
            eventId = event.getEventId();
            candidate = mapped.get();
        }

        LifecycleState current = store.get(messageId);
        ApplyResult result = TransitionValidator.apply(current, candidate, eventId);

        if (result.verdict() == Verdict.REGRESSION) {
            // HLD §10 / state_machines.md §1.3: аномалия оператора/системы,
            // логируется, история НЕ переписывается. Запись в
            // reconciliation_cases (упомянута в state_machines.md) не
            // реализована в этом срезе — только лог, см. README.
            LOG.warning("REGRESSION: message_id=" + messageId + " current=" + current.status()
                + " candidate=" + candidate + " event_id=" + eventId + " — отклонено, lifecycle не изменён");
            return;
        }
        if (result.verdict() == Verdict.DUPLICATE) {
            return; // no-op — оффсет всё равно коммитится транзакцией снаружи
        }

        store.put(messageId, result.state());
        MessageLifecycleEvent lifecycleEvent = buildLifecycleEvent(messageId, eventId, result.state());
        byte[] payload = lifecycleEvent.toByteArray();
        producer.send(new ProducerRecord<>(TOPIC_CHANGELOG, messageId, payload));
        producer.send(new ProducerRecord<>(TOPIC_LIFECYCLE, messageId, payload));
    }

    private static MessageLifecycleEvent buildLifecycleEvent(String messageId, String sourceEventId, LifecycleState state) {
        return MessageLifecycleEvent.newBuilder()
            .setEventId(UUID.randomUUID().toString())
            .setMessageId(messageId)
            .setStatus(LifecycleStatusMapping.toProto(state.status()))
            .setLifecycleVersion(state.lifecycleVersion())
            .setTerminal(TransitionValidator.TERMINAL.contains(state.status()))
            .setSourceEventId(sourceEventId)
            .setOccurredAt(toTimestamp(Instant.now()))
            .build();
    }

    private static Timestamp toTimestamp(Instant instant) {
        return Timestamp.newBuilder().setSeconds(instant.getEpochSecond()).setNanos(instant.getNano()).build();
    }
}
