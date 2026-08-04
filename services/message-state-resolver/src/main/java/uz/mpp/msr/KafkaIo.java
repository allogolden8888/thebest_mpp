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

    /**
     * {@code restore_from_changelog} — закрывает часть HIGH находки кодревью
     * (PART 2, message-state-resolver #3, "no RocksDB" gap): раньше
     * {@link MessageStateStore} после рестарта пода стартовал полностью
     * пустым, из-за чего (а) реально-аномальная поздняя DLR молча
     * принималась как "message_id никогда не видели" вместо REGRESSION
     * (state_machines.md §1.3), и (б) {@code lifecycle_version} начинался
     * заново с 1, ломая монотонный инвариант версии для уже известных
     * сообщений. {@code message-state.changelog} — compacted-топик
     * (infra/kafka/generate_kafka_topics.py: CHANGELOG-категория, "текущее
     * состояние по ключу, не поток") ровно с той же ролью, что state-store
     * changelog в Kafka Streams — читаем его целиком (seekToBeginning до
     * зафиксированных на момент старта end offsets, тот же "restore" паттерн,
     * что делает Kafka Streams перед тем, как приложение станет READY) и
     * материализуем в {@link MessageStateStore} ДО начала обработки живого
     * трафика из {@code stage.completed}/{@code delivery.status}.
     *
     * <p>Не решает polностью тот же класс gap, что уже раскрыт в README
     * (нет continuous tailing changelog-топика во время работы — если
     * партиция этого инстанса меняется под ребалансировкой без рестарта
     * процесса, локальная проекция не обновляется) — закрывает конкретно
     * "восстановление после рестарта", то, что называет находка.</p>
     */
    public static void restoreFromChangelog(String bootstrapServers, MessageStateStore store) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "message-state-resolver-restore-" + UUID.randomUUID());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        try (KafkaConsumer<String, byte[]> restoreConsumer = new KafkaConsumer<>(props)) {
            List<org.apache.kafka.common.PartitionInfo> partitionInfos = restoreConsumer.partitionsFor(TOPIC_CHANGELOG);
            if (partitionInfos == null || partitionInfos.isEmpty()) {
                LOG.warning("restore_from_changelog: топик " + TOPIC_CHANGELOG + " недоступен/пуст на старте, restore пропущен");
                return;
            }
            List<TopicPartition> partitions = partitionInfos.stream()
                .map(pi -> new TopicPartition(pi.topic(), pi.partition()))
                .toList();
            restoreConsumer.assign(partitions);
            restoreConsumer.seekToBeginning(partitions);
            Map<TopicPartition, Long> endOffsets = restoreConsumer.endOffsets(partitions);

            java.util.Set<TopicPartition> remaining = new java.util.HashSet<>(partitions);
            remaining.removeIf(tp -> restoreConsumer.position(tp) >= endOffsets.get(tp));

            long restoredCount = 0;
            while (!remaining.isEmpty()) {
                ConsumerRecords<String, byte[]> records = restoreConsumer.poll(Duration.ofSeconds(5));
                for (ConsumerRecord<String, byte[]> record : records) {
                    if (record.value() == null) {
                        continue; // tombstone (compaction delete marker) — нет состояния, восстанавливать нечего
                    }
                    try {
                        MessageLifecycleEvent event = MessageLifecycleEvent.parseFrom(record.value());
                        LifecycleStatus status = LifecycleStatusMapping.fromProto(event.getStatus());
                        if (status == null) {
                            LOG.warning("restore_from_changelog: запись с UNSPECIFIED/UNRECOGNIZED статусом пропущена, message_id=" + event.getMessageId());
                            continue;
                        }
                        store.put(event.getMessageId(),
                            new LifecycleState(status, event.getLifecycleVersion(), event.getSourceEventId()));
                        restoredCount++;
                    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                        LOG.log(Level.WARNING, "restore_from_changelog: нераспарсиваемая запись partition="
                            + record.partition() + " offset=" + record.offset() + " пропущена", e);
                    }
                }
                remaining.removeIf(tp -> restoreConsumer.position(tp) >= endOffsets.get(tp));
            }
            LOG.info("restore_from_changelog: восстановлено " + restoredCount + " состояний из " + TOPIC_CHANGELOG
                + " (" + partitions.size() + " партиций) перед стартом обработки живого трафика");
        }
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
                // CRITICAL находка кодревью: store.put(...) раньше применялся
                // прямо внутри processRecord, ДО commitTransaction() и без
                // отката на abortTransaction() — на abort Kafka-эффекты (changelog/
                // lifecycle/offset) реально откатываются, а локальная
                // материализованная проекция в MessageStateStore — нет, и
                // расходится с зафиксированной истиной. pendingUpdates —
                // батч-локальный staging: чтения внутри батча видят более
                // ранние pending-записи того же батча (нужно для цепочки
                // транзиций одного message_id в одном poll), а в реальный
                // store они применяются одним проходом только ПОСЛЕ успешного
                // commitTransaction(); на abort — просто отбрасываются.
                Map<String, LifecycleState> pendingUpdates = new HashMap<>();
                try {
                    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                    for (ConsumerRecord<String, byte[]> record : records) {
                        processRecord(record, store, pendingUpdates, producer);
                        TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                        offsets.merge(tp, new OffsetAndMetadata(record.offset() + 1),
                            (existing, next) -> next.offset() > existing.offset() ? next : existing);
                    }
                    producer.sendOffsetsToTransaction(offsets, consumer.groupMetadata());
                    producer.commitTransaction();
                    pendingUpdates.forEach(store::put);
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

    private static void processRecord(
        ConsumerRecord<String, byte[]> record,
        MessageStateStore store,
        Map<String, LifecycleState> pendingUpdates,
        KafkaProducer<String, byte[]> producer
    ) throws Exception {
        String messageId;
        String eventId;
        LifecycleStatus candidate;

        // HIGH находка кодревью: раньше InvalidProtocolBufferException здесь
        // распространялась наружу и валила ВЕСЬ batch-transaction (abort +
        // переобработка всего батча), делая одну поломанную запись poison
        // pill'ом для ВСЕХ остальных сообщений того же батча/партиции
        // навсегда — редеставка снова натыкается на ту же нераспарсиваемую
        // запись. delivery-service/dlr-manager для этого класса ошибок
        // изолируют одну запись, не блокируют остальные — здесь то же самое:
        // parse-failure одной записи логируется и пропускается (offset всё
        // равно уйдёт в коммит транзакции вместе с остальными записями того
        // же батча), не откатывает состояние ДРУГИХ сообщений в этом батче.
        if (record.topic().equals(TOPIC_STAGE_COMPLETED)) {
            StageCompletedEvent event;
            try {
                event = StageCompletedEvent.parseFrom(record.value());
            } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                LOG.log(Level.SEVERE, "не удалось декодировать StageCompletedEvent partition=" + record.partition()
                    + " offset=" + record.offset() + ", запись пропущена (не блокирует остальной батч)", e);
                return;
            }
            var mapped = CandidateTransitionResolver.fromStageCompleted(event);
            if (mapped.isEmpty()) {
                return;
            }
            messageId = event.getMessageId();
            eventId = event.getEventId();
            candidate = mapped.get();
        } else {
            DeliveryStatusEvent event;
            try {
                event = DeliveryStatusEvent.parseFrom(record.value());
            } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                LOG.log(Level.SEVERE, "не удалось декодировать DeliveryStatusEvent partition=" + record.partition()
                    + " offset=" + record.offset() + ", запись пропущена (не блокирует остальной батч)", e);
                return;
            }
            var mapped = CandidateTransitionResolver.fromDeliveryStatus(event);
            if (mapped.isEmpty()) {
                LOG.warning("delivery.status с нераспознанным normalized_status=" + event.getNormalizedStatus() + " для message_id=" + event.getMessageId());
                return;
            }
            messageId = event.getMessageId();
            eventId = event.getEventId();
            candidate = mapped.get();
        }

        // Читаем сначала батч-локальный staging (более ранняя запись того же
        // message_id в ЭТОМ ЖЕ батче ещё не в store, но должна быть видна
        // следующей записи для цепочки транзиций) и только потом — реальный
        // store.
        LifecycleState current = pendingUpdates.getOrDefault(messageId, store.get(messageId));
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

        pendingUpdates.put(messageId, result.state());
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
