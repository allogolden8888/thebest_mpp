package uz.mpp.partnersmpp.kafkaio;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import uz.mpp.platformcontracts.common.v1.ConfigEntityType;
import uz.mpp.platformcontracts.events.v1.ConfigChangeEvent;

import java.time.Duration;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * {@code config.changes} consumer — закрывает разрыв, описанный в
 * {@code BACKOFFICE_ROADMAP.md} "Production Readiness Review" P0#4: этот
 * сервис раньше вообще не потреблял {@code config.changes} (README
 * "Что НЕ реализовано" честно фиксировало это как известный пробел —
 * "тот же паттерн упрощения, что у Routing/Policy/partner-rest-receiver/
 * partner-notification-service", все они на момент того README читали
 * partner-конфиг только один раз при старте).
 *
 * <p>Только {@code entity_type=PARTNER} события интересны здесь. В
 * production callback применяет immutable {@code payload_json} события
 * напрямую через {@link PartnerConfigStore#applyEvent}; ждать независимый
 * config-cache-projector нельзя — он может записать эту версию в Redis как
 * до, так и после данного consumer. Redis используется только как быстрый
 * стартовый snapshot, а Kafka version fencing догоняет его без rollback.
 *
 * <p><b>Каждый под — свой независимый consumer group.</b> Это не выбор
 * "для надёжного replay на рестарте" (хотя даёт и это, {@code
 * auto.offset.reset=earliest} — компактированный топик, полный обход
 * гарантированно покрывает разрыв между {@code PartnerConfigStore.bootstrap()}
 * (Redis-снапшот на момент T1) и подпиской этого консьюмера (T2) — тот же
 * мотив, что {@code routing-service/src/config_reload.rs} документирует
 * для своего {@code unique_group_id}), а СТРУКТУРНАЯ необходимость: этот
 * сервис — StatefulSet с несколькими репликами, каждая из которых держит
 * СВОИ TCP SMPP-сессии и должна видеть КАЖДОЕ {@code config.changes}
 * событие независимо. Общий consumer group на все поды партиционировал бы
 * события между репликами (обычная Kafka-семантика consumer group) — тогда
 * только ОДНА реплика увидела бы ротацию конкретного партнёра, остальные
 * тихо остались бы на старом конфиге.
 *
 * <p><b>Коммит — только после успешного применения.</b> При ошибке consumer
 * делает seek на упавший offset, readiness краснеет и запись повторяется в
 * текущем процессе, а не только после restart/rebalance.
 */
public final class ConfigChangeConsumer implements AutoCloseable {

    public static final String TOPIC = "config.changes";

    private static final Logger LOG = Logger.getLogger(ConfigChangeConsumer.class.getName());

    // org.apache.kafka.clients.consumer.Consumer<K,V> — интерфейс, а не
    // KafkaConsumer<K,V> (конкретный класс) — так тестовый конструктор
    // ниже может принять org.apache.kafka.clients.consumer.MockConsumer
    // (тот же официальный in-memory test double, что IncomingPublisherTest
    // уже использует как MockProducer для продюсера). Полное имя вместо
    // импорта — не строго обязательно теперь (partnerRefresher больше не
    // java.util.function.Consumer, конфликта имён нет), но сохранено, чтобы
    // не трогать лишний импорт ради минимального диффа этой правки.
    private final org.apache.kafka.clients.consumer.Consumer<String, byte[]> consumer;
    private final Consumer<ConfigChangeEvent> partnerRefresher;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicBoolean initialReplayReady = new AtomicBoolean(false);
    private final CountDownLatch initialReplayFinished = new CountDownLatch(1);
    private final Set<TopicPartition> stalledPartitions = ConcurrentHashMap.newKeySet();
    private volatile RuntimeException terminalFailure;
    private Thread thread;

    /**
     * @param partnerRefresher получает полный immutable PARTNER event; в
     *                         production применяет payload с version fence и
     *                         при архивации закрывает живые сессии. Функциональный
     *                         интерфейс сохраняет batch-логику тестируемой без
     *                         Kafka и Redis.
     */
    public ConfigChangeConsumer(String bootstrapServers, String groupId, Consumer<ConfigChangeEvent> partnerRefresher) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        // Компактированный топик, свежая (уникальная на каждый запуск) группа —
        // см. javadoc класса "Каждый под — свой независимый consumer group".
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        this.consumer = new KafkaConsumer<>(props);
        this.partnerRefresher = partnerRefresher;
    }

    /** Тестовый конструктор — принимает уже сконфигурированный/мок consumer напрямую. */
    ConfigChangeConsumer(org.apache.kafka.clients.consumer.Consumer<String, byte[]> consumer, Consumer<ConfigChangeEvent> partnerRefresher) {
        this.consumer = consumer;
        this.partnerRefresher = partnerRefresher;
    }

    public void start() {
        thread = new Thread(this::run, "partner-smpp-config-changes");
        thread.setDaemon(true);
        thread.start();
    }

    /**
     * Fail-closed startup barrier. The SMPP listener must not accept binds
     * from the Redis bootstrap snapshot until every partition has been
     * replayed through the end offset captured for this process.
     */
    public void awaitInitialReplay(Duration timeout) {
        try {
            if (!initialReplayFinished.await(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
                throw new IllegalStateException("config.changes initial replay timeout after " + timeout);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while waiting for config.changes initial replay", e);
        }
        if (!initialReplayReady.get()) {
            throw new IllegalStateException("config.changes initial replay failed", terminalFailure);
        }
    }

    /** Readiness signal: initial replay completed and no partition is stuck. */
    public boolean isHealthy() {
        Thread activeThread = thread;
        return initialReplayReady.get()
            && terminalFailure == null
            && stalledPartitions.isEmpty()
            && activeThread != null
            && activeThread.isAlive();
    }

    private void run() {
        consumer.subscribe(List.of(TOPIC));
        Map<TopicPartition, Long> replayEnds = null;
        try {
            while (running.get()) {
                ConsumerRecords<String, byte[]> records;
                try {
                    records = consumer.poll(Duration.ofSeconds(1));
                } catch (WakeupException e) {
                    if (running.get()) {
                        throw e;
                    }
                    break;
                }
                processBatch(records);

                if (replayEnds == null) {
                    Set<TopicPartition> assignment = consumer.assignment();
                    if (!assignment.isEmpty()) {
                        replayEnds = consumer.endOffsets(assignment, Duration.ofSeconds(10));
                    }
                }
                if (!initialReplayReady.get() && replayEnds != null && caughtUp(replayEnds, currentPositions(replayEnds.keySet()))) {
                    initialReplayReady.set(true);
                    initialReplayFinished.countDown();
                    LOG.info("config.changes initial replay reached captured end of every partition");
                }
            }
        } catch (RuntimeException e) {
            terminalFailure = e;
            LOG.log(Level.SEVERE, "config.changes consumer stopped", e);
        } finally {
            initialReplayFinished.countDown();
            consumer.close();
        }
    }

    private Map<TopicPartition, Long> currentPositions(Set<TopicPartition> partitions) {
        Map<TopicPartition, Long> positions = new HashMap<>();
        for (TopicPartition partition : partitions) {
            positions.put(partition, consumer.position(partition, Duration.ofSeconds(10)));
        }
        return positions;
    }

    static boolean caughtUp(Map<TopicPartition, Long> ends, Map<TopicPartition, Long> positions) {
        if (ends.isEmpty() || !positions.keySet().containsAll(ends.keySet())) {
            return false;
        }
        for (Map.Entry<TopicPartition, Long> end : ends.entrySet()) {
            if (positions.get(end.getKey()) < end.getValue()) {
                return false;
            }
        }
        return true;
    }

    /** Пакетная логика вынесена отдельно — тестируема без реального опроса Kafka (см. {@code ConfigChangeConsumerTest}). */
    void processBatch(ConsumerRecords<String, byte[]> records) {
        for (TopicPartition tp : records.partitions()) {
            long nextCommittable = -1;
            for (ConsumerRecord<String, byte[]> record : records.records(tp)) {
                try {
                    handle(record);
                    stalledPartitions.remove(tp);
                    nextCommittable = record.offset() + 1;
                } catch (RuntimeException e) {
                    // KafkaConsumer уже сдвинул локальную position за весь
                    // полученный batch. Одного "не коммитим" недостаточно:
                    // без seek текущий процесс продолжил бы со следующего
                    // batch, а упавшая запись повторилась бы только после
                    // restart/rebalance. Явно возвращаем position на первый
                    // необработанный offset; успешно обработанный префикс
                    // ниже по-прежнему можно закоммитить.
                    consumer.seek(tp, record.offset());
                    stalledPartitions.add(tp);
                    LOG.log(Level.WARNING, e, () -> "config.changes: application failed at "
                        + tp + " offset=" + record.offset() + "; seek для немедленного retry");
                    break;
                }
            }
            if (nextCommittable >= 0) {
                consumer.commitSync(Map.of(tp, new OffsetAndMetadata(nextCommittable)));
            }
        }
    }

    private void handle(ConsumerRecord<String, byte[]> record) {
        byte[] value = record.value();
        if (value == null) {
            // Tombstone (compacted-топик) — ни один текущий консьюмер config.changes
            // (config-cache-projector, Rust config_reload.rs) не публикует их для
            // entity_type=PARTNER сегодня; безопасный no-op, не ошибка.
            return;
        }

        ConfigChangeEvent event;
        try {
            event = ConfigChangeEvent.parseFrom(value);
        } catch (InvalidProtocolBufferException e) {
            LOG.log(Level.WARNING, e, () -> "config.changes: не удалось распарсить ConfigChangeEvent offset=" + record.offset() + " — пропущено (poison-сообщение, не блокируем партицию навечно)");
            return;
        }

        if (event.getEntityType() != ConfigEntityType.CONFIG_ENTITY_TYPE_PARTNER) {
            return;
        }
        String partnerId = event.getEntityId();
        if (partnerId.isEmpty()) {
            LOG.warning(() -> "config.changes: entity_type=PARTNER событие offset=" + record.offset() + " без entity_id — пропущено");
            return;
        }

        if (record.key() == null || !partnerId.equals(record.key())) {
            LOG.warning(() -> "config.changes: Kafka key не совпадает с PARTNER entity_id=" + partnerId
                + " offset=" + record.offset() + " — событие пропущено");
            return;
        }

        LOG.info(() -> "config.changes: partner_id=" + partnerId + " status=" + event.getStatus()
            + " version=" + event.getVersion() + " — применяем versioned payload к живому SMPP snapshot");
        partnerRefresher.accept(event);
    }

    @Override
    public void close() {
        running.set(false);
        consumer.wakeup();
        if (thread != null) {
            try {
                thread.join(Duration.ofSeconds(5).toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
