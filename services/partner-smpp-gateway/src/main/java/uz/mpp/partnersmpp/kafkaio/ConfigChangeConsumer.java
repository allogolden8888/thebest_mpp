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
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
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
 * <p>Только {@code entity_type=PARTNER} события интересны здесь. На каждое
 * релевантное событие вызывается {@code
 * PartnerConfigStore.refreshPartner(partnerId, status)} — передаётся и
 * {@code status} самого события, не только {@code entity_id}: {@link
 * PartnerConfigStore} нуждается в нём, чтобы отличить "версия стала
 * активной, можно безопасно re-fetch'ить {@code config:current} из Redis"
 * от "версия архивная, {@code config:current} НЕ переставлен
 * config-cache-projector'ом, читать оттуда бессмысленно — убрать
 * credential'ы напрямую по сигналу из этого события" (см. её javadoc
 * "КРИТИЧНО" за полным разбором находки).
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
 * <p><b>Коммит — только после успешного {@code refreshPartner}.</b>
 * Транзиентная ошибка Redis не должна тихо "проглотить" событие — офсет
 * не коммитится, тот же at-least-once принцип, что {@code billing-service
 * KafkaIo}/{@code config-cache-projector} уже применяют к своим потокам.
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
    private final BiConsumer<String, String> partnerRefresher;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private Thread thread;

    /**
     * @param partnerRefresher вызывается с {@code (entity_id, status)} каждого
     *                         {@code entity_type=PARTNER} события — в проде это
     *                         {@code PartnerConfigStore::refreshPartner} (см.
     *                         {@code Main.java}, обёрнутый там дополнительно
     *                         принудительным разъединением на неактивный
     *                         статус); принимает функциональный интерфейс, а
     *                         не конкретный {@code PartnerConfigStore}, чтобы
     *                         {@code processBatch}/{@code handle} были
     *                         тестируемы без реального Redis (см. {@code
     *                         ConfigChangeConsumerTest}).
     */
    public ConfigChangeConsumer(String bootstrapServers, String groupId, BiConsumer<String, String> partnerRefresher) {
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
    ConfigChangeConsumer(org.apache.kafka.clients.consumer.Consumer<String, byte[]> consumer, BiConsumer<String, String> partnerRefresher) {
        this.consumer = consumer;
        this.partnerRefresher = partnerRefresher;
    }

    public void start() {
        thread = new Thread(this::run, "partner-smpp-config-changes");
        thread.setDaemon(true);
        thread.start();
    }

    private void run() {
        consumer.subscribe(List.of(TOPIC));
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
            }
        } finally {
            consumer.close();
        }
    }

    /** Пакетная логика вынесена отдельно — тестируема без реального опроса Kafka (см. {@code ConfigChangeConsumerTest}). */
    void processBatch(ConsumerRecords<String, byte[]> records) {
        for (TopicPartition tp : records.partitions()) {
            long nextCommittable = -1;
            for (ConsumerRecord<String, byte[]> record : records.records(tp)) {
                try {
                    handle(record);
                    nextCommittable = record.offset() + 1;
                } catch (RuntimeException e) {
                    // handle() уже залогировал причину (partnerRefresher.accept
                    // или parse-ошибка) — здесь только прерываем эту партицию НА ЭТОМ
                    // поллинге, не коммитим текущую и последующие записи, следующий
                    // poll() передоставит их заново (at-least-once).
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

        LOG.info(() -> "config.changes: partner_id=" + partnerId + " status=" + event.getStatus()
            + " version=" + event.getVersion() + " — перечитываем SMPP-конфиг из Configuration Redis");
        partnerRefresher.accept(partnerId, event.getStatus());
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
