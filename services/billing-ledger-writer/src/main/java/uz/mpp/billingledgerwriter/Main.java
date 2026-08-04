package uz.mpp.billingledgerwriter;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import uz.mpp.billingledgerwriter.health.HealthServer;
import uz.mpp.billingledgerwriter.kafkaio.LedgerEntryMapper;
import uz.mpp.billingledgerwriter.store.LedgerStore;
import uz.mpp.billingledgerwriter.store.ReconnectingConnectionProvider;
import uz.mpp.platformcontracts.events.v1.LedgerEvent;

import java.util.Collections;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Billing Ledger Writer (services_specifictaion.md §6.2): billing.ledger →
 * PostgreSQL double-entry ledger.
 */
public final class Main {

    public static void main(String[] args) throws Exception {
        HealthServer health = new HealthServer();
        health.start();

        ReconnectingConnectionProvider connectionProvider = new ReconnectingConnectionProvider(buildJdbcUrl());
        DSLContext dsl = DSL.using(connectionProvider, SQLDialect.POSTGRES);
        LedgerStore store = new LedgerStore(dsl);

        health.setReady(true);
        System.out.println("billing-ledger-writer готов");

        // CODE_REVIEW.md Critical #3: ENABLE_AUTO_COMMIT_CONFIG раньше не
        // выставлялся вообще (дефолт — true, 5с интервал) — offset
        // коммитился независимо от того, реально ли запись дошла до
        // Postgres. Теперь коммит только за успешно обработанные записи, тем
        // же паттерном (OffsetTracker, per-partition, suspend-on-failure),
        // что уже применён в delivery-service/message-state-resolver этой
        // же сессии.
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, env("KAFKA_BOOTSTRAP_SERVERS", "kafka-bootstrap.mpp.svc:9092"));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "billing-ledger-writer");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            health.stop();
            connectionProvider.close();
        }));

        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList("billing.ledger"));
            while (true) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofSeconds(1));

                OffsetTracker tracker = new OffsetTracker();
                for (ConsumerRecord<String, byte[]> record : records) {
                    TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                    if (tracker.isSuspended(tp)) {
                        continue;
                    }
                    try {
                        LedgerEvent event = LedgerEvent.parseFrom(record.value());
                        boolean inserted = store.insert(LedgerEntryMapper.fromProto(event));
                        if (!inserted) {
                            System.out.println("insert_double_entry: charge_id=" + event.getChargeId() + " уже существует (idempotent replay)");
                        }
                        tracker.recordSuccess(tp, record.offset());
                        health.setReady(true);
                    } catch (Exception e) {
                        // CODE_REVIEW.md Critical #3: раньше — только
                        // System.err.println и продолжить, с автокоммитом
                        // это тихо теряло запись навсегда. Теперь: offset НЕ
                        // коммитится, партиция приостанавливается до
                        // следующего poll (та же запись будет
                        // передоставлена — insert идемпотентен по charge_id,
                        // повтор безопасен), и /readyz отражает деградацию —
                        // Kubernetes получает реальный сигнал, а не
                        // "успешно" работающий, но тихо теряющий данные под.
                        System.err.println("on_ledger_event failed partition=" + tp + " offset=" + record.offset()
                            + ", оффсет не коммитится, партиция приостановлена до следующего поллинга: " + e.getMessage());
                        tracker.recordFailure(tp);
                        health.setReady(false);
                    }
                }

                Map<TopicPartition, Long> committable = tracker.committableOffsets();
                if (!committable.isEmpty()) {
                    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                    committable.forEach((tp, offset) -> offsets.put(tp, new OffsetAndMetadata(offset)));
                    consumer.commitSync(offsets);
                }
            }
        }
    }

    /**
     * Тот же паттерн, что {@code delivery-service/KafkaIo.OffsetTracker} —
     * commitAsync()/commitSync() без явных offset'ов коммитит позицию ВСЕГО
     * фетча, не оффсет только что обработанной записи; при частичном сбое
     * батча это молча продвигает коммит МИМО непрочитанной/неуспешной
     * записи. Здесь коммитится строго "offset последней успешной записи + 1"
     * на партицию, партиция с ошибкой приостанавливается до конца текущего
     * батча целиком (не только до следующей записи) — не теряет порядок
     * внутри партиции.
     */
    static final class OffsetTracker {
        private final Map<TopicPartition, Long> committable = new HashMap<>();
        private final Set<TopicPartition> suspended = new HashSet<>();

        boolean isSuspended(TopicPartition tp) {
            return suspended.contains(tp);
        }

        void recordSuccess(TopicPartition tp, long offset) {
            committable.put(tp, offset + 1);
        }

        void recordFailure(TopicPartition tp) {
            suspended.add(tp);
        }

        Map<TopicPartition, Long> committableOffsets() {
            return committable;
        }
    }

    private static String buildJdbcUrl() {
        String host = env("POSTGRES_HOST", "localhost");
        String port = env("POSTGRES_PORT", "5432");
        String db = env("POSTGRES_DB", "mpp");
        String user = env("POSTGRES_USER", "");
        String password = env("POSTGRES_PASSWORD", "");
        if (user.isEmpty()) {
            return "jdbc:postgresql://" + host + ":" + port + "/" + db;
        }
        return "jdbc:postgresql://" + host + ":" + port + "/" + db + "?user=" + user + "&password=" + password;
    }

    private static String env(String key, String fallback) {
        String v = System.getenv(key);
        return (v == null || v.isEmpty()) ? fallback : v;
    }
}
