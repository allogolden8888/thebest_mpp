package uz.mpp.billingledgerwriter;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import uz.mpp.billingledgerwriter.health.HealthServer;
import uz.mpp.billingledgerwriter.kafkaio.LedgerEntryMapper;
import uz.mpp.billingledgerwriter.store.LedgerStore;
import uz.mpp.platformcontracts.events.v1.LedgerEvent;

import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * Billing Ledger Writer (services_specifictaion.md §6.2): billing.ledger →
 * PostgreSQL double-entry ledger.
 */
public final class Main {

    public static void main(String[] args) throws Exception {
        HealthServer health = new HealthServer();
        health.start();

        Connection connection = DriverManager.getConnection(buildJdbcUrl());
        DSLContext dsl = DSL.using(connection, SQLDialect.POSTGRES);
        LedgerStore store = new LedgerStore(dsl);

        health.setReady(true);
        System.out.println("billing-ledger-writer готов");

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, env("KAFKA_BOOTSTRAP_SERVERS", "kafka-bootstrap.mpp.svc:9092"));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "billing-ledger-writer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        Runtime.getRuntime().addShutdownHook(new Thread(health::stop));

        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList("billing.ledger"));
            while (true) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofSeconds(1));
                for (var record : records) {
                    try {
                        LedgerEvent event = LedgerEvent.parseFrom(record.value());
                        boolean inserted = store.insert(LedgerEntryMapper.fromProto(event));
                        if (!inserted) {
                            System.out.println("insert_double_entry: charge_id=" + event.getChargeId() + " уже существует (idempotent replay)");
                        }
                    } catch (Exception e) {
                        System.err.println("on_ledger_event failed: " + e.getMessage());
                    }
                }
            }
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