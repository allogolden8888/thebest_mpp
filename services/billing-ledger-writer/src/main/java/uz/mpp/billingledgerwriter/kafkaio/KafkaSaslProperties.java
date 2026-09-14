package uz.mpp.billingledgerwriter.kafkaio;

import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Map;
import java.util.Properties;

/**
 * SASL_SCRAM + TLS consumer properties — billing-ledger-writer — один из
 * трёх пилотных клиентов нового SASL_SCRAM+TLS листенера
 * (BACKOFFICE_ROADMAP.md P1 "Kafka — plaintext listener без SASL/ACL, хотя
 * HLD требует ACL"; порт 9094,
 * infra/kafka/generate_kafka_topics.py::build_kafka_cluster_crd). По одному
 * сервису на язык (Go/Rust/Java) —
 * k8s/generate_manifests.py::KAFKA_SASL_DEMO_SERVICES; остальные ~37
 * Kafka-клиентов платформы (включая billing-service и другие Java-сервисы)
 * не задают ни одну из этих переменных и продолжают работать через
 * KAFKA_BOOTSTRAP_SERVERS (порт 9092) без изменений.
 *
 * <p>Truststore не требует ручного разбора PEM: Kafka clients с версии 2.7
 * (KIP-651) поддерживают {@code ssl.truststore.type=PEM} c
 * {@code ssl.truststore.location}, указывающим прямо на файл сертификата
 * (тот же Strimzi cluster CA {@code ca.crt}, что рантайм читает у Go/Rust
 * пилотов) — не нужен JKS/PKCS12, конвертация не нужна.
 */
public final class KafkaSaslProperties {

    private KafkaSaslProperties() {}

    public static final String DEFAULT_MECHANISM = "SCRAM-SHA-512";

    /**
     * {@code null}, если хотя бы одна из обязательных переменных отсутствует —
     * вызывающая сторона тогда остаётся на plaintext bootstrap (тот же
     * fail-safe принцип, что {@code env()}-с-fallback в {@code Main}).
     */
    public static Properties fromEnv(Map<String, String> env) {
        String bootstrap = env.get("KAFKA_SASL_BOOTSTRAP_SERVERS");
        String username = env.get("KAFKA_SASL_USERNAME");
        String password = env.get("KAFKA_SASL_PASSWORD");
        String caPath = env.get("KAFKA_TLS_CA_PATH");
        if (isEmpty(bootstrap) || isEmpty(username) || isEmpty(password) || isEmpty(caPath)) {
            return null;
        }
        String mechanism = env.getOrDefault("KAFKA_SASL_MECHANISM", DEFAULT_MECHANISM);
        if (isEmpty(mechanism)) {
            mechanism = DEFAULT_MECHANISM;
        }

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", mechanism);
        props.put("sasl.jaas.config", jaasConfig(username, password));
        props.put("ssl.truststore.type", "PEM");
        props.put("ssl.truststore.location", caPath);
        return props;
    }

    static String jaasConfig(String username, String password) {
        return "org.apache.kafka.common.security.scram.ScramLoginModule required username=\""
            + escape(username) + "\" password=\"" + escape(password) + "\";";
    }

    private static String escape(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static boolean isEmpty(String value) {
        return value == null || value.isEmpty();
    }
}
