package uz.mpp.billingledgerwriter.kafkaio;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * BACKOFFICE_ROADMAP.md P1 "Kafka — plaintext listener без SASL/ACL, хотя HLD
 * требует ACL": billing-ledger-writer — один из трёх пилотных клиентов нового
 * SASL_SCRAM+TLS листенера. Эти тесты покрывают ТОЛЬКО построение
 * Properties (env parsing) — без живого брокера; live round-trip против
 * SASL-листенера этим не проверяется.
 */
class KafkaSaslPropertiesTest {

    private static Map<String, String> fullEnv() {
        Map<String, String> env = new HashMap<>();
        env.put("KAFKA_SASL_BOOTSTRAP_SERVERS", "sasl-broker:9094");
        env.put("KAFKA_SASL_USERNAME", "billing-ledger-writer-kafka-user");
        env.put("KAFKA_SASL_PASSWORD", "s3cr3t");
        env.put("KAFKA_TLS_CA_PATH", "/etc/mpp/kafka-tls/ca.crt");
        return env;
    }

    @Test
    void returnsNullWhenNoSaslVarsAreSet() {
        assertNull(KafkaSaslProperties.fromEnv(new HashMap<>()));
    }

    @Test
    void returnsNullWhenOnlySomeSaslVarsAreSet() {
        // Частичная конфигурация (например, опечатка в rollout, потерявшая одну
        // переменную) не должна тихо наполовину аутентифицироваться.
        Map<String, String> env = new HashMap<>();
        env.put("KAFKA_SASL_BOOTSTRAP_SERVERS", "sasl-broker:9094");
        env.put("KAFKA_SASL_USERNAME", "billing-ledger-writer-kafka-user");
        // password и CA path намеренно не заданы.

        assertNull(KafkaSaslProperties.fromEnv(env));
    }

    @Test
    void buildsSaslSslPropertiesWhenAllVarsArePresent() {
        Properties props = KafkaSaslProperties.fromEnv(fullEnv());

        assertEquals("sasl-broker:9094", props.get(org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals("SASL_SSL", props.get("security.protocol"));
        assertEquals("SCRAM-SHA-512", props.get("sasl.mechanism"));
        assertEquals("PEM", props.get("ssl.truststore.type"));
        assertEquals("/etc/mpp/kafka-tls/ca.crt", props.get("ssl.truststore.location"));
        assertEquals(
            "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"billing-ledger-writer-kafka-user\" password=\"s3cr3t\";",
            props.get("sasl.jaas.config"));
    }

    @Test
    void explicitMechanismOverridesDefault() {
        Map<String, String> env = fullEnv();
        env.put("KAFKA_SASL_MECHANISM", "SCRAM-SHA-256");

        Properties props = KafkaSaslProperties.fromEnv(env);

        assertEquals("SCRAM-SHA-256", props.get("sasl.mechanism"));
    }

    @Test
    void jaasConfigEscapesQuotesAndBackslashesInCredentials() {
        String jaas = KafkaSaslProperties.jaasConfig("us\"er", "pa\\ss");
        assertEquals(
            "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"us\\\"er\" password=\"pa\\\\ss\";",
            jaas);
    }
}
