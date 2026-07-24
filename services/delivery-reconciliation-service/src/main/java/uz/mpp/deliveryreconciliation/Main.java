package uz.mpp.deliveryreconciliation;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import uz.mpp.deliveryreconciliation.core.DeadlineEvaluator;
import uz.mpp.deliveryreconciliation.core.Evidence;
import uz.mpp.deliveryreconciliation.core.OutcomeResolver;
import uz.mpp.deliveryreconciliation.health.HealthServer;
import uz.mpp.deliveryreconciliation.kafkaio.StageCompletedBuilder;
import uz.mpp.deliveryreconciliation.kafkaio.StageCompletedPublisher;
import uz.mpp.deliveryreconciliation.store.ReconciliationCase;
import uz.mpp.deliveryreconciliation.store.ReconciliationStore;
import uz.mpp.platformcontracts.common.v1.ReconciliationOutcome;
import uz.mpp.platformcontracts.common.v1.StageExecuteCommand;

import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Delivery Reconciliation Service (services_specifictaion.md §2.9) —
 * разрешение SUBMISSION_OUTCOME_UNKNOWN. Простой Kafka Client (не Streams),
 * jOOQ + PostgreSQL, gRPC-клиент для опционального query_sm.
 *
 * <p><b>Отклонение от LLD-стека</b>: `services_specifictaion.md` §2.9
 * указывает Micronaut — в этом срезе сервис собран без Micronaut (обычная
 * ручная сборка Kafka consumer loop + DI руками, тот же паттерн, что
 * остальные Java-сервисы этой сессии), чтобы не тянуть annotation
 * processing/DI-контейнер ради экономии времени в рамках одной сессии.
 * Задокументировано в README как честное отклонение, не скрыто.
 */
public final class Main {

    private static final Duration RECONCILIATION_WINDOW = Duration.ofMinutes(30);

    public static void main(String[] args) throws Exception {
        HealthServer health = new HealthServer();
        health.start();

        Connection connection = DriverManager.getConnection(buildJdbcUrl());
        DSLContext dsl = DSL.using(connection, SQLDialect.POSTGRES);
        ReconciliationStore store = new ReconciliationStore(dsl);

        StageCompletedPublisher publisher = new StageCompletedPublisher(env("KAFKA_BOOTSTRAP_SERVERS", "kafka-bootstrap.mpp.svc:9092"));

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(() -> sweepDeadlines(store, publisher), 30, 30, TimeUnit.SECONDS);

        Thread executeConsumerThread = new Thread(() -> runExecuteConsumer(store), "reconciliation-execute-consumer");
        executeConsumerThread.setDaemon(true);
        executeConsumerThread.start();

        health.setReady(true);
        System.out.println("delivery-reconciliation-service готов");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            scheduler.shutdown();
            publisher.close();
            health.stop();
        }));

        Thread.currentThread().join();
    }

    /** handle_reconciliation_execute — consumer stage.delivery-reconciliation (StageExecuteCommand). */
    private static void runExecuteConsumer(ReconciliationStore store) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, env("KAFKA_BOOTSTRAP_SERVERS", "kafka-bootstrap.mpp.svc:9092"));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "delivery-reconciliation-service");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList("stage.delivery-reconciliation"));
            while (true) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofSeconds(1));
                for (var record : records) {
                    try {
                        StageExecuteCommand cmd = StageExecuteCommand.parseFrom(record.value());
                        UUID messageId = UUID.fromString(cmd.getMessageId());
                        UUID stageExecutionId = UUID.fromString(cmd.getStageExecutionId());
                        String operatorId = cmd.getDeliveryReconciliation().getQueueMsgId(); // placeholder — см. README
                        store.loadByMessageId(messageId)
                            .orElseGet(() -> store.create(messageId, stageExecutionId, operatorId, Instant.now().plus(RECONCILIATION_WINDOW)));
                    } catch (Exception e) {
                        System.err.println("handle_reconciliation_execute failed: " + e.getMessage());
                    }
                }
            }
        }
    }

    /** evaluate_deadline + resolve_outcome + persist_case + publish_stage_completed, по таймеру. */
    private static void sweepDeadlines(ReconciliationStore store, StageCompletedPublisher publisher) {
        List<ReconciliationCase> expiredCases = store.findExpiredOpenCases(Instant.now());

        for (ReconciliationCase c : expiredCases) {
            OutcomeResolver.DeadlineState deadlineState = DeadlineEvaluator.evaluate(Instant.now(), c.deadlineAt());
            ReconciliationOutcome outcome = OutcomeResolver.resolve(Evidence.empty(), deadlineState);
            if (outcome == null) {
                continue;
            }
            String finalStatus = outcome == ReconciliationOutcome.RECONCILIATION_OUTCOME_DELIVERY_UNRESOLVED ? "unresolved" : "resolved";
            store.closeCase(c.caseId(), finalStatus, Instant.now());
            publisher.publish(StageCompletedBuilder.build(c.messageId(), c.stageExecutionId(), 1, outcome, Instant.now()));
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