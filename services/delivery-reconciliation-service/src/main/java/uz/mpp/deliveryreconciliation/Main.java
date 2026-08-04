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
import uz.mpp.deliveryreconciliation.core.EvidenceCodec;
import uz.mpp.deliveryreconciliation.core.OutcomeResolver;
import uz.mpp.deliveryreconciliation.health.HealthServer;
import uz.mpp.deliveryreconciliation.kafkaio.StageCompletedBuilder;
import uz.mpp.deliveryreconciliation.kafkaio.StageCompletedPublisher;
import uz.mpp.deliveryreconciliation.store.ReconciliationCase;
import uz.mpp.deliveryreconciliation.store.ReconciliationStore;
import uz.mpp.platformcontracts.common.v1.ReconciliationOutcome;
import uz.mpp.platformcontracts.common.v1.StageExecuteCommand;
import uz.mpp.platformcontracts.events.v1.DeliveryStatusEvent;
import uz.mpp.platformcontracts.events.v1.OperatorSubmitAccepted;

import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;

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

    /**
     * CODE_REVIEW.md #11 — per-case lock для read-modify-write поверх
     * evidence JSONB. {@code operator.submit.accepted} и {@code delivery.status}
     * консюмятся в двух независимых потоках и оба вызывают
     * {@link #mergeEvidence}; без сериализации внутри одного JVM конкурентный
     * read-modify-write мог бы потерять одно из двух обновлений (classic
     * lost-update). Это НЕ защищает от гонки между несколькими репликами
     * сервиса (нет CAS/optimistic-lock на уровне БД) — задокументировано в
     * README как остаточный, менее вероятный риск (тот же класс проблемы, что
     * billing-reconciliation #8, вне текущего прохода по этому сервису).
     */
    private static final ConcurrentHashMap<UUID, Object> caseLocks = new ConcurrentHashMap<>();

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

        // CODE_REVIEW.md #11 — collect_evidence: до этой находки эти два
        // топика не консюмились нигде, поэтому sweepDeadlines всегда резолвил
        // Evidence.empty() (см. докстринг sweepDeadlines).
        Thread submitAcceptedConsumerThread = new Thread(() -> runSubmitAcceptedConsumer(store), "reconciliation-submit-accepted-consumer");
        submitAcceptedConsumerThread.setDaemon(true);
        submitAcceptedConsumerThread.start();

        Thread deliveryStatusConsumerThread = new Thread(() -> runDeliveryStatusConsumer(store), "reconciliation-delivery-status-consumer");
        deliveryStatusConsumerThread.setDaemon(true);
        deliveryStatusConsumerThread.start();

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
        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerProps("delivery-reconciliation-service"))) {
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

    /**
     * CODE_REVIEW.md #11 — collect_evidence, ветка {@code operator.submit.accepted}.
     * Позднее подтверждение, что оператор принял submit — единственный
     * положительный сигнал, если DLR/query_sm так и не пришли до дедлайна
     * (см. {@code OutcomeResolver.resolve}, ветка
     * {@code RECONCILIATION_OUTCOME_CONFIRMED_SUBMITTED}).
     */
    private static void runSubmitAcceptedConsumer(ReconciliationStore store) {
        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerProps("delivery-reconciliation-service-submit-accepted"))) {
            consumer.subscribe(Collections.singletonList("operator.submit.accepted"));
            while (true) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofSeconds(1));
                for (var record : records) {
                    try {
                        OperatorSubmitAccepted event = OperatorSubmitAccepted.parseFrom(record.value());
                        UUID messageId = UUID.fromString(event.getMessageId());
                        mergeEvidence(store, messageId, Evidence::withSubmitAccepted);
                    } catch (Exception e) {
                        System.err.println("collect_evidence (operator.submit.accepted) failed: " + e.getMessage());
                    }
                }
            }
        }
    }

    /**
     * CODE_REVIEW.md #11 — collect_evidence, ветка {@code delivery.status}.
     * DLR — самое сильное свидетельство в {@code OutcomeResolver.resolve}
     * (побеждает независимо от прочего), поэтому это — самое важное из двух
     * новых consumer'ов с точки зрения корректности итогового исхода.
     */
    private static void runDeliveryStatusConsumer(ReconciliationStore store) {
        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerProps("delivery-reconciliation-service-delivery-status"))) {
            consumer.subscribe(Collections.singletonList("delivery.status"));
            while (true) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofSeconds(1));
                for (var record : records) {
                    try {
                        DeliveryStatusEvent event = DeliveryStatusEvent.parseFrom(record.value());
                        UUID messageId = UUID.fromString(event.getMessageId());
                        Evidence.DeliveryOutcome outcome = mapNormalizedStatus(event.getNormalizedStatus());
                        if (outcome != null) {
                            mergeEvidence(store, messageId, e -> e.withDeliveryStatus(outcome));
                        }
                    } catch (Exception e) {
                        System.err.println("collect_evidence (delivery.status) failed: " + e.getMessage());
                    }
                }
            }
        }
    }

    /**
     * normalize_operator_status словарь (services/dlr-manager/internal/dlr/parser.go
     * {@code normalizedStatusByRawSMPPStat}) публикует ровно "DELIVERED" |
     * "UNDELIVERABLE" в {@code delivery.status}; всё прочее (в т.ч.
     * нераспознанные/не-терминальные коды) не даёт основания менять
     * {@code Evidence.deliveryStatusObserved} — {@code null} здесь означает
     * "это событие не несёт нового свидетельства", вызывающая сторона его
     * пропускает, не мутирует Evidence.
     */
    private static Evidence.DeliveryOutcome mapNormalizedStatus(String normalizedStatus) {
        return switch (normalizedStatus) {
            case "DELIVERED" -> Evidence.DeliveryOutcome.SUCCESS;
            case "UNDELIVERABLE" -> Evidence.DeliveryOutcome.FAILURE;
            default -> null;
        };
    }

    /**
     * CODE_REVIEW.md #11 — read-modify-write поверх evidence JSONB конкретного
     * case'а, с блокировкой per-caseId (см. докстринг {@link #caseLocks}).
     * Case, закрытый до прихода этого evidence (поздний DLR/submit_accepted
     * после дедлайна), — не ошибка: получателя для evidence уже нет, тихо
     * игнорируем (тот же паттерн, что at-least-once-редоставка везде в этой
     * сессии — не каждое сообщение обязано на что-то повлиять).
     */
    private static void mergeEvidence(ReconciliationStore store, UUID messageId, UnaryOperator<Evidence> mutator) {
        store.loadByMessageId(messageId).ifPresent(initial -> {
            if (!"open".equals(initial.status())) {
                return;
            }
            Object lock = caseLocks.computeIfAbsent(initial.caseId(), id -> new Object());
            synchronized (lock) {
                ReconciliationCase fresh = store.loadByMessageId(messageId).orElse(initial);
                if (!"open".equals(fresh.status())) {
                    return;
                }
                Evidence current = EvidenceCodec.decode(fresh.evidenceJson());
                Evidence updated = mutator.apply(current);
                store.persistEvidence(fresh.caseId(), EvidenceCodec.encode(updated));
            }
        });
    }

    /** evaluate_deadline + resolve_outcome (по накопленному Evidence — CODE_REVIEW.md #11) + publish_stage_completed + persist_case, по таймеру. */
    private static void sweepDeadlines(ReconciliationStore store, StageCompletedPublisher publisher) {
        List<ReconciliationCase> expiredCases = store.findExpiredOpenCases(Instant.now());

        for (ReconciliationCase c : expiredCases) {
            OutcomeResolver.DeadlineState deadlineState = DeadlineEvaluator.evaluate(Instant.now(), c.deadlineAt());
            Evidence evidence = EvidenceCodec.decode(c.evidenceJson());
            ReconciliationOutcome outcome = OutcomeResolver.resolve(evidence, deadlineState);
            if (outcome == null) {
                continue;
            }
            String finalStatus = outcome == ReconciliationOutcome.RECONCILIATION_OUTCOME_DELIVERY_UNRESOLVED ? "unresolved" : "resolved";

            // CODE_REVIEW.md #12 — публикуем ДО close_case и ждём подтверждения
            // через Future.get() (тот же паттерн, что уже использует
            // billing-outbox-publisher). Раньше closeCase шёл первым и Future
            // не проверялся: неудачный publish навсегда оставлял case
            // "closed", но никогда не опубликованным в stage.completed. Теперь
            // при сбое публикации case остаётся "open" и будет подобран
            // повторно следующим прогоном sweepDeadlines (findExpiredOpenCases
            // снова его вернёт, т.к. deadline_at уже в прошлом) — идемпотентно,
            // т.к. persist_case для незакрытого case'а не имеет побочных
            // эффектов, требующих отмены.
            try {
                publisher.publish(StageCompletedBuilder.build(c.messageId(), c.stageExecutionId(), 1, outcome, Instant.now())).get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                continue;
            } catch (Exception e) {
                System.err.println("publish_stage_completed failed for case " + c.caseId() + ", case остаётся open для повторной попытки: " + e.getMessage());
                continue;
            }

            store.closeCase(c.caseId(), finalStatus, Instant.now());
            caseLocks.remove(c.caseId());
        }
    }

    private static Properties consumerProps(String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, env("KAFKA_BOOTSTRAP_SERVERS", "kafka-bootstrap.mpp.svc:9092"));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        return props;
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