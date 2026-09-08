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
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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

    /**
     * Окно реконсиляции — сколько ждать позднее свидетельство (submit_sm_resp,
     * DLR, query_sm), прежде чем закрывать case по дедлайну. Значение по
     * умолчанию (30 минут) не изменено; вынесено в env по той же причине, что
     * и параметры sweep'а — это ГЛАВНЫЙ множитель удержания памяти платформы,
     * а не косметика: до закрытия case'а pipeline-engine не зовёт
     * finalize_pipeline, и ключи exec:/msgctx: сообщения (+4 на сообщение)
     * живут в Runtime Redis. При 300 сообщ/с окно в 30 минут само по себе
     * держит ~540 000 сообщений «в полёте» даже при идеально успевающем
     * sweep'е. Sweep, который догоняет поток, убирает НЕОГРАНИЧЕННЫЙ рост
     * сверх этого; сам пол задаёт окно, и подбирать его теперь можно без
     * пересборки.
     */
    private static final Duration RECONCILIATION_WINDOW =
        Duration.ofMinutes(Long.parseLong(env("RECONCILIATION_WINDOW_MINUTES", "30")));

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

        // По отдельному JDBC-соединению на поток. java.sql.Connection у
        // pgJDBC — это ОДИН защищённый локом канал к бэкенду: пока по нему
        // идёт запрос, все прочие потоки блокируются на нём. Раньше все
        // четыре потока сервиса (три consumer'а + sweep) делили одно
        // соединение, а нагрузка на него несимметричная: при 300 сообщ/с
        // consumer'ы дают ~2700 запросов/с (loadByMessageId/create/
        // persistEvidence на каждое событие трёх топиков), и sweep стоял в
        // общей очереди за ними на каждый closeCase. Соединение на поток
        // убирает это внутрисервисное сериализующее звено; JVM-локи
        // (см. {@link #caseLocks}) остаются единственным механизмом
        // сериализации read-modify-write, они от соединения не зависели.
        List<Connection> connections = new ArrayList<>();
        ReconciliationStore sweepStore = openStore(connections);
        ReconciliationStore executeStore = openStore(connections);
        ReconciliationStore submitAcceptedStore = openStore(connections);
        ReconciliationStore deliveryStatusStore = openStore(connections);

        StageCompletedPublisher publisher = new StageCompletedPublisher(env("KAFKA_BOOTSTRAP_SERVERS", "kafka-bootstrap.mpp.svc:9092"));

        // Оба параметра — env, чтобы подбирать под конкретный стенд без
        // пересборки (принятый в репозитории приём, ср. OUTBOX_NUM_SHARDS в
        // billing-outbox-publisher, MAX_CONCURRENT_SUBMITS в
        // operator-smpp-session-manager).
        //
        // Интервал: было 30с. При 300 сообщ/с за один такой промежуток
        // накапливается ~9000 case'ов, и вся работа шла рывком — сервис
        // 30 секунд простаивал, потом пытался догнать, а всё это время
        // pipeline-engine не звал finalize_pipeline и ключи exec:/msgctx:
        // висели в Runtime Redis (+4 ключа на сообщение). 2с — тот же объём
        // работы, размазанный ровно: ~600 case'ов на прогон, фиксированные
        // издержки прогона (один SELECT + одно ожидание подтверждений + 1-2
        // UPDATE) при этом полностью амортизируются, а задержка закрытия
        // case'а после дедлайна падает с "до 30с" до "до 2с".
        //
        // scheduleWithFixedDelay, а не scheduleAtFixedRate: если прогон
        // затянулся (догон бэклога), нам нужен интервал ПОСЛЕ его окончания,
        // а не пачка немедленно накопившихся запусков подряд.
        long sweepIntervalMs = Long.parseLong(env("RECONCILIATION_SWEEP_INTERVAL_MS", "2000"));
        int sweepBatchSize = Integer.parseInt(env("RECONCILIATION_SWEEP_BATCH_SIZE", "2000"));

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleWithFixedDelay(() -> sweepDeadlines(sweepStore, publisher, sweepBatchSize),
            sweepIntervalMs, sweepIntervalMs, TimeUnit.MILLISECONDS);

        Thread executeConsumerThread = new Thread(() -> runExecuteConsumer(executeStore), "reconciliation-execute-consumer");
        executeConsumerThread.setDaemon(true);
        executeConsumerThread.start();

        // CODE_REVIEW.md #11 — collect_evidence: до этой находки эти два
        // топика не консюмились нигде, поэтому sweepDeadlines всегда резолвил
        // Evidence.empty() (см. докстринг sweepDeadlines).
        Thread submitAcceptedConsumerThread = new Thread(() -> runSubmitAcceptedConsumer(submitAcceptedStore), "reconciliation-submit-accepted-consumer");
        submitAcceptedConsumerThread.setDaemon(true);
        submitAcceptedConsumerThread.start();

        Thread deliveryStatusConsumerThread = new Thread(() -> runDeliveryStatusConsumer(deliveryStatusStore), "reconciliation-delivery-status-consumer");
        deliveryStatusConsumerThread.setDaemon(true);
        deliveryStatusConsumerThread.start();

        health.setReady(true);
        System.out.println("delivery-reconciliation-service готов (sweep: интервал " + sweepIntervalMs + "мс, батч " + sweepBatchSize + ")");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            scheduler.shutdown();
            publisher.close();
            for (Connection c : connections) {
                try {
                    c.close();
                } catch (SQLException e) {
                    System.err.println("close JDBC connection failed: " + e.getMessage());
                }
            }
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

    /**
     * evaluate_deadline + resolve_outcome (по накопленному Evidence —
     * CODE_REVIEW.md #11) + publish_stage_completed + persist_case, по таймеру.
     *
     * <p>Один прогон выгребает бэклог чанками по {@code batchSize}, пока чанки
     * не кончатся, а не одним неограниченным SELECT'ом: так пиковое
     * потребление памяти не зависит от размера бэклога, но догон при этом не
     * растягивается на много тактов таймера.
     */
    private static void sweepDeadlines(ReconciliationStore store, StageCompletedPublisher publisher, int batchSize) {
        try {
            while (true) {
                List<ReconciliationCase> expiredCases = store.findExpiredOpenCases(Instant.now(), batchSize);
                if (expiredCases.isEmpty()) {
                    return;
                }
                int closed = sweepBatch(store, publisher, expiredCases);
                // closed == 0 — ни один case чанка закрыть не удалось (Kafka
                // недоступна, либо resolve_outcome вернул null). Следующий
                // SELECT вернул бы ровно тот же чанк: без этой проверки
                // прогон крутился бы в бесконечном цикле. Прерываемся и ждём
                // следующего такта таймера.
                if (closed == 0 || expiredCases.size() < batchSize) {
                    return;
                }
                if (Thread.currentThread().isInterrupted()) {
                    return;
                }
            }
        } catch (Exception e) {
            System.err.println("sweep_deadlines failed: " + e);
        }
    }

    /** Один case, отправленный в Kafka и ждущий подтверждения. */
    private record InFlight(UUID caseId, String finalStatus, Future<?> ack) {
    }

    /**
     * Один чанк sweep'а. Раньше на КАЖДЫЙ case делалось два последовательных
     * блокирующих round-trip'а — {@code publisher.publish(...).get()} и
     * {@code store.closeCase(...)}. На тесной машине, где park+wakeup стоит
     * ~25мс, это ~50мс на case, т.е. потолок ~20 case/с при входящем потоке
     * 300/с (замерено на живой системе: ~45 событий DELIVERY_RECONCILIATION в
     * секунду против 300 входящих — отставание в 6-7 раз). Отставание не
     * косметическое: пока case не закрыт, pipeline-engine не получает
     * stage.completed, не зовёт finalize_pipeline, и ключи exec:/msgctx:
     * (+4 на сообщение) продолжают копиться в Runtime Redis, выедая память
     * всей платформы.
     *
     * <p>Здесь те же самые ожидания сложены: сначала отправляются ВСЕ записи
     * чанка (KafkaProducer асинхронный, linger.ms=5 их ещё и упакует), потом
     * один раз ждём подтверждений, потом закрываем подтверждённые одним-двумя
     * UPDATE'ами. Вместо N последовательных ожиданий — одно; ни одного
     * дополнительного потока не добавлено.
     *
     * <p><b>Гарантия CODE_REVIEW.md #12 сохранена дословно</b>: закрываются
     * ТОЛЬКО те case'ы, для которых {@code Future.get()} вернулся без
     * исключения. Любой сбой публикации (или прерывание ожидания) оставляет
     * свой case в статусе "open" — его подберёт следующий прогон, т.к.
     * deadline_at уже в прошлом. Батчинг здесь ничего не ослабляет: сбой
     * одной записи не тянет за собой соседей по чанку, потому что решение
     * принимается по каждому Future отдельно.
     *
     * @return сколько case'ов реально закрыто
     */
    private static int sweepBatch(ReconciliationStore store, StageCompletedPublisher publisher, List<ReconciliationCase> batch) {
        List<InFlight> inFlight = new ArrayList<>(batch.size());
        int sendFailures = 0;
        String lastSendError = null;

        for (ReconciliationCase c : batch) {
            OutcomeResolver.DeadlineState deadlineState = DeadlineEvaluator.evaluate(Instant.now(), c.deadlineAt());
            Evidence evidence = EvidenceCodec.decode(c.evidenceJson());
            ReconciliationOutcome outcome = OutcomeResolver.resolve(evidence, deadlineState);
            if (outcome == null) {
                continue;
            }
            String finalStatus = outcome == ReconciliationOutcome.RECONCILIATION_OUTCOME_DELIVERY_UNRESOLVED ? "unresolved" : "resolved";
            try {
                // send() не блокирует в ожидании брокера (кроме случая
                // переполненного буфера — max.block.ms=10с), поэтому весь
                // чанк уходит без ожиданий между записями.
                Future<?> ack = publisher.publish(StageCompletedBuilder.build(c.messageId(), c.stageExecutionId(), 1, outcome, Instant.now()));
                inFlight.add(new InFlight(c.caseId(), finalStatus, ack));
            } catch (Exception e) {
                sendFailures++;
                lastSendError = e.getMessage();
            }
        }

        // Одно ожидание на весь чанк вместо N последовательных: к моменту,
        // когда дойдём до последнего Future, первые давно подтверждены.
        List<UUID> resolvedIds = new ArrayList<>();
        List<UUID> unresolvedIds = new ArrayList<>();
        int ackFailures = 0;
        String lastAckError = null;
        for (InFlight f : inFlight) {
            try {
                f.ack().get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                // Прерывание — не повод терять уже подтверждённые case'ы:
                // выходим из ожидания, но закрываем то, что подтверждено.
                break;
            } catch (Exception e) {
                ackFailures++;
                lastAckError = e.getMessage();
                continue;
            }
            if ("unresolved".equals(f.finalStatus())) {
                unresolvedIds.add(f.caseId());
            } else {
                resolvedIds.add(f.caseId());
            }
        }

        Instant resolvedAt = Instant.now();
        store.closeCases(resolvedIds, "resolved", resolvedAt);
        store.closeCases(unresolvedIds, "unresolved", resolvedAt);
        for (UUID caseId : resolvedIds) {
            caseLocks.remove(caseId);
        }
        for (UUID caseId : unresolvedIds) {
            caseLocks.remove(caseId);
        }

        // Агрегированный лог: при бэклоге в тысячи case'ов строка на каждый
        // сбой сама по себе стала бы источником нагрузки.
        if (sendFailures > 0 || ackFailures > 0) {
            System.err.println("publish_stage_completed failed для " + (sendFailures + ackFailures)
                + " case(ов) из " + batch.size() + ", они остаются open для повторной попытки; последняя ошибка: "
                + (lastAckError != null ? lastAckError : lastSendError));
        }
        return resolvedIds.size() + unresolvedIds.size();
    }

    private static Properties consumerProps(String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, env("KAFKA_BOOTSTRAP_SERVERS", "kafka-bootstrap.mpp.svc:9092"));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        return props;
    }

    /** Отдельное JDBC-соединение (см. комментарий в {@link #main}); все они закрываются в shutdown hook. */
    private static ReconciliationStore openStore(List<Connection> registry) throws SQLException {
        Connection connection = DriverManager.getConnection(buildJdbcUrl());
        registry.add(connection);
        DSLContext dsl = DSL.using(connection, SQLDialect.POSTGRES);
        return new ReconciliationStore(dsl);
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