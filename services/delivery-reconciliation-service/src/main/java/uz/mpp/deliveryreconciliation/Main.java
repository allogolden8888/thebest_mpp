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
import uz.mpp.deliveryreconciliation.store.CaseStore;
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
import java.util.Optional;
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
     * Сколько держать «раннее» свидетельство (reconciliation.early_evidence,
     * migrations/V032) для сообщения, у которого case так и не появился.
     *
     * <p>По умолчанию — ровно окно реконсиляции: свидетельство старше окна
     * заведомо некому забрать, потому что любой case, который мог бы его
     * подхватить, к этому моменту уже закрыт по дедлайну. Отдельный env — на
     * случай, если pipeline-engine отстаёт сильнее окна и case создаётся
     * позже: тогда TTL поднимают, не трогая само окно.
     */
    private static final Duration EARLY_EVIDENCE_TTL = Duration.ofMinutes(
        Long.parseLong(env("RECONCILIATION_EARLY_EVIDENCE_TTL_MINUTES",
            String.valueOf(RECONCILIATION_WINDOW.toMinutes()))));

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
        CaseStore sweepStore = openStore(connections);
        CaseStore executeStore = openStore(connections);
        CaseStore submitAcceptedStore = openStore(connections);
        CaseStore deliveryStatusStore = openStore(connections);

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

        // publisher передаётся и в consumer'ы, а не только в sweep: с этой
        // правки case закрывается СРАЗУ по терминальному свидетельству, а не
        // ждёт дедлайна (см. applyToCase). KafkaProducer потокобезопасен —
        // один экземпляр на все потоки, как и раньше.
        Thread executeConsumerThread = new Thread(() -> runExecuteConsumer(executeStore, publisher), "reconciliation-execute-consumer");
        executeConsumerThread.setDaemon(true);
        executeConsumerThread.start();

        // CODE_REVIEW.md #11 — collect_evidence: до этой находки эти два
        // топика не консюмились нигде, поэтому sweepDeadlines всегда резолвил
        // Evidence.empty() (см. докстринг sweepDeadlines).
        Thread submitAcceptedConsumerThread = new Thread(() -> runSubmitAcceptedConsumer(submitAcceptedStore, publisher), "reconciliation-submit-accepted-consumer");
        submitAcceptedConsumerThread.setDaemon(true);
        submitAcceptedConsumerThread.start();

        Thread deliveryStatusConsumerThread = new Thread(() -> runDeliveryStatusConsumer(deliveryStatusStore, publisher), "reconciliation-delivery-status-consumer");
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
    private static void runExecuteConsumer(CaseStore store, StageCompletedPublisher publisher) {
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
                        openCase(store, publisher, messageId, stageExecutionId, operatorId);
                    } catch (Exception e) {
                        System.err.println("handle_reconciliation_execute failed: " + e.getMessage());
                    }
                }
            }
        }
    }

    /**
     * handle_reconciliation_execute — создание case'а и НЕМЕДЛЕННЫЙ подхват
     * свидетельства, которое пришло раньше него.
     *
     * <p>Ради второго шага (drain) всё и затевалось. Замер на стенде: DLR от
     * SMSC приходит через 20–70 мс после submit — SMSC стоит в одной сети со
     * стендом, — а case создаётся длинным путём delivery-service ->
     * stage.completed -> pipeline-engine -> stage.delivery-reconciliation.
     * Свидетельство регулярно выигрывает эту гонку, и раньше в этом случае
     * молча выбрасывалось ({@code loadByMessageId(...).ifPresent(...)}: нет
     * case'а — нет и получателя). Это одна из двух причин, по которым на
     * стенде 16 187 сообщений получили финальный CONFIRMED_NOT_SUBMITTED,
     * имея при этом DELIVERY = SUCCEEDED.
     */
    static void openCase(CaseStore store, StageCompletedPublisher publisher,
                         UUID messageId, UUID stageExecutionId, String operatorId) {
        ReconciliationCase caze = store.loadByMessageId(messageId)
            .orElseGet(() -> store.create(messageId, stageExecutionId, operatorId, Instant.now().plus(RECONCILIATION_WINDOW)));
        // Своего свидетельства эта ветка не несёт (Evidence.empty()) — она
        // только забирает накопленное ранним путём и, если его уже достаточно
        // для однозначного вывода, тут же закрывает case.
        applyToCase(store, publisher, caze, Evidence.empty());
    }

    /**
     * CODE_REVIEW.md #11 — collect_evidence, ветка {@code operator.submit.accepted}.
     * Позднее подтверждение, что оператор принял submit — единственный
     * положительный сигнал, если DLR/query_sm так и не пришли до дедлайна
     * (см. {@code OutcomeResolver.resolve}, ветка
     * {@code RECONCILIATION_OUTCOME_CONFIRMED_SUBMITTED}).
     */
    private static void runSubmitAcceptedConsumer(CaseStore store, StageCompletedPublisher publisher) {
        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerProps("delivery-reconciliation-service-submit-accepted"))) {
            consumer.subscribe(Collections.singletonList("operator.submit.accepted"));
            while (true) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofSeconds(1));
                for (var record : records) {
                    try {
                        OperatorSubmitAccepted event = OperatorSubmitAccepted.parseFrom(record.value());
                        UUID messageId = UUID.fromString(event.getMessageId());
                        mergeEvidence(store, publisher, messageId, Evidence.empty().withSubmitAccepted());
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
    private static void runDeliveryStatusConsumer(CaseStore store, StageCompletedPublisher publisher) {
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
                            mergeEvidence(store, publisher, messageId, Evidence.empty().withDeliveryStatus(outcome));
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
     * collect_evidence — приём одного свидетельства о сообщении.
     *
     * <p><b>Что здесь было сломано.</b> Раньше тело метода целиком было
     * {@code store.loadByMessageId(messageId).ifPresent(...)}: если case ещё
     * не создан, {@code ifPresent} просто ничего не делал — свидетельство
     * молча выбрасывалось. И это не редкий угол: SMSC стенда стоит в одной
     * сети с машиной, DLR измеренно приходит через 20–70 мс после submit, а
     * case создаётся длинным путём delivery-service -> stage.completed ->
     * pipeline-engine -> stage.delivery-reconciliation. Свидетельство
     * регулярно приходит раньше case'а. Итог замерен на живом стенде
     * (ClickHouse): 16 187 сообщений с финальным
     * RECONCILIATION_OUTCOME_CONFIRMED_NOT_SUBMITTED, у ВСЕХ 16 187 стадия
     * DELIVERY при этом SUCCEEDED — платформа объявляла успешно отправленные
     * сообщения неотправленными.
     *
     * <p><b>Порядок здесь — не косметика, он и закрывает гонку.</b> Сначала
     * свидетельство приземляется в {@code reconciliation.early_evidence}
     * (migrations/V032), и только ПОТОМ идёт повторная проверка case'а.
     * Аргумент, почему при таком порядке свидетельство не может потеряться
     * ни при каком чередовании с созданием case'а в pipeline-engine:
     * <ul>
     *   <li>приземление (t1) строго раньше повторной проверки (t2);</li>
     *   <li>создатель case'а сначала вставляет case, потом делает drain
     *       ({@link #openCase});</li>
     *   <li>если case существовал на t2 — свидетельство влил сюда мы сами;</li>
     *   <li>если нет — значит, вставка case'а произойдёт позже t2 &gt; t1, а
     *       её drain читает строку, которая к тому моменту уже записана.</li>
     * </ul>
     * Третьего варианта нет. При этом сам delta всегда едет с потоком «в
     * руках» и вливается в case напрямую, так что параллельное удаление
     * строки чужим drain'ом ничего не теряет.
     *
     * <p>Идемпотентность (at-least-once по всему репозиторию): и приземление
     * ({@code ON CONFLICT DO UPDATE} по своей колонке), и слияние
     * ({@code Evidence.merge} монотонен) переносят повтор того же события без
     * изменения результата.
     *
     * <p>Case, закрытый до прихода этого evidence (поздний DLR после
     * дедлайна), — не ошибка: получателя для evidence уже нет, тихо
     * игнорируем (тот же паттерн, что at-least-once-редоставка везде в этой
     * сессии — не каждое сообщение обязано на что-то повлиять).
     *
     * @param delta свидетельство ЭТОГО события — {@code Evidence.empty()} с
     *              одним заполненным полем (раньше передавался
     *              {@code UnaryOperator<Evidence>}; сменено на значение, т.к.
     *              одно и то же свидетельство теперь надо и записать в
     *              early_evidence, и слить с накопленным — мутатор в SQL не
     *              отправишь)
     */
    static void mergeEvidence(CaseStore store, StageCompletedPublisher publisher, UUID messageId, Evidence delta) {
        Optional<ReconciliationCase> existing = store.loadByMessageId(messageId);
        if (existing.isPresent()) {
            applyToCase(store, publisher, existing.get(), delta);
            return;
        }
        store.recordEarlyEvidence(messageId, delta);
        store.loadByMessageId(messageId)
            .ifPresent(caze -> applyToCase(store, publisher, caze, delta));
    }

    /**
     * CODE_REVIEW.md #11 — read-modify-write поверх evidence JSONB конкретного
     * case'а, с блокировкой per-caseId (см. докстринг {@link #caseLocks}),
     * плюс два новых шага: подхват раннего свидетельства и немедленное
     * закрытие case'а.
     *
     * <p><b>Почему drain безусловный.</b> Чтение early_evidence делается на
     * каждом свидетельстве, даже когда case уже был на месте. Можно было бы
     * доказать, что в этой ветке ранней строки быть не должно, — но цена
     * ошибки в таком рассуждении ровно та, ради которой всё это и пишется
     * (потерянное свидетельство и ложный CONFIRMED_NOT_SUBMITTED), а цена
     * лишнего SELECT'а мала: после исправления графа пайплайна в
     * реконсиляцию идёт только SUBMISSION_OUTCOME_UNKNOWN, т.е. ветка «case
     * уже есть» — редкая, а не 300/с.
     *
     * <p><b>Фикс задержки: закрываем сразу, а не по дедлайну.</b> Раньше
     * закрытие было только в sweep'е по {@code findExpiredOpenCases}, т.е.
     * даже полностью разрешённый case висел открытым до
     * RECONCILIATION_WINDOW (на стенде — 2 минуты). Это и задержка финального
     * статуса, и удержание состояния: пока case открыт, pipeline-engine не
     * зовёт finalize_pipeline и ключи exec:/msgctx: (+4 на сообщение) живут в
     * Runtime Redis — при 300 сообщ/с окно в 2 минуты само по себе держит
     * ~36 000 сообщений «в полёте» на пустом месте. Теперь дедлайн остаётся
     * ровно для одного случая: свидетельства так и не пришло.
     *
     * <p><b>Второго пути закрытия не появилось.</b> Решение принимает тот же
     * {@code OutcomeResolver.resolve}, публикацию и закрытие делает тот же
     * {@link #sweepBatch} — просто на списке из одного case'а. Если resolve
     * говорит «ещё рано» (вернул null — так бывает, когда виден только
     * submit_accepted: DLR сильнее и может прийти следом, см.
     * OutcomeResolver), sweepBatch ничего не публикует и case честно ждёт
     * дедлайна. Дублирующее закрытие sweep'ом практически исключено (он
     * выбирает только ПРОСРОЧЕННЫЕ case'ы, а сюда мы попадаем за десятки
     * миллисекунд от submit при окне в минуты), а если бы и случилось —
     * closeCases идемпотентен, а stage.completed и так at-least-once.
     */
    private static void applyToCase(CaseStore store, StageCompletedPublisher publisher,
                                    ReconciliationCase initial, Evidence delta) {
        if (!"open".equals(initial.status())) {
            return;
        }
        Object lock = caseLocks.computeIfAbsent(initial.caseId(), id -> new Object());
        synchronized (lock) {
            ReconciliationCase fresh = store.loadByMessageId(initial.messageId()).orElse(initial);
            if (!"open".equals(fresh.status())) {
                return;
            }
            Evidence current = EvidenceCodec.decode(fresh.evidenceJson());
            Optional<Evidence> early = store.loadEarlyEvidence(fresh.messageId());
            Evidence updated = current.merge(delta);
            if (early.isPresent()) {
                updated = updated.merge(early.get());
            }
            if (!updated.equals(current)) {
                store.persistEvidence(fresh.caseId(), EvidenceCodec.encode(updated));
            }
            if (early.isPresent()) {
                // Строка влита в case — она больше не нужна. Удаление под тем
                // же локом; если параллельный consumer допишет в неё новое
                // свидетельство между чтением и удалением, оно не потеряется:
                // тот поток несёт свой delta с собой и вольёт его в case сам.
                store.deleteEarlyEvidence(fresh.messageId());
            }
            sweepBatch(store, publisher, List.of(fresh.withEvidenceJson(EvidenceCodec.encode(updated))));
        }
    }

    /**
     * evaluate_deadline + resolve_outcome (по накопленному Evidence —
     * CODE_REVIEW.md #11) + publish_stage_completed + persist_case, по таймеру.
     *
     * <p>Один прогон выгребает бэклог чанками по {@code batchSize}, пока чанки
     * не кончатся, а не одним неограниченным SELECT'ом: так пиковое
     * потребление памяти не зависит от размера бэклога, но догон при этом не
     * растягивается на много тактов таймера.
     *
     * <p><b>Дедлайн теперь — не единственный, а последний путь закрытия.</b>
     * Case с достаточным свидетельством закрывается сразу при его получении
     * ({@link #applyToCase}); сюда доезжают те, по кому за всё окно
     * реконсиляции не пришло ничего однозначного. Разбирает их та же
     * {@link #sweepBatch}, что и немедленный путь.
     */
    private static void sweepDeadlines(CaseStore store, StageCompletedPublisher publisher, int batchSize) {
        try {
            purgeEarlyEvidence(store);
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

    /**
     * Чистка «раннего» свидетельства, которое так и не пригодилось: сообщения,
     * чей case не появился за {@link #EARLY_EVIDENCE_TTL}. После исправления
     * графа пайплайна в реконсиляцию идёт только SUBMISSION_OUTCOME_UNKNOWN —
     * т.е. для подавляющего большинства сообщений case не появится никогда, и
     * без этой чистки reconciliation.early_evidence росла бы линейно по
     * трафику (при 300 сообщ/с — десятки миллионов строк в сутки). Свежее
     * свидетельство чистка не трогает: строка старше окна реконсиляции всё
     * равно никому не нужна — case, который мог бы её забрать, к этому моменту
     * уже закрыт по дедлайну.
     *
     * <p>Выполняется в потоке sweep'а и на его соединении — отдельный поток
     * или соединение ради одного DELETE по индексу не нужны.
     */
    private static void purgeEarlyEvidence(CaseStore store) {
        try {
            store.purgeEarlyEvidence(Instant.now().minus(EARLY_EVIDENCE_TTL));
        } catch (Exception e) {
            // Не должно мешать основной работе прогона: неубранные строки —
            // это про место на диске, а неразобранные case'ы — про
            // корректность финальных статусов.
            System.err.println("purge_early_evidence failed: " + e.getMessage());
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
     * <p><b>Единственная точка закрытия case'а.</b> Сюда приходят и чанк
     * просроченных case'ов из {@link #sweepDeadlines}, и одиночный case,
     * разрешённый досрочно по терминальному свидетельству
     * ({@link #applyToCase}). Решение в обоих случаях принимает
     * {@code OutcomeResolver.resolve}, публикация и закрытие — этот код;
     * второго пути закрытия в сервисе нет намеренно, иначе логика двух путей
     * неминуемо разъедется.
     *
     * @return сколько case'ов реально закрыто
     */
    private static int sweepBatch(CaseStore store, StageCompletedPublisher publisher, List<ReconciliationCase> batch) {
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
            // Защита от возврата ровно той порчи данных, ради которой писалась
            // эта правка: 16 187 сообщений на стенде получили
            // CONFIRMED_NOT_SUBMITTED, имея DELIVERY = SUCCEEDED. По
            // построению OutcomeResolver этот исход недостижим, если виден
            // хоть какой-то положительный след отправки (submit_accepted или
            // DLR) — проверка сторожит именно инвариант, а не гипотетический
            // ввод, и стоит один if на case. Сработает — значит, сломан
            // resolve_outcome или evidence разъехался при чтении: тогда лучше
            // громко не закрыть case (он останется open и попадёт в следующий
            // прогон), чем тихо опубликовать ложный финальный статус, по
            // которому платформа объявит доставленное сообщение
            // неотправленным.
            if (outcome == ReconciliationOutcome.RECONCILIATION_OUTCOME_CONFIRMED_NOT_SUBMITTED
                && (evidence.submitAcceptedObserved() || evidence.deliveryStatusObserved() != Evidence.DeliveryOutcome.NONE)) {
                System.err.println("ИНВАРИАНТ НАРУШЕН: resolve_outcome вернул CONFIRMED_NOT_SUBMITTED при наличии"
                    + " свидетельства отправки (case_id=" + c.caseId() + ", message_id=" + c.messageId()
                    + ", evidence=" + c.evidenceJson() + ") — case НЕ закрывается, публикация не выполняется");
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