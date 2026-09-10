package uz.mpp.operatorsmpp;

import com.google.protobuf.Timestamp;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import uz.mpp.operatorsmpp.client.OperatorSmppClient;
import uz.mpp.operatorsmpp.client.SmppConnectionSupervisor;
import uz.mpp.operatorsmpp.core.AdaptiveThreadPoolCalibrator;
import uz.mpp.operatorsmpp.core.AdaptiveThreadPoolCalibrator.ResizableSemaphore;
import uz.mpp.operatorsmpp.core.DeliveryReceiptParser;
import uz.mpp.operatorsmpp.core.PacerCore;
import uz.mpp.operatorsmpp.core.PduLogEvent;
import uz.mpp.operatorsmpp.core.PacerMetrics;
import uz.mpp.operatorsmpp.core.PriorityGate;
import uz.mpp.operatorsmpp.core.PriorityTier;
import uz.mpp.operatorsmpp.core.TokenBucket;
import uz.mpp.operatorsmpp.grpcserver.OperatorQuerySmServer;
import uz.mpp.operatorsmpp.grpcserver.OperatorSubmitServer;
import uz.mpp.operatorsmpp.grpcserver.QueuedSubmit;
import uz.mpp.operatorsmpp.health.HealthServer;
import uz.mpp.operatorsmpp.kafkaio.OperatorEventPublisher;
import uz.mpp.operatorsmpp.registry.OperatorRouteRegistry;
import uz.mpp.platformcontracts.common.v1.Protocol;
import uz.mpp.platformcontracts.events.v1.OperatorDlr;
import uz.mpp.platformcontracts.events.v1.OperatorPduLog;
import uz.mpp.platformcontracts.events.v1.PduDirection;

import java.time.Duration;
import java.time.Instant;
import java.util.EnumMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Operator SMPP Session Manager (services_specifictaion.md §2.3) — SMPP
 * binds с операторами, reconnect, enquire_link, TPS/throttling,
 * submit_sm/DLR, приоритет submit_sm над query_sm.
 *
 * <p>dynamic-seeking-russell.md "Priority-tier scheduler": единственный
 * процесс, владеющий реальным SMPP-туннелем к данному оператору — поэтому
 * пейсер (3 очереди по приоритету + HTB-style bucket'ы + Semaphore
 * конкурентности + tick-петля) живёт здесь, а не в delivery-service (там
 * может быть много реплик, ни одна не имела бы корректного глобального
 * представления об общем туннеле).
 */
public final class Main {

    public static void main(String[] args) throws Exception {
        String operatorId = env("OPERATOR_ID", "beeline_uz");
        String routeId = env("ROUTE_ID", "route-1");
        String kafkaBrokers = env("KAFKA_BOOTSTRAP_SERVERS", "kafka-bootstrap.mpp.svc:9092");
        OperatorEventPublisher eventPublisher = new OperatorEventPublisher(kafkaBrokers);

        String redisUri = RedisUrl.buildRuntimeUrl();
        OperatorRouteRegistry routeRegistry = new OperatorRouteRegistry(redisUri, env("HOSTNAME", "operator-smpp-session-manager-0"), Duration.ofSeconds(30));

        // per-PDU диагностический след (BACKOFFICE_DESIGN_SPEC.md Экраны
        // 38-40) — построение OperatorPduLog остаётся здесь (Main.java), не
        // в client/codec-слое, тем же принципом, что и dlrSink ниже: тот
        // слой видит только сырые SMPP-структуры (PduLogEvent), протобуф и
        // Kafka-топик — забота этого файла.
        Consumer<PduLogEvent> pduLogSink = pduEvent -> {
            OperatorPduLog log = OperatorPduLog.newBuilder()
                .setOperatorId(operatorId)
                .setProtocol(Protocol.PROTOCOL_SMPP)
                .setDirection(PduLogEvent.DIRECTION_A2P.equals(pduEvent.direction())
                    ? PduDirection.PDU_DIRECTION_A2P : PduDirection.PDU_DIRECTION_DLR)
                .setPduType(pduEvent.pduType())
                .setSequenceNumber(pduEvent.sequenceNumber())
                .setMessageId(pduEvent.messageId())
                .setStageExecutionId(pduEvent.stageExecutionId())
                .setSmscMessageId(pduEvent.smscMessageId())
                .setSegmentId(1)
                .setStatus(pduEvent.status())
                .setOccurredAt(toTimestamp(pduEvent.occurredAt()))
                .build();
            eventPublisher.publishPduLog(log);
        };

        OperatorSmppClient client = new OperatorSmppClient(dlrPdu -> {
            String rawReceipt = new String(dlrPdu.shortMessage());
            // smsc_message_id — ЕДИНСТВЕННЫЙ ключ корреляции DLR с
            // message_id (dlr-manager джойнит по нему dlr.dlr_correlation).
            // Раньше здесь он не заполнялся вообще — dlr-manager отбрасывал
            // каждый DLR с "OperatorDlr без smsc_message_id", delivery.status
            // оставался пустым, и любое сообщение навсегда застревало в
            // SUBMITTED. См. DeliveryReceiptParser javadoc.
            OperatorDlr dlr = OperatorDlr.newBuilder()
                .setOperatorId(operatorId)
                .setProtocol(Protocol.PROTOCOL_SMPP)
                .setSmscMessageId(DeliveryReceiptParser.extractSmscMessageId(rawReceipt))
                // segment_id: у одиночного (не multipart) receipt'а сегмент
                // всегда 1 — это же значение проставляет delivery-service при
                // публикации operator.submit.accepted для односегментных
                // сообщений, так что ключ корреляции сходится. Реального
                // per-segment DLR из PDU здесь не достать: SMPP-receipt не
                // несёт номер сегмента как отдельное поле.
                .setSegmentId(1)
                // raw_status по контракту operator_events.proto — это КОД
                // статуса в терминологии оператора ("DELIVRD"), а не весь
                // текст receipt'а: dlr-manager делает по нему прямой lookup
                // в словаре SMPP v3.4 §4.7.3 (см. dlr/parser.go
                // normalizedStatusByRawSMPPStat). Раньше сюда клался весь
                // receipt целиком — lookup не находил его никогда, и
                // dlr-manager отбрасывал КАЖДЫЙ DLR с "нераспознанный
                // raw_status", даже когда корреляция уже находилась.
                //
                // Полный текст receipt'а при этом теряется — под него в
                // proto нет отдельного поля. Для "delivery snippet" в UI
                // (см. BACKOFFICE_DESIGN_SPEC.md, блок C карточки
                // сообщения) нужно добавить отдельное поле raw_receipt в
                // OperatorDlr — отдельная правка с регенерацией protobuf,
                // сюда не мешается.
                .setRawStatus(DeliveryReceiptParser.extractStatus(rawReceipt))
                .setReceivedAt(toTimestamp(Instant.now()))
                .build();
            eventPublisher.publishDlr(dlr);
        }, pduLogSink);

        // --- Priority-tier scheduler (пейсер) ---
        double tpsLimit = Double.parseDouble(env("TPS_LIMIT", "500"));
        int maxConcurrentSubmits = Integer.parseInt(env("MAX_CONCURRENT_SUBMITS", "100"));
        long nowMs = System.currentTimeMillis();

        // ceilBucket — полный потолок туннеля (capacity==rate, БЕЗ
        // headroom-урезания — та же семантика "просто TPS-лимит", что была
        // у старого tpsBucket). highBucket/mediumBucket/lowBucket — С
        // headroom-урезанием (см. TokenBucket.withBurstHeadroom): это они
        // копят токены простоя и потом могли бы единомоментно их слить,
        // ceilBucket ничего "не копит" сверх своего honest rate.
        TokenBucket ceilBucket = new TokenBucket(tpsLimit, tpsLimit, nowMs);
        TokenBucket highBucket = TokenBucket.withBurstHeadroom(tpsLimit * PacerCore.HIGH_SHARE, nowMs);
        TokenBucket mediumBucket = TokenBucket.withBurstHeadroom(tpsLimit * PacerCore.MEDIUM_SHARE, nowMs);
        TokenBucket lowBucket = TokenBucket.withBurstHeadroom(tpsLimit * PacerCore.LOW_SHARE, nowMs);
        PacerCore pacerCore = new PacerCore(ceilBucket, highBucket, mediumBucket, lowBucket);

        // Semaphore — НОВЫЙ механизм, отдельный от PriorityGate: PriorityGate
        // сегодня только СЧИТАЕТ in-flight submit'ы, чтобы решить, откладывать
        // ли query_sm (enforce_query_sm_priority), реального предела
        // конкурентности не задаёт. ResizableSemaphore — жёсткий предел
        // одновременных client.submitSm() к оператору, стартует на
        // AdaptiveThreadPoolCalibrator.FLOOR и растёт/сжимается в лок-степе
        // с pacerWorkerPool (см. calibrator ниже) — не на статическом
        // MAX_CONCURRENT_SUBMITS напрямую (тот путь однажды уже уронил
        // саму SMPP-сессию под 300 потоками на этой машине, см.
        // AdaptiveThreadPoolCalibrator javadoc/b713c53).
        ResizableSemaphore concurrentSubmitPermits = new ResizableSemaphore(AdaptiveThreadPoolCalibrator.FLOOR);

        Map<PriorityTier, Integer> maxQueueDepthByTier = buildMaxQueueDepthByTier(tpsLimit);
        Map<PriorityTier, ArrayBlockingQueue<QueuedSubmit>> queuesByTier = new EnumMap<>(PriorityTier.class);
        for (PriorityTier tier : PriorityTier.values()) {
            // Реальная Java-ёмкость всегда >= 1 (ArrayBlockingQueue не
            // поддерживает 0) — логический предел (может быть 0, например
            // MAX_QUEUE_DEPTH_PER_TIER=0 в тестах) применяется отдельно,
            // явной проверкой в OperatorSubmitServer.submit().
            queuesByTier.put(tier, new ArrayBlockingQueue<>(Math.max(1, maxQueueDepthByTier.get(tier))));
        }

        long maxQueueWaitMs = Long.parseLong(env("MAX_QUEUE_WAIT_MS", "2000"));

        PacerMetrics pacerMetrics = new PacerMetrics();
        for (PriorityTier tier : PriorityTier.values()) {
            ArrayBlockingQueue<QueuedSubmit> queue = queuesByTier.get(tier);
            pacerMetrics.bindQueueDepth(tier, queue::size);
        }

        HealthServer health = new HealthServer(pacerMetrics, client);
        health.start();

        connectAndBindWithRetry(client, operatorId, routeId, routeRegistry);

        // reconnect (CODE_REVIEW.md CRITICAL #1, было: connectAndBindWithRetry запускался
        // только здесь, один раз, при старте — обрыв после этой точки никогда не приводил
        // к повторному connect+bind, и health.setReady никогда не переоценивался). Supervisor
        // вешает слушателя на channel.closeFuture(), при обрыве роняет readiness и запускает
        // тот же connectAndBindWithRetry в отдельном потоке; после успеха — readiness назад
        // в true, слушатель перевешивается на новый channel.
        SmppConnectionSupervisor connectionSupervisor = new SmppConnectionSupervisor(
            client,
            () -> connectAndBindWithRetry(client, operatorId, routeId, routeRegistry),
            health::setReady);
        connectionSupervisor.arm();

        Timer enquireLinkTimer = new Timer("enquire-link-tick", true);
        enquireLinkTimer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                if (client.isActive()) {
                    client.sendEnquireLink();
                    routeRegistry.heartbeat(operatorId, routeId);
                }
            }
        }, 30_000, 30_000);

        PriorityGate priorityGate = new PriorityGate(maxConcurrentSubmits);
        OperatorSubmitServer submitServer = new OperatorSubmitServer(
            client, queuesByTier, maxQueueDepthByTier, priorityGate, eventPublisher, pacerMetrics);

        // Worker-пул, дёргающий реальный client.submitSm() — тот же
        // daemon-ThreadFactory convention, что уже установлен в
        // billing-service/KafkaIo.java и delivery-service/KafkaIo.java.
        // Стартует на AdaptiveThreadPoolCalibrator.FLOOR, не на
        // maxConcurrentSubmits напрямую — calibrator ниже сам находит
        // безопасный размер (растит и pacerWorkerPool, и
        // concurrentSubmitPermits в лок-степе).
        ThreadPoolExecutor pacerWorkerPool = new ThreadPoolExecutor(
            AdaptiveThreadPoolCalibrator.FLOOR, AdaptiveThreadPoolCalibrator.FLOOR,
            0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), r -> {
                Thread t = new Thread(r, "operator-smpp-pacer-worker");
                t.setDaemon(true);
                return t;
            });
        AdaptiveThreadPoolCalibrator threadPoolCalibrator = new AdaptiveThreadPoolCalibrator(
            pacerWorkerPool, concurrentSubmitPermits, client::isActive,
            maxConcurrentSubmits,
            Integer.parseInt(env("THREAD_POOL_CALIBRATION_WINDOW_MS", "120000")),
            Integer.parseInt(env("THREAD_POOL_CALIBRATION_TICK_MS", "10000")));

        // Tick-петля пейсера — 20мс (50Hz), тот же Timer-паттерн, что и
        // enquire-link-tick выше. Каждый тик: дренирует протухшие элементы
        // (safety valve max-wait), считает PacerCore.decide(), диспетчирует
        // допущенные элементы в pacerWorkerPool.
        // ИЗМЕРЕНО 2026-09-07 (JFR delivery-service, 300 TPS): воркеры
        // delivery-service проводят 60% времени в блокирующем gRPC submit —
        // 1473с из 2435с суммарного park-времени, в среднем 53.8мс на вызов.
        // При RTT до SMSC ~12мс это в основном НЕ сеть. Часть разницы —
        // дискретизация тиком: сообщение, пришедшее сразу после тика, ждёт
        // следующего, то есть в среднем половину периода. 20мс -> 5мс убирает
        // ~7мс среднего ожидания на каждое сообщение. Вынесено в env, чтобы
        // подбирать без пересборки (тик дешёвый: это не поток на сообщение, а
        // один Timer, который лишь считает PacerCore.decide()).
        int pacerTickMs = Integer.parseInt(env("PACER_TICK_MS", "5"));
        Timer pacerTimer = new Timer("pacer-dispatch-tick", true);
        pacerTimer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                runPacerTick(queuesByTier, pacerCore, concurrentSubmitPermits, pacerMetrics,
                    submitServer, pacerWorkerPool, maxQueueWaitMs, threadPoolCalibrator);
            }
        }, 0, pacerTickMs);

        Server grpcServer = ServerBuilder.forPort(Integer.parseInt(env("GRPC_PORT", "9000")))
            .addService(submitServer)
            .addService(new OperatorQuerySmServer(priorityGate))
            .build()
            .start();
        System.out.println("gRPC OperatorSubmitService/OperatorQuerySmService слушает :" + env("GRPC_PORT", "9000"));

        health.setReady(true);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            connectionSupervisor.stop();
            enquireLinkTimer.cancel();
            pacerTimer.cancel();
            pacerWorkerPool.shutdown();
            grpcServer.shutdown();
            client.close();
            routeRegistry.unregister(operatorId, routeId);
            routeRegistry.close();
            eventPublisher.close();
            health.stop();
        }));

        grpcServer.awaitTermination();
    }

    /**
     * Safety valve + PacerCore.decide() + фактическая диспетчеризация в
     * worker-пул — один тик (20мс) tick-петли пейсера. Единственный
     * "consumer" всех трёх очередей (single-threaded Timer), поэтому
     * состояние очередей между началом и концом одного вызова не может
     * поменяться конкурентно ниоткуда, кроме producer-стороны (submit()
     * добавляет новые элементы через offer(), это безопасно для
     * ArrayBlockingQueue при конкурентном poll()).
     */
    private static void runPacerTick(Map<PriorityTier, ArrayBlockingQueue<QueuedSubmit>> queuesByTier,
                                      PacerCore pacerCore, Semaphore concurrentSubmitPermits, PacerMetrics pacerMetrics,
                                      OperatorSubmitServer submitServer, ThreadPoolExecutor pacerWorkerPool, long maxQueueWaitMs,
                                      AdaptiveThreadPoolCalibrator threadPoolCalibrator) {
        long now = System.currentTimeMillis();

        // Safety valve: max-wait-per-item. Протухшие элементы (дольше
        // MAX_QUEUE_WAIT_MS в голове FIFO) дренируются и отклоняются ДО
        // расчёта плана диспетчеризации — PacerCore ничего не знает о
        // времени ожидания (только про глубину очереди/bucket'ы), значит
        // без этого шага протухший элемент мог бы всё равно попасть в план.
        for (PriorityTier tier : PriorityTier.values()) {
            ArrayBlockingQueue<QueuedSubmit> queue = queuesByTier.get(tier);
            QueuedSubmit head;
            while ((head = queue.peek()) != null && now - head.enqueuedAtEpochMs() > maxQueueWaitMs) {
                QueuedSubmit expired = queue.poll();
                if (expired == null) {
                    break;
                }
                pacerMetrics.recordRejected(tier, "PACER_QUEUE_TIMEOUT");
                OperatorSubmitServer.sendRejected(expired.responseObserver(), "PACER_QUEUE_TIMEOUT");
            }
        }

        Map<PriorityTier, Integer> queueDepth = new EnumMap<>(PriorityTier.class);
        for (PriorityTier tier : PriorityTier.values()) {
            queueDepth.put(tier, queuesByTier.get(tier).size());
        }

        PacerCore.AdmitPlan plan = pacerCore.decide(queueDepth, concurrentSubmitPermits.availablePermits(), now);

        for (PriorityTier tier : PriorityTier.values()) {
            ArrayBlockingQueue<QueuedSubmit> queue = queuesByTier.get(tier);
            int admits = plan.forTier(tier);
            for (int i = 0; i < admits; i++) {
                QueuedSubmit queued = queue.poll();
                if (queued == null) {
                    break; // очередь опустела раньше плана (например протухла между расчётом и дренажом) — защитно
                }
                if (!concurrentSubmitPermits.tryAcquire()) {
                    // PacerCore уже учёл availablePermits при расчёте плана —
                    // сюда попадаем только при гонке, которой в
                    // single-threaded tick-петле быть не должно. Защитно
                    // возвращаем элемент в очередь (в хвост — FIFO-порядок
                    // здесь не критичен, это fallback-путь, не штатный).
                    queue.offer(queued);
                    break;
                }
                pacerMetrics.recordDispatched(tier);
                pacerWorkerPool.submit(() -> {
                    long startNanos = System.nanoTime();
                    try {
                        submitServer.dispatchOne(queued);
                        threadPoolCalibrator.recordTaskLatency((System.nanoTime() - startNanos) / 1_000_000);
                    } finally {
                        concurrentSubmitPermits.release();
                    }
                });
            }
        }
    }

    /**
     * MAX_QUEUE_DEPTH_PER_TIER — если задан, применяется КАК ЕСТЬ ко всем
     * трём tier'ам одинаково (используется в тестах, например =0, чтобы
     * форсировать немедленный PACER_QUEUE_FULL). Если не задан — дефолт
     * ≈3 секунды гарантированного трафика ЭТОГО tier'а (dynamic-seeking-russell.md
     * "Safety valve"), считается из TPS_LIMIT и его доли (70/20/10).
     */
    private static Map<PriorityTier, Integer> buildMaxQueueDepthByTier(double tpsLimit) {
        Map<PriorityTier, Integer> maxQueueDepthByTier = new EnumMap<>(PriorityTier.class);
        String override = System.getenv("MAX_QUEUE_DEPTH_PER_TIER");
        if (override != null && !override.isEmpty()) {
            int depth = Integer.parseInt(override);
            for (PriorityTier tier : PriorityTier.values()) {
                maxQueueDepthByTier.put(tier, depth);
            }
            return maxQueueDepthByTier;
        }
        maxQueueDepthByTier.put(PriorityTier.HIGH, (int) Math.round(3 * tpsLimit * PacerCore.HIGH_SHARE));
        maxQueueDepthByTier.put(PriorityTier.MEDIUM, (int) Math.round(3 * tpsLimit * PacerCore.MEDIUM_SHARE));
        maxQueueDepthByTier.put(PriorityTier.LOW, (int) Math.round(3 * tpsLimit * PacerCore.LOW_SHARE));
        return maxQueueDepthByTier;
    }

    /**
     * connect_and_bind с retry — простая retry-петля с линейным backoff
     * ({@code min(30s, attempt*1s)}). Используется и при первом старте (блокирующе, в
     * {@code main}), и как {@link SmppConnectionSupervisor.Connector} для реального
     * reconnect после обрыва (CODE_REVIEW.md CRITICAL #1) — до фикса эта петля
     * запускалась ровно один раз за жизнь процесса. Реальная reconnect_policy
     * (backoff, jitter, лимит попыток) из partner config snapshot по-прежнему не
     * подключена в этом срезе (см. README "Что НЕ реализовано") — attempt-счётчик
     * локален для каждого вызова, т.е. каждый reconnect снова начинает backoff с 1с.
     */
    private static void connectAndBindWithRetry(OperatorSmppClient client, String operatorId, String routeId, OperatorRouteRegistry routeRegistry) throws InterruptedException {
        String host = env("OPERATOR_SMSC_HOST", "localhost");
        int port = Integer.parseInt(env("OPERATOR_SMSC_PORT", "2775"));
        String systemId = env("SMPP_SYSTEM_ID", "mpp_esme");
        String password = env("SMPP_PASSWORD", "demo_password");
        // HLD §11.4 / GatewayRegistry.resolve (delivery-service): endpoint в
        // operator_route:{operator_id}:{route_id} — адрес ВЛАДЕЮЩЕГО ИНСТАНСА
        // для instance-addressed gRPC от Delivery, не адрес SMSC. Реальная
        // находка (docker-compose прогон против живого SMSC, не статичное
        // чтение): здесь ошибочно писался host+port SMSC — Delivery дозванивался
        // напрямую на SMPP-порт SMSC как на gRPC-таргет и падал с UNAVAILABLE.
        // Тот же паттерн, что уже сделан правильно в partner-smpp-gateway/Main.java
        // (env("HOSTNAME") + ":" + свой listen-порт).
        String ownGrpcEndpoint = env("HOSTNAME", "operator-smpp-session-manager-0") + ":" + env("GRPC_PORT", "9000");

        int attempt = 0;
        while (true) {
            try {
                client.connect(host, port);
                client.bind(systemId, password, "", 5000);
                long routeEpoch = System.nanoTime();
                routeRegistry.register(operatorId, routeId, routeEpoch, ownGrpcEndpoint);
                return;
            } catch (Exception e) {
                attempt++;
                System.err.println("bind_operator попытка " + attempt + " не удалась: " + e.getMessage());
                Thread.sleep(Math.min(30_000, 1000L * attempt));
            }
        }
    }

    private static Timestamp toTimestamp(Instant instant) {
        return Timestamp.newBuilder().setSeconds(instant.getEpochSecond()).setNanos(instant.getNano()).build();
    }

    private static String env(String key, String fallback) {
        String v = System.getenv(key);
        return (v == null || v.isEmpty()) ? fallback : v;
    }
}
