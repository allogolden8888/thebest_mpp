package uz.mpp.delivery;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import java.time.Instant;
import uz.mpp.delivery.DeliveryService.SubmitOutcome;
import uz.mpp.delivery.GatewayRegistry.GatewayEndpoint;
import uz.mpp.delivery.MessageContextStore.MessageContext;
import uz.mpp.delivery.SegmentMessage.Segment;
import uz.mpp.platformcontracts.common.v1.DeliveryExtension;
import uz.mpp.platformcontracts.common.v1.Outcome;
import uz.mpp.platformcontracts.common.v1.StageCompletedEvent;
import uz.mpp.platformcontracts.common.v1.StageExecuteCommand;
import uz.mpp.platformcontracts.events.v1.DeliveryStatusEvent;
import uz.mpp.platformcontracts.grpc.v1.SubmitRequest;
import uz.mpp.platformcontracts.grpc.v1.SubmitResponse;

/**
 * Kafka I/O — потребляет {@code stage.delivery}, публикует {@code stage.completed}.
 * Тот же offset-per-partition паттерн, что {@code billing-service/KafkaIo.java}
 * (найденный кодревью класс бага — {@code commitAsync()} без аргументов
 * коммитит позицию всего фетча, не оффсет только что обработанной записи —
 * применён здесь с самого начала, не после отдельной находки).
 *
 * <p>Осознанно не входит в этот срез (тот же явно раскрытый класс gap, что
 * {@code destination-resolution-service/kafka_io.rs}): DLQ на не парсящийся
 * {@code StageExecuteCommand} (в отличие от отсутствующего msgctx — исправлено
 * ниже — здесь нет валидного {@code message_id}/{@code stage_execution_id},
 * StageCompletedEvent построить нечем). {@code stage.delivery.dlq} уже
 * запланирован в {@code infra/kafka/}, но producer сюда не подключён —
 * partition-suspend поведение ниже блокирует такую запись навсегда, до
 * ручного вмешательства; по архитектуре (hld.md §20, "Critical Sweep")
 * маршрутизация в DLQ по deadline — не локальная забота отдельного
 * stage-consumer'а.</p>
 */
public final class KafkaIo {

    private static final Logger LOG = Logger.getLogger(KafkaIo.class.getName());
    public static final String INPUT_TOPIC = "stage.delivery";
    public static final String OUTPUT_TOPIC = "stage.completed";
    // Фаза 11 плана закрытия API-пробелов: тот же топик, что использует
    // dlr-manager для настоящих DLR (см. services/dlr-manager/internal/kafkaio,
    // TopicDeliveryStatus) — message-state-resolver не отличает синтетику
    // от реального DLR, читает оба одинаково.
    public static final String DELIVERY_STATUS_TOPIC = "delivery.status";
    private static final Duration PRODUCER_SEND_TIMEOUT = Duration.ofSeconds(10);

    public static KafkaConsumer<String, byte[]> buildConsumer(String bootstrapServers, String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        // NEXT_STEPS_1500TPS.md 1.2 подняло fetch.min.bytes=32768, рассчитывая
        // сэкономить round trip'ы под высоким throughput. 2026-08-18 —
        // реальное измерение (rate sweep 50/100/200/300 TPS на этой машине,
        // 8 CPU/7.75GB) показало latency НЕ падает на низких rate — p95 на
        // 50 TPS (1760мс) почти как на 300 TPS — то есть узкое место не в
        // пропускной способности, а в фиксированной стоимости на хоп. При
        // текущем объёме сообщений (маленький партнёр, некрупные payload'ы)
        // партиция физически не набирает 32КБ быстро, и консьюмер почти
        // всегда упирается в fetch.max.wait.ms (дефолт 500мс) — то есть эта
        // "оптимизация" добавляет фиксированный налог ~500мс НА ХОП вместо
        // экономии round trip'ов. Возвращаем к дефолту (1 байт — не ждать
        // накопления вообще), измерили эффект отдельно.
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1000);
        return new KafkaConsumer<>(props);
    }

    public static KafkaProducer<String, byte[]> buildProducer(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        // Тот же класс риска, что уже найден и исправлен в
        // billing-service/KafkaIo.java#buildProducer (см. комментарий там
        // для полного разбора): без явной настройки KafkaProducer
        // (kafka-clients 3.9.0) работает на дефолтах — buffer.memory=32MB,
        // max.block.ms=60000мс — а до DELIVERY_CONCURRENCY (128 по
        // умолчанию) worker-потоков шлют в один и тот же shared producer.
        // Здесь риск даже более прямой, чем в billing: producer.send()
        // вызывается ПОСЛЕ блокирующего gRPC submit (реальный SMPP
        // round-trip), так что каждый воркер держит слот в буфере дольше,
        // прежде чем его сообщение реально уйдёт в сеть — если брокер
        // отстаёт от темпа продьюса, send() молча БЛОКИРУЕТ вызывающий
        // поток до max.block.ms (60с!) в ожидании места в буфере, ДО
        // возврата Future, то есть до PRODUCER_SEND_TIMEOUT на .get() дело
        // не доходит. Применяем тот же фикс превентивно, тем же способом:
        // 1) buffer.memory поднят с запасом на всю конкурентность сервиса;
        // 2) max.block.ms снижен до PRODUCER_SEND_TIMEOUT — если
        // backpressure всё же наступит, воркер падает громко за 10с
        // (at-least-once переподхватит), а не зависает на минуту молча.
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 67_108_864L); // 64MB (было 32MB по умолчанию)
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, PRODUCER_SEND_TIMEOUT.toMillis()); // 10s (было 60s по умолчанию)
        // NEXT_STEPS_1500TPS.md 1.1: linger.ms=0 по умолчанию — каждый send()
        // уходит брокеру отдельным запросом. 5мс даёт клиенту собрать пачку
        // без заметного вклада в p50/p95 (бюджет — сотни мс).
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        return new KafkaProducer<>(props);
    }

    private static int envInt(String key, int fallback) {
        String v = System.getenv(key);
        if (v == null || v.isEmpty()) {
            return fallback;
        }
        try {
            return Integer.parseInt(v);
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    public static void run(
        KafkaConsumer<String, byte[]> consumer,
        KafkaProducer<String, byte[]> producer,
        MessageContextStore contextStore,
        GatewayRegistry gatewayRegistry,
        ControlSnapshot controlSnapshot,
        OperatorSubmitClient submitClient,
        SubmitIdempotencyStore idempotencyStore,
        AtomicBoolean running
    ) {
        consumer.subscribe(List.of(INPUT_TOPIC));
        // Реальная находка (нагрузочный прогон 1000 msg/s, JFR подтвердил
        // отсутствие CPU-хотспота — сервис большую часть времени ждёт, не
        // считает): последовательная обработка — Redis fetch(msgctx) ->
        // Redis resolve(gateway) -> БЛОКИРУЮЩИЙ gRPC submit (реальный SMPP
        // round-trip до SMSC через operator-smpp-session-manager) -> Kafka
        // produce().get() -> следующая запись. Тот же класс находки, что
        // уже был исправлен в billing-service/KafkaIo.java (пул потоков,
        // существующий OffsetTracker переиспользован без изменений — только
        // подаём результаты в него в порядке offset, не порядке завершения).
        // Все зависимости (MessageContextStore/GatewayRegistry/
        // SubmitIdempotencyStore — один общий Lettuce-коннекшн,
        // OperatorSubmitClient — ManagedChannel в ConcurrentHashMap,
        // KafkaProducer — потокобезопасен по документации клиента) уже были
        // сделаны потокобезопасными в более ранних правках этой сессии.
        // Самокалибрующийся размер пула вместо статического DELIVERY_CONCURRENCY
        // — тот же класс фикса, что billing-service/KafkaIo (см. javadoc
        // AdaptiveThreadPoolCalibrator).
        ThreadPoolExecutor pool = new ThreadPoolExecutor(
            AdaptiveThreadPoolCalibrator.FLOOR, AdaptiveThreadPoolCalibrator.FLOOR,
            0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), r -> {
                Thread t = new Thread(r, "delivery-worker");
                t.setDaemon(true);
                return t;
            });
        AdaptiveThreadPoolCalibrator calibrator = new AdaptiveThreadPoolCalibrator(
            pool,
            envInt("DELIVERY_CONCURRENCY_CEILING", 512),
            envInt("THREAD_POOL_CALIBRATION_WINDOW_MS", 120_000),
            envInt("THREAD_POOL_CALIBRATION_TICK_MS", 10_000));
        try {
            while (running.get()) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofSeconds(1));
                if (records.isEmpty()) {
                    continue;
                }

                Map<ConsumerRecord<String, byte[]>, Future<Void>> futures = new HashMap<>();
                Map<TopicPartition, List<ConsumerRecord<String, byte[]>>> byPartition = new HashMap<>();
                for (ConsumerRecord<String, byte[]> record : records) {
                    byPartition.computeIfAbsent(new TopicPartition(record.topic(), record.partition()), k -> new ArrayList<>()).add(record);
                    futures.put(record, pool.submit(() -> {
                        long startNanos = System.nanoTime();
                        processRecord(record, contextStore, gatewayRegistry, controlSnapshot, submitClient, idempotencyStore, producer);
                        calibrator.recordTaskLatency((System.nanoTime() - startNanos) / 1_000_000);
                        return null;
                    }));
                }

                OffsetTracker tracker = new OffsetTracker();
                for (Map.Entry<TopicPartition, List<ConsumerRecord<String, byte[]>>> entry : byPartition.entrySet()) {
                    TopicPartition tp = entry.getKey();
                    List<ConsumerRecord<String, byte[]>> ordered = entry.getValue();
                    ordered.sort(Comparator.comparingLong(ConsumerRecord::offset));
                    for (ConsumerRecord<String, byte[]> record : ordered) {
                        try {
                            futures.get(record).get();
                            tracker.recordSuccess(tp, record.offset());
                        } catch (Exception e) {
                            LOG.log(Level.SEVERE, "не удалось обработать stage.delivery запись partition=" + tp
                                + " offset=" + record.offset() + ", оффсет не коммитится, партишен приостановлен до следующего поллинга", e);
                            tracker.recordFailure(tp);
                            break; // остальные записи этой партиции в батче остаются неподтверждёнными, at-least-once
                        }
                    }
                }

                Map<TopicPartition, Long> committable = tracker.committableOffsets();
                if (!committable.isEmpty()) {
                    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                    committable.forEach((tp, offset) -> offsets.put(tp, new OffsetAndMetadata(offset)));
                    consumer.commitSync(offsets);
                }
            }
        } catch (org.apache.kafka.common.errors.WakeupException e) {
            if (running.get()) {
                throw e;
            }
        } finally {
            pool.shutdown();
        }
    }

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

    /**
     * package-private (не private) — тот же принцип, что
     * {@code billing-service/KafkaIo.processRecord}: напрямую тестируется
     * {@code KafkaIoProcessRecordTest} без живого {@link KafkaConsumer}.
     * {@code Producer<String, byte[]>}, не конкретный {@link KafkaProducer} —
     * позволяет подставить {@link org.apache.kafka.clients.producer.MockProducer}
     * в тестах, {@link #run} по-прежнему передаёт сюда настоящий {@link KafkaProducer}.
     */
    static void processRecord(
        ConsumerRecord<String, byte[]> record,
        MessageContextStore contextStore,
        GatewayRegistry gatewayRegistry,
        ControlSnapshot controlSnapshot,
        OperatorSubmitClient submitClient,
        SubmitIdempotencyStore idempotencyStore,
        Producer<String, byte[]> producer
    ) throws Exception {
        StageExecuteCommand command = StageExecuteCommand.parseFrom(record.value());
        DeliveryExtension extension = command.getDelivery();

        // check_control_state — повторная проверка OPERATOR_ROUTE перед submit.
        // HOLD не публикует stage.completed здесь — Scheduler Standard Lane
        // владеет решением, когда отпустить (см. DeliveryService.isAdmitted).
        if (!DeliveryService.isAdmitted(controlSnapshot.check(extension.getRouteId()))) {
            LOG.info("stage_execution_id=" + command.getStageExecutionId() + " held по OPERATOR_ROUTE control state, submit отложен");
            return;
        }

        // Детерминированный queue_msg_id (по stage_execution_id, не
        // UUID.randomUUID() на каждый вызов) — исправление CRITICAL находки
        // кодревью: hld.md §21 требует "stable stage_execution_id" как одну
        // из опор дедупликации; случайный id на каждую редеставку эту опору
        // ломал. См. SubmitIdempotencyStore.
        String stageExecutionId = command.getStageExecutionId();
        String deterministicQueueMsgId = "dlv-" + stageExecutionId;

        // Фаза 11 плана закрытия API-пробелов: sandbox — до gatewayRegistry.resolve,
        // ни contextStore.fetch (не нужен, submit не строится), ни
        // idempotencyStore.claim, ни submitClient.submit не вызываются — оба
        // операторских шлюза структурно недостижимы для sandbox-трафика.
        // Синтетический успех + синтетический DLR публикуются тут же, одним
        // branch'ем (см. DeliveryService.buildSandboxDeliveryStatusEvent).
        if (command.getSandbox()) {
            SubmitOutcome sandboxOutcome = new SubmitOutcome(Outcome.OUTCOME_SUCCEEDED, "", "SANDBOX-" + deterministicQueueMsgId);
            StageCompletedEvent event = DeliveryService.buildEvent(command, deterministicQueueMsgId, sandboxOutcome);
            producer.send(new ProducerRecord<>(OUTPUT_TOPIC, event.getMessageId(), event.toByteArray()))
                .get(PRODUCER_SEND_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

            DeliveryStatusEvent dlrEvent = DeliveryService.buildSandboxDeliveryStatusEvent(command, extension, Instant.now());
            producer.send(new ProducerRecord<>(DELIVERY_STATUS_TOPIC, dlrEvent.getMessageId(), dlrEvent.toByteArray()))
                .get(PRODUCER_SEND_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            return;
        }

        MessageContext context = contextStore.fetch(command.getMessageId());
        if (context == null) {
            // HIGH находка кодревью: раньше это был throw, который блокировал
            // партицию НАВСЕГДА, если msgctx реально никогда не появится
            // (TTL-эвикция/несогласованность), в отличие от gateway-not-found
            // ниже, который корректно публикует stage.completed с UNKNOWN.
            // command уже успешно распарсен — message_id/stage_execution_id
            // валидны, StageCompletedEvent построить можно; нет причины
            // блокировать всю партицию ради одной записи с недостающим
            // контекстом, если можно завершить её тем же путём, что и любой
            // другой неопределённый исход submit'а.
            LOG.warning("stage_execution_id=" + stageExecutionId + " MessageContext не найден для "
                + command.getMessageId() + ", завершаем как SUBMISSION_OUTCOME_UNKNOWN вместо блокировки партиции");
            SubmitOutcome outcome = DeliveryService.handleGrpcFailure("MESSAGE_CONTEXT_NOT_FOUND");
            StageCompletedEvent event = DeliveryService.buildEvent(command, deterministicQueueMsgId, outcome);
            sendCompletedEvent(producer, event);
            return;
        }

        GatewayEndpoint endpoint = gatewayRegistry.resolve(extension.getResolvedOperatorId(), extension.getRouteId());

        SubmitOutcome outcome;
        String queueMsgId;
        if (endpoint == null) {
            // Registry-запись отсутствует (владеющая реплика ещё не
            // зарегистрировалась/сдохла без переизбрания) — тот же исход,
            // что неопределённый результат submit, не считается FAILED
            // (нельзя утверждать, что оператор отклонил бы сообщение).
            // Нет реального side effect — claim через идемпотентный store не
            // нужен, безопасно ретраится сколько угодно раз.
            queueMsgId = deterministicQueueMsgId;
            outcome = DeliveryService.handleGrpcFailure("GATEWAY_INSTANCE_NOT_FOUND");
        } else {
            SubmitIdempotencyStore.ClaimResult claim = idempotencyStore.claim(stageExecutionId, deterministicQueueMsgId);
            if (claim instanceof SubmitIdempotencyStore.ClaimResult.AlreadyDone already) {
                // Реальный submit уже состоялся при более ранней попытке
                // (эта — редеставка после сбоя публикации stage.completed) —
                // повторный вызов submitClient.submit(...) исключён, исход
                // переиспользуется как есть.
                queueMsgId = already.queueMsgId();
                outcome = already.outcome();
            } else if (claim instanceof SubmitIdempotencyStore.ClaimResult.AmbiguousInFlight ambiguous) {
                // Claim уже занят, но исход не записан — крэш между реальным
                // submit и записью результата. Нельзя утверждать, дошёл ли
                // submit до оператора: hld.md §21 явно запрещает
                // автоматический повторный submit при UNKNOWN, поэтому здесь
                // НЕ вызываем submitClient.submit(...) снова.
                queueMsgId = ambiguous.queueMsgId();
                outcome = DeliveryService.handleGrpcFailure("AMBIGUOUS_PRIOR_ATTEMPT_NOT_RESUBMITTED");
                idempotencyStore.recordOutcome(stageExecutionId, outcome);
            } else {
                SubmitIdempotencyStore.ClaimResult.Won won = (SubmitIdempotencyStore.ClaimResult.Won) claim;
                queueMsgId = won.queueMsgId();
                List<Segment> segments = SegmentMessage.segment(context.body(), context.encoding());
                SubmitRequest request = DeliveryService.buildSubmitRequest(command, extension, context, queueMsgId, segments);
                try {
                    SubmitResponse response = submitClient.submit(endpoint.endpoint(), request);
                    outcome = DeliveryService.interpretSubmitResult(response);
                } catch (io.grpc.StatusRuntimeException e) {
                    outcome = DeliveryService.handleGrpcFailure(e.getStatus().getCode().name());
                }
                idempotencyStore.recordOutcome(stageExecutionId, outcome);
            }
        }

        StageCompletedEvent event = DeliveryService.buildEvent(command, queueMsgId, outcome);
        sendCompletedEvent(producer, event);
    }

    /**
     * Общий helper для обоих мест публикации {@code stage.completed} в
     * {@link #processRecord} (путь MESSAGE_CONTEXT_NOT_FOUND и обычный путь).
     */
    private static void sendCompletedEvent(Producer<String, byte[]> producer, StageCompletedEvent event) throws Exception {
        producer.send(new ProducerRecord<>(OUTPUT_TOPIC, event.getMessageId(), event.toByteArray()))
            .get(PRODUCER_SEND_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }
}
