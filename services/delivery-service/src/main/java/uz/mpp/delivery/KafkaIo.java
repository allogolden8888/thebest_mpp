package uz.mpp.delivery;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
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
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import uz.mpp.delivery.DeliveryService.SubmitOutcome;
import uz.mpp.delivery.GatewayRegistry.GatewayEndpoint;
import uz.mpp.delivery.MessageContextStore.MessageContext;
import uz.mpp.delivery.SegmentMessage.Segment;
import uz.mpp.platformcontracts.common.v1.DeliveryExtension;
import uz.mpp.platformcontracts.common.v1.StageCompletedEvent;
import uz.mpp.platformcontracts.common.v1.StageExecuteCommand;
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
    private static final Duration PRODUCER_SEND_TIMEOUT = Duration.ofSeconds(10);

    public static KafkaConsumer<String, byte[]> buildConsumer(String bootstrapServers, String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        return new KafkaConsumer<>(props);
    }

    public static KafkaProducer<String, byte[]> buildProducer(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        return new KafkaProducer<>(props);
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
        try {
            while (running.get()) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofSeconds(1));

                OffsetTracker tracker = new OffsetTracker();
                for (ConsumerRecord<String, byte[]> record : records) {
                    TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                    if (tracker.isSuspended(tp)) {
                        continue;
                    }
                    try {
                        processRecord(record, contextStore, gatewayRegistry, controlSnapshot, submitClient, idempotencyStore, producer);
                        tracker.recordSuccess(tp, record.offset());
                    } catch (Exception e) {
                        LOG.log(Level.SEVERE, "не удалось обработать stage.delivery запись partition=" + tp
                            + " offset=" + record.offset() + ", оффсет не коммитится, партишен приостановлен до следующего поллинга", e);
                        tracker.recordFailure(tp);
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

    private static void processRecord(
        ConsumerRecord<String, byte[]> record,
        MessageContextStore contextStore,
        GatewayRegistry gatewayRegistry,
        ControlSnapshot controlSnapshot,
        OperatorSubmitClient submitClient,
        SubmitIdempotencyStore idempotencyStore,
        KafkaProducer<String, byte[]> producer
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
            producer.send(new ProducerRecord<>(OUTPUT_TOPIC, event.getMessageId(), event.toByteArray()))
                .get(PRODUCER_SEND_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
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
        producer.send(new ProducerRecord<>(OUTPUT_TOPIC, event.getMessageId(), event.toByteArray()))
            .get(PRODUCER_SEND_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }
}
