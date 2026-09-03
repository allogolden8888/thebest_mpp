package uz.mpp.delivery;

import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import uz.mpp.delivery.MessageContextStore.MessageContext;
import uz.mpp.delivery.SegmentMessage.Segment;
import uz.mpp.platformcontracts.common.v1.DeliveryExtension;
import uz.mpp.platformcontracts.common.v1.DeliveryResult;
import uz.mpp.platformcontracts.common.v1.Outcome;
import uz.mpp.platformcontracts.common.v1.StageCompletedEvent;
import uz.mpp.platformcontracts.common.v1.StageExecuteCommand;
import uz.mpp.platformcontracts.events.v1.DeliveryStatusEvent;
import uz.mpp.platformcontracts.grpc.v1.MessageSegment;
import uz.mpp.platformcontracts.grpc.v1.SubmitOutcomeStatus;
import uz.mpp.platformcontracts.grpc.v1.SubmitRequest;
import uz.mpp.platformcontracts.grpc.v1.SubmitResponse;

/**
 * Оркестрация {@code handle_delivery_execute} (service_internal_methods.md
 * §1.8) — чистые функции, тестируются без сети/Redis/gRPC. Реальный I/O
 * (Redis fetch, gRPC call, Kafka publish) — в {@code KafkaIo}/{@code Main}.
 */
public final class DeliveryService {

    private DeliveryService() {
    }

    public record SubmitOutcome(Outcome outcome, String reasonCode, String smscMessageId) {
    }

    /** {@code interpret_submit_result} */
    public static SubmitOutcome interpretSubmitResult(SubmitResponse response) {
        return switch (response.getStatus()) {
            case SUBMIT_OUTCOME_STATUS_ACCEPTED ->
                new SubmitOutcome(Outcome.OUTCOME_SUCCEEDED, "", response.getSmscMessageId());
            case SUBMIT_OUTCOME_STATUS_REJECTED ->
                new SubmitOutcome(Outcome.OUTCOME_FAILED, response.getReasonCode(), "");
            case SUBMIT_OUTCOME_STATUS_AMBIGUOUS, SUBMIT_OUTCOME_STATUS_UNSPECIFIED, UNRECOGNIZED ->
                new SubmitOutcome(Outcome.OUTCOME_SUBMISSION_OUTCOME_UNKNOWN, response.getReasonCode(), "");
        };
    }

    /**
     * gRPC-вызов сам по себе не удался (network error/timeout/`UNAVAILABLE`)
     * — нельзя утверждать ни accepted, ни rejected, оператор мог реально
     * принять submit до обрыва ответа. `SUBMISSION_OUTCOME_UNKNOWN`, тот же
     * исход, что `AMBIGUOUS` — HLD §13 явно резервирует эту категорию именно
     * под "не удалось подтвердить исход", включая транспортные сбои.
     */
    public static SubmitOutcome handleGrpcFailure(String reason) {
        return new SubmitOutcome(Outcome.OUTCOME_SUBMISSION_OUTCOME_UNKNOWN, reason, "");
    }

    /** {@code call_submit} — подготовка запроса, без самого вызова. */
    public static SubmitRequest buildSubmitRequest(
        StageExecuteCommand command,
        DeliveryExtension extension,
        MessageContext context,
        String queueMsgId,
        List<Segment> segments) {
        SubmitRequest.Builder builder =
            SubmitRequest.newBuilder()
                .setMessageId(command.getMessageId())
                .setStageExecutionId(command.getStageExecutionId())
                .setQueueMsgId(queueMsgId)
                .setOperatorId(extension.getResolvedOperatorId())
                .setRouteId(extension.getRouteId())
                .setDestinationAddress(context.msisdn())
                // Приоритет — partner-supplied (REST API), разрешён ещё на
                // ingestion в partner-rest-receiver (там же и дефолт для
                // непомеченного трафика). Здесь чистый passthrough до самой
                // SMPP-трубы — тарификация по приоритету осознанно не входит
                // в этот срез.
                .setPriorityFlag(extension.getPriorityFlag());
        for (Segment segment : segments) {
            builder.addSegments(
                MessageSegment.newBuilder()
                    .setSegmentId(segment.segmentId())
                    .setContent(ByteString.copyFrom(segment.content()))
                    .setEncoding(segment.encoding()));
        }
        if (command.hasDeadline()) {
            builder.setDeadline(command.getDeadline());
        }
        return builder.build();
    }

    /**
     * Причины, по которым имеет смысл повторить попытку — временная нехватка
     * ёмкости/окна, а не настоящий отказ. До сих пор {@code retryable}
     * значило "outcome != SUCCEEDED", то есть true для абсолютно любой
     * неудачи, включая настоящие SMPP-отклонения — поле при этом нигде не
     * читалось (pipeline-engine его игнорировал), так что мёртвые данные не
     * вредили. pipeline-engine параллельно дорабатывается, чтобы ретраить
     * DELIVERY до истечения {@code message_ttl} именно по этому полю —
     * значит разметка должна стать честной: бесконечный ретрай настоящего
     * permanent-отказа (реальный SMPP reject от оператора) хуже, чем один
     * раз не отретраить пограничный transient-случай, поэтому неизвестный
     * reasonCode по умолчанию считается permanent, а не retryable.
     *
     * <p>Retryable (transient/capacity):
     * <ul>
     *   <li>{@code TPS_THROTTLED} — legacy-причина отказа TokenBucket в
     *       operator-smpp-session-manager, может ещё недолго встречаться
     *       параллельно с новым HTB-пейсером</li>
     *   <li>{@code PACER_QUEUE_FULL}/{@code PACER_QUEUE_TIMEOUT} — safety
     *       valve нового приоритетного пейсера (переполнение очереди тира /
     *       превышение максимального ожидания в ней) — чистая нехватка
     *       ёмкости трубы, не отказ оператора</li>
     *   <li>{@code SUBMIT_TIMEOUT} — OperatorSubmitServer не дождался ответа
     *       SMSC на {@code submit_sm} за {@code SUBMIT_TIMEOUT_MS}; исход
     *       {@code AMBIGUOUS}, не факт, что оператор реально отклонил</li>
     *   <li>{@code GATEWAY_INSTANCE_NOT_FOUND} — GatewayRegistry ещё не
     *       перерегистрировался/переизбрание в процессе (см. KafkaIo); нет
     *       реального side effect, ретраится сколько угодно раз безопасно</li>
     *   <li>{@code AMBIGUOUS_PRIOR_ATTEMPT_NOT_RESUBMITTED} — крэш между
     *       реальным submit и записью исхода (см. KafkaIo); неизвестно,
     *       дошёл ли submit, но сама причина не запрещает попытку позже</li>
     * </ul>
     *
     * <p>Permanent (не retryable):
     * <ul>
     *   <li>{@code SMPP_STATUS_0x...} — настоящий SMPP-level reject
     *       оператора (см. {@code OperatorSubmitServer})</li>
     *   <li>{@code MESSAGE_CONTEXT_NOT_FOUND} — данных в Redis нет (см.
     *       KafkaIo), повторный submit их не создаст</li>
     *   <li>имена {@code io.grpc.Status.Code} (например
     *       {@code UNAVAILABLE}/{@code DEADLINE_EXCEEDED}, см.
     *       {@code handleGrpcFailure} в KafkaIo) — на gRPC-транспортном
     *       уровне это чаще реальная конфиг/роутинг проблема, а не
     *       временная нехватка ёмкости пейсера</li>
     *   <li>всё остальное неизвестное — permanent по умолчанию (см. выше
     *       про консервативность)</li>
     * </ul>
     */
    private static final Set<String> RETRYABLE_REASON_CODES = Set.of(
        "TPS_THROTTLED",
        "PACER_QUEUE_FULL",
        "PACER_QUEUE_TIMEOUT",
        "SUBMIT_TIMEOUT",
        "GATEWAY_INSTANCE_NOT_FOUND",
        "AMBIGUOUS_PRIOR_ATTEMPT_NOT_RESUBMITTED"
    );

    /**
     * Пакетная (не {@code private}) видимость намеренно — чистая функция,
     * покрывается табличным тестом напрямую из {@code DeliveryServiceTest}
     * без похода через gRPC/Kafka.
     */
    static boolean isRetryable(String reasonCode) {
        if (reasonCode == null) {
            return false;
        }
        if (reasonCode.startsWith("SMPP_STATUS_0x")) {
            return false;
        }
        return RETRYABLE_REASON_CODES.contains(reasonCode);
    }

    /** {@code publish_stage_completed} — построение события, без публикации. */
    public static StageCompletedEvent buildEvent(StageExecuteCommand command, String queueMsgId, SubmitOutcome outcome) {
        boolean retryable = outcome.outcome() != Outcome.OUTCOME_SUCCEEDED && isRetryable(outcome.reasonCode());
        return StageCompletedEvent.newBuilder()
            .setEventId("evt-" + command.getStageExecutionId())
            .setMessageId(command.getMessageId())
            .setStageExecutionId(command.getStageExecutionId())
            .setAttempt(command.getAttempt())
            .setStageName(command.getStageName())
            .setOutcome(outcome.outcome())
            .setReasonCode(outcome.reasonCode() == null ? "" : outcome.reasonCode())
            .setRetryable(retryable)
            .setTraceparent(command.getTraceparent())
            // Фаза 11 плана закрытия API-пробелов: эхо command.sandbox.
            .setSandbox(command.getSandbox())
            // Эхо command.partner_id — см. BillingService.baseEventBuilder.
            .setPartnerId(command.getPartnerId())
            // completed_at раньше не заполнялся ни одной стадией — из-за
            // этого пер-стадийные длительности в аналитике были нулевыми.
            .setCompletedAt(nowTimestamp())
            .setDelivery(DeliveryResult.newBuilder().setQueueMsgId(queueMsgId))
            .build();
    }

    private static com.google.protobuf.Timestamp nowTimestamp() {
        java.time.Instant now = java.time.Instant.now();
        return com.google.protobuf.Timestamp.newBuilder()
            .setSeconds(now.getEpochSecond())
            .setNanos(now.getNano())
            .build();
    }

    /**
     * Фаза 11 плана закрытия API-пробелов: синтетический DLR для
     * sandbox-сообщений — публикуется delivery-service напрямую на
     * {@code delivery.status} (тот же топик, что использует dlr-manager для
     * настоящих DLR), в обход всей цепочки submit→correlate→DLR, которой
     * dlr-manager/dlr-correlation-writer владеют для реального трафика.
     * {@code normalized_status = "DELIVERED"} — не произвольная строка,
     * {@code message-state-resolver} парсит её как литерал {@code LifecycleStatus}
     * (см. {@code CandidateTransitionResolver.fromDeliveryStatus}), так что
     * значение обязано совпадать буквально с существующим словарём.
     */
    public static DeliveryStatusEvent buildSandboxDeliveryStatusEvent(StageExecuteCommand command, DeliveryExtension extension, Instant now) {
        return DeliveryStatusEvent.newBuilder()
            .setEventId("evt-sandbox-dlr-" + command.getStageExecutionId())
            .setMessageId(command.getMessageId())
            .setOperatorId(extension.getResolvedOperatorId())
            .setNormalizedStatus("DELIVERED")
            .setRawOperatorStatus("SANDBOX_SYNTHETIC")
            .setOccurredAt(toTimestamp(now))
            .build();
    }

    private static Timestamp toTimestamp(Instant instant) {
        return Timestamp.newBuilder().setSeconds(instant.getEpochSecond()).setNanos(instant.getNano()).build();
    }

    /**
     * {@code check_control_state}/hold-путь — held-команда не строит
     * {@code SubmitRequest}, событие не публикуется здесь (Scheduler
     * Standard Lane решает, когда её отпустить — Delivery не ретраит сам).
     * Возвращает {@code true}, если можно продолжать submit.
     */
    public static boolean isAdmitted(ControlSnapshot.Decision decision) {
        return decision == ControlSnapshot.Decision.ADMIT;
    }
}
