package uz.mpp.delivery;

import com.google.protobuf.ByteString;
import java.util.List;
import uz.mpp.delivery.MessageContextStore.MessageContext;
import uz.mpp.delivery.SegmentMessage.Segment;
import uz.mpp.platformcontracts.common.v1.DeliveryExtension;
import uz.mpp.platformcontracts.common.v1.DeliveryResult;
import uz.mpp.platformcontracts.common.v1.Outcome;
import uz.mpp.platformcontracts.common.v1.StageCompletedEvent;
import uz.mpp.platformcontracts.common.v1.StageExecuteCommand;
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
                .setDestinationAddress(context.msisdn());
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

    /** {@code publish_stage_completed} — построение события, без публикации. */
    public static StageCompletedEvent buildEvent(StageExecuteCommand command, String queueMsgId, SubmitOutcome outcome) {
        return StageCompletedEvent.newBuilder()
            .setEventId("evt-" + command.getStageExecutionId())
            .setMessageId(command.getMessageId())
            .setStageExecutionId(command.getStageExecutionId())
            .setAttempt(command.getAttempt())
            .setStageName(command.getStageName())
            .setOutcome(outcome.outcome())
            .setReasonCode(outcome.reasonCode() == null ? "" : outcome.reasonCode())
            .setRetryable(outcome.outcome() != Outcome.OUTCOME_SUCCEEDED)
            .setTraceparent(command.getTraceparent())
            .setDelivery(DeliveryResult.newBuilder().setQueueMsgId(queueMsgId))
            .build();
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
