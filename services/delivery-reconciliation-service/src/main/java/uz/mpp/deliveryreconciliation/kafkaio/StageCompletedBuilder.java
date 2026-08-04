package uz.mpp.deliveryreconciliation.kafkaio;

import com.google.protobuf.Timestamp;
import uz.mpp.platformcontracts.common.v1.*;

import java.time.Instant;
import java.util.UUID;

/** publish_stage_completed (service_internal_methods.md §1.9) — чистая сборка, без сети. */
public final class StageCompletedBuilder {

    private StageCompletedBuilder() {
    }

    public static StageCompletedEvent build(UUID messageId, UUID stageExecutionId, int attempt,
                                             ReconciliationOutcome outcome, Instant now) {
        // publish_stage_completed происходит только при закрытии case (resolve_outcome
        // вернул не-null) — SUBMISSION_OUTCOME_UNKNOWN сюда никогда не попадает,
        // это входной триггер этой стадии, не её исход.
        Outcome stageOutcome = switch (outcome) {
            case RECONCILIATION_OUTCOME_CONFIRMED_SUBMITTED, RECONCILIATION_OUTCOME_DELIVERY_CONFIRMED ->
                Outcome.OUTCOME_SUCCEEDED;
            case RECONCILIATION_OUTCOME_DELIVERY_FAILED, RECONCILIATION_OUTCOME_CONFIRMED_NOT_SUBMITTED ->
                Outcome.OUTCOME_FAILED;
            case RECONCILIATION_OUTCOME_DELIVERY_UNRESOLVED -> Outcome.OUTCOME_DELIVERY_UNRESOLVED;
            default -> throw new IllegalArgumentException("resolve_outcome не должен был вернуть " + outcome + " как финальный");
        };

        return StageCompletedEvent.newBuilder()
            .setEventId(UUID.randomUUID().toString())
            .setMessageId(messageId.toString())
            .setStageExecutionId(stageExecutionId.toString())
            .setAttempt(attempt)
            .setStageName(StageName.STAGE_NAME_DELIVERY_RECONCILIATION)
            .setOutcome(stageOutcome)
            .setReasonCode(outcome.name())
            .setRetryable(false)
            .setCompletedAt(toTimestamp(now))
            .setDeliveryReconciliation(DeliveryReconciliationResult.newBuilder().setReconciliationOutcome(outcome).build())
            .build();
    }

    private static Timestamp toTimestamp(Instant instant) {
        return Timestamp.newBuilder().setSeconds(instant.getEpochSecond()).setNanos(instant.getNano()).build();
    }
}