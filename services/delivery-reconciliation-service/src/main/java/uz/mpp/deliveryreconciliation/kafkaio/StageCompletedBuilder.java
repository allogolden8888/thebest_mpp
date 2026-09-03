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
            // partner_id НЕ проставляется здесь, и это не забытое поле:
            // эта стадия строит событие не из StageExecuteCommand, а из
            // reconciliation.reconciliation_cases (case_id/message_id/
            // stage_execution_id/operator_id/status/...), где partner_id
            // отсутствует как колонка. Чтобы заполнить его, нужна миграция
            // (добавить partner_id в таблицу и писать его при открытии
            // case'а) — отдельная правка, не прячется здесь молча.
            // Следствие: строки analytics.stage_events со stage_name=
            // DELIVERY_RECONCILIATION будут с пустым partner_id и не
            // попадут в разбивку отчётов по партнёру. Путь редкий
            // (срабатывает только когда DLR не пришёл в SLA).
            .setDeliveryReconciliation(DeliveryReconciliationResult.newBuilder().setReconciliationOutcome(outcome).build())
            .build();
    }

    private static Timestamp toTimestamp(Instant instant) {
        return Timestamp.newBuilder().setSeconds(instant.getEpochSecond()).setNanos(instant.getNano()).build();
    }
}