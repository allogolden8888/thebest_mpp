package uz.mpp.msr;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Optional;
import org.junit.jupiter.api.Test;
import uz.mpp.platformcontracts.common.v1.DeliveryReconciliationResult;
import uz.mpp.platformcontracts.common.v1.Outcome;
import uz.mpp.platformcontracts.common.v1.ReconciliationOutcome;
import uz.mpp.platformcontracts.common.v1.StageCompletedEvent;
import uz.mpp.platformcontracts.common.v1.StageName;
import uz.mpp.platformcontracts.events.v1.DeliveryStatusEvent;

class CandidateTransitionResolverTest {

    private StageCompletedEvent.Builder event(StageName stage, Outcome outcome) {
        return StageCompletedEvent.newBuilder()
            .setEventId("e1")
            .setMessageId("m1")
            .setStageName(stage)
            .setOutcome(outcome)
            .setRetryable(false);
    }

    @Test
    void policyRejectedMapsToRejected() {
        var event = event(StageName.STAGE_NAME_POLICY, Outcome.OUTCOME_REJECTED).build();
        assertEquals(Optional.of(LifecycleStatus.REJECTED), CandidateTransitionResolver.fromStageCompleted(event));
    }

    @Test
    void billingNeverProducesCandidateRegardlessOfOutcome() {
        for (Outcome outcome : Outcome.values()) {
            if (outcome == Outcome.UNRECOGNIZED) {
                continue; // protobuf-синтетическое значение, не устанавливаемо через setOutcome
            }
            var event = event(StageName.STAGE_NAME_BILLING, outcome).build();
            assertEquals(Optional.empty(), CandidateTransitionResolver.fromStageCompleted(event),
                "BILLING outcome=" + outcome + " не должен производить lifecycle-переход");
        }
    }

    @Test
    void retryableEventNeverProducesCandidateRegardlessOfStage() {
        // Billing ACCOUNT_FROZEN/STALE_EPOCH — retryable=true — Scheduler ещё повторит.
        var event = event(StageName.STAGE_NAME_POLICY, Outcome.OUTCOME_REJECTED).setRetryable(true).build();
        assertEquals(Optional.empty(), CandidateTransitionResolver.fromStageCompleted(event));
    }

    @Test
    void deliverySucceededMapsToSubmitted() {
        var event = event(StageName.STAGE_NAME_DELIVERY, Outcome.OUTCOME_SUCCEEDED).build();
        assertEquals(Optional.of(LifecycleStatus.SUBMITTED), CandidateTransitionResolver.fromStageCompleted(event));
    }

    @Test
    void deliveryFailedMapsToUndeliverable() {
        var event = event(StageName.STAGE_NAME_DELIVERY, Outcome.OUTCOME_FAILED).build();
        assertEquals(Optional.of(LifecycleStatus.UNDELIVERABLE), CandidateTransitionResolver.fromStageCompleted(event));
    }

    @Test
    void deliveryAmbiguousProducesNoCandidateYet() {
        var event = event(StageName.STAGE_NAME_DELIVERY, Outcome.OUTCOME_SUBMISSION_OUTCOME_UNKNOWN).build();
        assertEquals(Optional.empty(), CandidateTransitionResolver.fromStageCompleted(event));
    }

    @Test
    void deliveryTimedOutMapsToSystemUnavailable() {
        var event = event(StageName.STAGE_NAME_DELIVERY, Outcome.OUTCOME_TIMED_OUT).build();
        assertEquals(Optional.of(LifecycleStatus.SYSTEM_UNAVAILABLE), CandidateTransitionResolver.fromStageCompleted(event));
    }

    @Test
    void destinationResolutionRejectedMapsToFailed() {
        var event = event(StageName.STAGE_NAME_DESTINATION_RESOLUTION, Outcome.OUTCOME_REJECTED).build();
        assertEquals(Optional.of(LifecycleStatus.FAILED), CandidateTransitionResolver.fromStageCompleted(event));
    }

    @Test
    void routingFailedMapsToFailed() {
        var event = event(StageName.STAGE_NAME_ROUTING, Outcome.OUTCOME_FAILED).build();
        assertEquals(Optional.of(LifecycleStatus.FAILED), CandidateTransitionResolver.fromStageCompleted(event));
    }

    @Test
    void reconciliationOutcomesMapOneToOne() {
        assertEquals(Optional.of(LifecycleStatus.SUBMITTED), reconciliation(ReconciliationOutcome.RECONCILIATION_OUTCOME_CONFIRMED_SUBMITTED));
        assertEquals(Optional.of(LifecycleStatus.UNDELIVERABLE), reconciliation(ReconciliationOutcome.RECONCILIATION_OUTCOME_CONFIRMED_NOT_SUBMITTED));
        assertEquals(Optional.of(LifecycleStatus.DELIVERED), reconciliation(ReconciliationOutcome.RECONCILIATION_OUTCOME_DELIVERY_CONFIRMED));
        assertEquals(Optional.of(LifecycleStatus.UNDELIVERABLE), reconciliation(ReconciliationOutcome.RECONCILIATION_OUTCOME_DELIVERY_FAILED));
        assertEquals(Optional.of(LifecycleStatus.DELIVERY_UNRESOLVED), reconciliation(ReconciliationOutcome.RECONCILIATION_OUTCOME_DELIVERY_UNRESOLVED));
    }

    private Optional<LifecycleStatus> reconciliation(ReconciliationOutcome outcome) {
        var event = event(StageName.STAGE_NAME_DELIVERY_RECONCILIATION, Outcome.OUTCOME_SUCCEEDED)
            .setDeliveryReconciliation(DeliveryReconciliationResult.newBuilder().setReconciliationOutcome(outcome))
            .build();
        return CandidateTransitionResolver.fromStageCompleted(event);
    }

    @Test
    void deliveryStatusNormalizedStatusParsesDirectlyToEnum() {
        var event = DeliveryStatusEvent.newBuilder().setEventId("e1").setMessageId("m1").setNormalizedStatus("DELIVERED").build();
        assertEquals(Optional.of(LifecycleStatus.DELIVERED), CandidateTransitionResolver.fromDeliveryStatus(event));
    }

    @Test
    void deliveryStatusUnrecognizedStringReturnsEmpty() {
        var event = DeliveryStatusEvent.newBuilder().setEventId("e1").setMessageId("m1").setNormalizedStatus("GARBAGE").build();
        assertEquals(Optional.empty(), CandidateTransitionResolver.fromDeliveryStatus(event));
    }
}
