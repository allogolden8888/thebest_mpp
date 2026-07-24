package uz.mpp.deliveryreconciliation.kafkaio;

import org.junit.jupiter.api.Test;
import uz.mpp.platformcontracts.common.v1.Outcome;
import uz.mpp.platformcontracts.common.v1.ReconciliationOutcome;
import uz.mpp.platformcontracts.common.v1.StageName;

import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StageCompletedBuilderTest {

    @Test
    void deliveryConfirmedMapsToSucceeded() {
        var event = StageCompletedBuilder.build(UUID.randomUUID(), UUID.randomUUID(), 1,
            ReconciliationOutcome.RECONCILIATION_OUTCOME_DELIVERY_CONFIRMED, Instant.now());
        assertEquals(Outcome.OUTCOME_SUCCEEDED, event.getOutcome());
        assertEquals(StageName.STAGE_NAME_DELIVERY_RECONCILIATION, event.getStageName());
    }

    @Test
    void deliveryFailedMapsToFailed() {
        var event = StageCompletedBuilder.build(UUID.randomUUID(), UUID.randomUUID(), 1,
            ReconciliationOutcome.RECONCILIATION_OUTCOME_DELIVERY_FAILED, Instant.now());
        assertEquals(Outcome.OUTCOME_FAILED, event.getOutcome());
        assertFalse(event.getRetryable());
    }

    @Test
    void confirmedNotSubmittedMapsToFailed() {
        var event = StageCompletedBuilder.build(UUID.randomUUID(), UUID.randomUUID(), 1,
            ReconciliationOutcome.RECONCILIATION_OUTCOME_CONFIRMED_NOT_SUBMITTED, Instant.now());
        assertEquals(Outcome.OUTCOME_FAILED, event.getOutcome());
    }

    @Test
    void deliveryUnresolvedMapsToDedicatedOutcome() {
        var event = StageCompletedBuilder.build(UUID.randomUUID(), UUID.randomUUID(), 1,
            ReconciliationOutcome.RECONCILIATION_OUTCOME_DELIVERY_UNRESOLVED, Instant.now());
        assertEquals(Outcome.OUTCOME_DELIVERY_UNRESOLVED, event.getOutcome());
    }

    @Test
    void unspecifiedOutcomeIsRejected() {
        assertThrows(IllegalArgumentException.class, () ->
            StageCompletedBuilder.build(UUID.randomUUID(), UUID.randomUUID(), 1,
                ReconciliationOutcome.RECONCILIATION_OUTCOME_UNSPECIFIED, Instant.now()));
    }

    @Test
    void reasonCodeCarriesReconciliationOutcomeName() {
        var event = StageCompletedBuilder.build(UUID.randomUUID(), UUID.randomUUID(), 1,
            ReconciliationOutcome.RECONCILIATION_OUTCOME_CONFIRMED_SUBMITTED, Instant.now());
        assertEquals("RECONCILIATION_OUTCOME_CONFIRMED_SUBMITTED", event.getReasonCode());
    }
}
