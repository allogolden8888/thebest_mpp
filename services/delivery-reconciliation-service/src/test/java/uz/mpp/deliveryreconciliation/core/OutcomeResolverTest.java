package uz.mpp.deliveryreconciliation.core;

import org.junit.jupiter.api.Test;
import uz.mpp.platformcontracts.common.v1.ReconciliationOutcome;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class OutcomeResolverTest {

    @Test
    void deliverySuccessAlwaysWinsRegardlessOfDeadline() {
        Evidence evidence = Evidence.empty().withDeliveryStatus(Evidence.DeliveryOutcome.SUCCESS);
        assertEquals(ReconciliationOutcome.RECONCILIATION_OUTCOME_DELIVERY_CONFIRMED,
            OutcomeResolver.resolve(evidence, OutcomeResolver.DeadlineState.CONTINUE));
    }

    @Test
    void deliveryFailureAlwaysWinsRegardlessOfDeadline() {
        Evidence evidence = Evidence.empty().withDeliveryStatus(Evidence.DeliveryOutcome.FAILURE);
        assertEquals(ReconciliationOutcome.RECONCILIATION_OUTCOME_DELIVERY_FAILED,
            OutcomeResolver.resolve(evidence, OutcomeResolver.DeadlineState.CONTINUE));
    }

    @Test
    void querySmConfirmedDeliveredResolvesEarly() {
        Evidence evidence = Evidence.empty().withQuerySm(Evidence.QuerySmOutcome.CONFIRMED_DELIVERED);
        assertEquals(ReconciliationOutcome.RECONCILIATION_OUTCOME_DELIVERY_CONFIRMED,
            OutcomeResolver.resolve(evidence, OutcomeResolver.DeadlineState.CONTINUE));
    }

    @Test
    void querySmConfirmedNotFoundResolvesEarly() {
        Evidence evidence = Evidence.empty().withQuerySm(Evidence.QuerySmOutcome.CONFIRMED_NOT_FOUND);
        assertEquals(ReconciliationOutcome.RECONCILIATION_OUTCOME_CONFIRMED_NOT_SUBMITTED,
            OutcomeResolver.resolve(evidence, OutcomeResolver.DeadlineState.CONTINUE));
    }

    @Test
    void noEvidenceYetContinuesBeforeDeadline() {
        assertNull(OutcomeResolver.resolve(Evidence.empty(), OutcomeResolver.DeadlineState.CONTINUE));
    }

    @Test
    void submitAcceptedWithoutDlrAtDeadlineIsConfirmedSubmitted() {
        Evidence evidence = Evidence.empty().withSubmitAccepted();
        assertEquals(ReconciliationOutcome.RECONCILIATION_OUTCOME_CONFIRMED_SUBMITTED,
            OutcomeResolver.resolve(evidence, OutcomeResolver.DeadlineState.EXPIRED));
    }

    @Test
    void noEvidenceAtAllAtDeadlineIsConfirmedNotSubmitted() {
        assertEquals(ReconciliationOutcome.RECONCILIATION_OUTCOME_CONFIRMED_NOT_SUBMITTED,
            OutcomeResolver.resolve(Evidence.empty(), OutcomeResolver.DeadlineState.EXPIRED));
    }

    @Test
    void inconclusiveQuerySmAtDeadlineIsDeliveryUnresolved() {
        Evidence evidence = Evidence.empty().withSubmitAccepted().withQuerySm(Evidence.QuerySmOutcome.INCONCLUSIVE);
        assertEquals(ReconciliationOutcome.RECONCILIATION_OUTCOME_DELIVERY_UNRESOLVED,
            OutcomeResolver.resolve(evidence, OutcomeResolver.DeadlineState.EXPIRED));
    }
}
