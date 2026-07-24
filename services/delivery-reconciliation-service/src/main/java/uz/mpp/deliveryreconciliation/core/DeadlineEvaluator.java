package uz.mpp.deliveryreconciliation.core;

import java.time.Instant;

/** evaluate_deadline (service_internal_methods.md §1.9): ReconciliationCase, operator_specific_deadline -> Continue | Expire. */
public final class DeadlineEvaluator {

    private DeadlineEvaluator() {
    }

    public static OutcomeResolver.DeadlineState evaluate(Instant now, Instant deadlineAt) {
        return now.isBefore(deadlineAt) ? OutcomeResolver.DeadlineState.CONTINUE : OutcomeResolver.DeadlineState.EXPIRED;
    }
}