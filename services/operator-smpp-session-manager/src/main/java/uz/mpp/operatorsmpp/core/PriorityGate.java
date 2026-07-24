package uz.mpp.operatorsmpp.core;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * enforce_query_sm_priority (service_internal_methods.md §1.3): submit_sm
 * имеет приоритет над query_sm — если сейчас есть outstanding submit-нагрузка
 * выше порога, query_sm откладывается (Defer), не Permit.
 */
public final class PriorityGate {

    private final int maxConcurrentSubmitsBeforeDeferringQuery;
    private final AtomicInteger inFlightSubmits = new AtomicInteger(0);

    public PriorityGate(int maxConcurrentSubmitsBeforeDeferringQuery) {
        this.maxConcurrentSubmitsBeforeDeferringQuery = maxConcurrentSubmitsBeforeDeferringQuery;
    }

    public void submitStarted() {
        inFlightSubmits.incrementAndGet();
    }

    public void submitFinished() {
        inFlightSubmits.updateAndGet(v -> Math.max(0, v - 1));
    }

    public enum QueryDecision { PERMIT, DEFER }

    public QueryDecision checkQuerySm() {
        return inFlightSubmits.get() >= maxConcurrentSubmitsBeforeDeferringQuery
            ? QueryDecision.DEFER
            : QueryDecision.PERMIT;
    }
}