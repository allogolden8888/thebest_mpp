package uz.mpp.deliveryreconciliation.core;

/**
 * collect_evidence (service_internal_methods.md §1.9) — накопленные
 * свидетельства для одного {@code ReconciliationCase}, из
 * {@code operator.submit.accepted}, {@code delivery.status} и опционального
 * {@code query_sm}.
 */
public record Evidence(
    boolean submitAcceptedObserved,
    DeliveryOutcome deliveryStatusObserved,
    QuerySmOutcome querySmObserved
) {
    public enum DeliveryOutcome { NONE, SUCCESS, FAILURE }
    public enum QuerySmOutcome { NOT_CALLED, CONFIRMED_DELIVERED, CONFIRMED_NOT_FOUND, INCONCLUSIVE }

    public static Evidence empty() {
        return new Evidence(false, DeliveryOutcome.NONE, QuerySmOutcome.NOT_CALLED);
    }

    public Evidence withSubmitAccepted() {
        return new Evidence(true, deliveryStatusObserved, querySmObserved);
    }

    public Evidence withDeliveryStatus(DeliveryOutcome outcome) {
        return new Evidence(submitAcceptedObserved, outcome, querySmObserved);
    }

    public Evidence withQuerySm(QuerySmOutcome outcome) {
        return new Evidence(submitAcceptedObserved, deliveryStatusObserved, outcome);
    }
}
