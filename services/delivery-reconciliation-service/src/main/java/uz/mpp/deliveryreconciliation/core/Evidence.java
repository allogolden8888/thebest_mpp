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

    /**
     * Объединение двух наборов свидетельств об ОДНОМ сообщении.
     *
     * <p>Нужно потому, что свидетельство теперь копится в двух местах:
     * в {@code reconciliation_cases.evidence} (когда case уже есть) и в
     * {@code reconciliation.early_evidence} (когда свидетельство обогнало
     * создание case'а — измеренная гонка, DLR приходит через 20–70 мс после
     * submit, см. migrations/V032). В момент создания case'а и при каждом
     * последующем свидетельстве обе половины сливаются здесь.
     *
     * <p>Слияние монотонно по каждому полю: «наблюдалось» никогда не
     * возвращается в «не наблюдалось», поэтому повторное применение того же
     * свидетельства (at-least-once Kafka) ничего не меняет —
     * {@code a.merge(b).merge(b) == a.merge(b)}. Именно эта идемпотентность
     * позволяет не бояться, что drain раннего evidence выполнится дважды
     * (двумя consumer-потоками или после передоставки).
     *
     * <p>Если оба набора несут РАЗНЫЕ терминальные значения одного поля
     * (напр. DLR SUCCESS в case'е и FAILURE в раннем) — побеждает {@code other}
     * (last-writer-wins). Такой конфликт означает два противоречивых DLR по
     * одному сообщению; выбор произвольный, но детерминированный, и он не
     * влияет на предмет находки: ни одно из значений не даёт
     * CONFIRMED_NOT_SUBMITTED (см. {@code OutcomeResolver.resolve}).
     */
    public Evidence merge(Evidence other) {
        return new Evidence(
            submitAcceptedObserved || other.submitAcceptedObserved,
            other.deliveryStatusObserved != DeliveryOutcome.NONE ? other.deliveryStatusObserved : deliveryStatusObserved,
            other.querySmObserved != QuerySmOutcome.NOT_CALLED ? other.querySmObserved : querySmObserved
        );
    }

    /**
     * Есть ли здесь хоть какое-то свидетельство. {@code false} — ровно тот
     * случай, ради которого дедлайн и существует: за всё окно реконсиляции
     * не пришло ничего.
     */
    public boolean isEmpty() {
        return equals(empty());
    }
}
