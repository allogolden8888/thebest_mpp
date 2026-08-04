package uz.mpp.deliveryreconciliation.core;

import uz.mpp.platformcontracts.common.v1.ReconciliationOutcome;

/**
 * resolve_outcome (service_internal_methods.md §1.9): накопленный Evidence
 * -&gt; один из 5 исходов ({@code common/enums.proto ReconciliationOutcome}).
 *
 * <p><b>Интерпретация, не буквально специфицированная в LLD</b> (задокументирована,
 * не скрыта — см. README "Интерпретация resolve_outcome"): триггер этого
 * сервиса всегда {@code SUBMISSION_OUTCOME_UNKNOWN}
 * (DeliveryReconciliationExtension докстринг в common/stage_contract.proto),
 * т.е. первичная неопределённость — дошёл ли submit до оператора вообще.
 * DLR — более сильное свидетельство, чем сам факт submit_accepted (DLR
 * невозможен без успешного submit), поэтому DLR всегда побеждает независимо
 * от того, наблюдался ли submit_accepted напрямую.
 */
public final class OutcomeResolver {

    private OutcomeResolver() {
    }

    public enum DeadlineState { CONTINUE, EXPIRED }

    /**
     * @return null, если резолюция ещё не готова (Continue, не дедлайн) —
     *         вызывающая сторона (persist_case) не должен закрывать case.
     */
    public static ReconciliationOutcome resolve(Evidence evidence, DeadlineState deadlineState) {
        // DLR — самое сильное свидетельство, побеждает независимо от прочего.
        if (evidence.deliveryStatusObserved() == Evidence.DeliveryOutcome.SUCCESS) {
            return ReconciliationOutcome.RECONCILIATION_OUTCOME_DELIVERY_CONFIRMED;
        }
        if (evidence.deliveryStatusObserved() == Evidence.DeliveryOutcome.FAILURE) {
            return ReconciliationOutcome.RECONCILIATION_OUTCOME_DELIVERY_FAILED;
        }

        // query_sm может напрямую подтвердить, что оператор доставил или не нашёл сообщение.
        if (evidence.querySmObserved() == Evidence.QuerySmOutcome.CONFIRMED_DELIVERED) {
            return ReconciliationOutcome.RECONCILIATION_OUTCOME_DELIVERY_CONFIRMED;
        }
        if (evidence.querySmObserved() == Evidence.QuerySmOutcome.CONFIRMED_NOT_FOUND) {
            return ReconciliationOutcome.RECONCILIATION_OUTCOME_CONFIRMED_NOT_SUBMITTED;
        }

        if (deadlineState == DeadlineState.CONTINUE) {
            return null; // ещё рано резолвить — case остаётся open
        }

        // Дедлайн наступил без окончательного DLR.
        if (evidence.querySmObserved() == Evidence.QuerySmOutcome.INCONCLUSIVE) {
            // Активно спросили оператора (query_sm), но ответ не дал
            // определённости — это худший случай: не просто "нет данных",
            // а "данные есть, но противоречивые/неполные".
            return ReconciliationOutcome.RECONCILIATION_OUTCOME_DELIVERY_UNRESOLVED;
        }
        if (evidence.submitAcceptedObserved()) {
            // Late submit_sm_resp подтвердил, что оператор получил сообщение —
            // первичная неопределённость (SUBMISSION_OUTCOME_UNKNOWN) разрешена
            // положительно; финальная доставка (DLR) может прийти позже отдельным
            // путём, не блокирует закрытие этого case.
            return ReconciliationOutcome.RECONCILIATION_OUTCOME_CONFIRMED_SUBMITTED;
        }
        // Ни submit_accepted, ни DLR, ни убедительный query_sm за весь
        // reconciliation window — считаем, что submit не дошёл (консервативно:
        // не списывать за недоставленное).
        return ReconciliationOutcome.RECONCILIATION_OUTCOME_CONFIRMED_NOT_SUBMITTED;
    }
}