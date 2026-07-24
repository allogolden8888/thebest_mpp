package uz.mpp.msr;

import java.util.Optional;
import uz.mpp.platformcontracts.common.v1.Outcome;
import uz.mpp.platformcontracts.common.v1.ReconciliationOutcome;
import uz.mpp.platformcontracts.common.v1.StageCompletedEvent;
import uz.mpp.platformcontracts.common.v1.StageName;
import uz.mpp.platformcontracts.events.v1.DeliveryStatusEvent;

/**
 * {@code on_stage_completed}/{@code on_delivery_status}
 * (service_internal_methods.md §2.4) — реальная находка при реализации: ни
 * один документ этой сессии (`hld.md` §10, `state_machines.md` §1,
 * `service_internal_methods.md` §2.4) не даёт явную таблицу
 * "stage_name+outcome → LifecycleStatus". `state_machines.md` формализует
 * только САМУ машину переходов (какие статусы куда могут вести), не откуда
 * эти статусы берутся из сырых событий. Ниже — построенная здесь таблица,
 * обоснование по каждой строке в комментариях, не угадано вслепую.
 */
public final class CandidateTransitionResolver {

    private CandidateTransitionResolver() {
    }

    /**
     * {@code retryable=true} — Scheduler ещё повторит эту стадию (типичный
     * пример — Billing {@code ACCOUNT_FROZEN}/{@code STALE_EPOCH},
     * `billing-service/BillingService.java::rejectedRetryable`) — исход
     * ещё не окончателен, публиковать партнёру рано: если бы мы применили
     * статус сейчас, а retry потом успел бы, пришлось бы либо молча
     * откатывать историю (запрещено state_machines.md), либо получить
     * REGRESSION на легитимный последующий переход. Правильный момент —
     * когда retry реально исчерпан ({@code RETRY_EXHAUSTED}), не раньше.
     */
    public static Optional<LifecycleStatus> fromStageCompleted(StageCompletedEvent event) {
        if (event.getRetryable()) {
            return Optional.empty();
        }

        StageName stage = event.getStageName();
        Outcome outcome = event.getOutcome();

        // BILLING — чисто внутренняя финансовая бухгалтерия с точки зрения
        // партнёра: SUCCEEDED (обычная либо BLOCKED-тариф после отклонения
        // Policy) никогда сам по себе не двигает lifecycle. Если сообщение
        // было отклонено Policy, REJECTED уже применился на предыдущем
        // stage.completed (POLICY) — Billing-событие для того же message_id
        // придёт позже и станет REGRESSION-но-безопасным no-op (терминальный
        // статус уже занят), что корректно, не потому что здесь есть особый
        // случай для BILLING.
        if (stage == StageName.STAGE_NAME_BILLING) {
            return Optional.empty();
        }

        // POLICY REJECTED (non-retryable, всегда так в policy-service —
        // banwords/blacklist/sender validation/spam/time-of-day, ни один
        // путь не retryable) — партнёр должен узнать: отклонено до
        // отправки оператору. Единственный явный пример из
        // state_machines.md/hld.md ("REJECTED — Policy отклонила сообщение
        // до отправки оператору").
        if (stage == StageName.STAGE_NAME_POLICY && outcome == Outcome.OUTCOME_REJECTED) {
            return Optional.of(LifecycleStatus.REJECTED);
        }

        // DELIVERY — единственная стадия, чей SUCCEEDED партнёр-видим:
        // оператор ПРИНЯЛ submit, доставка ещё не подтверждена — это и есть
        // "SUBMITTED" по определению статуса ("сообщение принято, ожидает
        // исхода доставки").
        if (stage == StageName.STAGE_NAME_DELIVERY) {
            switch (outcome) {
                case OUTCOME_SUCCEEDED:
                    return Optional.of(LifecycleStatus.SUBMITTED);
                // Оператор СИНХРОННО и ОКОНЧАТЕЛЬНО отклонил submit_sm/HTTP
                // submit (напр. невалидный destination) — подтверждённый
                // отказ на этапе submit, ближе по смыслу к UNDELIVERABLE
                // ("подтверждён отказ доставки"), чем к FAILED
                // ("внутренняя ошибка пайплайна") — отказ пришёл ОТ
                // оператора, не из-за бага/сбоя нашей системы.
                case OUTCOME_FAILED:
                    return Optional.of(LifecycleStatus.UNDELIVERABLE);
                // AMBIGUOUS/SUBMISSION_OUTCOME_UNKNOWN — граф пайплайна
                // (pipeline.valid.json) направляет это в DELIVERY_RECONCILIATION
                // следующим шагом, не терминально здесь — реконсиляция даст
                // окончательный статус своим отдельным stage.completed.
                case OUTCOME_SUBMISSION_OUTCOME_UNKNOWN:
                    return Optional.empty();
                // TIMED_OUT/RETRY_EXHAUSTED на стадии DELIVERY — оператор/
                // канал не отвечал достаточно долго, чтобы Scheduler
                // исчерпал попытки — это и есть SYSTEM_UNAVAILABLE
                // ("оператор/канал недоступен на момент попытки"), не
                // общий внутренний FAILED.
                case OUTCOME_TIMED_OUT:
                case OUTCOME_RETRY_EXHAUSTED:
                    return Optional.of(LifecycleStatus.SYSTEM_UNAVAILABLE);
                default:
                    return Optional.empty();
            }
        }

        if (stage == StageName.STAGE_NAME_DELIVERY_RECONCILIATION) {
            return fromReconciliationOutcome(event);
        }

        // DESTINATION_RESOLUTION/ROUTING — внутренние технические стадии;
        // их SUCCEEDED никогда партнёр-видим. Их REJECTED/FAILED (не
        // операторская, а системная невозможность обработать: нерезолвимый
        // msisdn, NO_HEALTHY_ROUTE) — внутренняя ошибка пайплайна, не
        // подтверждённый отказ оператора и не блокировка Policy — FAILED.
        if ((stage == StageName.STAGE_NAME_DESTINATION_RESOLUTION || stage == StageName.STAGE_NAME_ROUTING)
            && (outcome == Outcome.OUTCOME_REJECTED || outcome == Outcome.OUTCOME_FAILED
            || outcome == Outcome.OUTCOME_TIMED_OUT || outcome == Outcome.OUTCOME_RETRY_EXHAUSTED)) {
            return Optional.of(LifecycleStatus.FAILED);
        }

        return Optional.empty();
    }

    /**
     * {@code ReconciliationOutcome} спроектирован так, что его значения уже
     * почти дословно совпадают с именами {@link LifecycleStatus}
     * (DELIVERY_CONFIRMED/DELIVERY_FAILED/DELIVERY_UNRESOLVED) — сильное,
     * не притянутое обоснование для этого маппинга, не совпадение.
     */
    private static Optional<LifecycleStatus> fromReconciliationOutcome(StageCompletedEvent event) {
        if (!event.hasDeliveryReconciliation()) {
            return Optional.empty();
        }
        ReconciliationOutcome outcome = event.getDeliveryReconciliation().getReconciliationOutcome();
        return switch (outcome) {
            case RECONCILIATION_OUTCOME_CONFIRMED_SUBMITTED -> Optional.of(LifecycleStatus.SUBMITTED);
            case RECONCILIATION_OUTCOME_CONFIRMED_NOT_SUBMITTED -> Optional.of(LifecycleStatus.UNDELIVERABLE);
            case RECONCILIATION_OUTCOME_DELIVERY_CONFIRMED -> Optional.of(LifecycleStatus.DELIVERED);
            case RECONCILIATION_OUTCOME_DELIVERY_FAILED -> Optional.of(LifecycleStatus.UNDELIVERABLE);
            case RECONCILIATION_OUTCOME_DELIVERY_UNRESOLVED -> Optional.of(LifecycleStatus.DELIVERY_UNRESOLVED);
            default -> Optional.empty();
        };
    }

    /**
     * {@code delivery.status} — {@code normalized_status} уже несёт ровно
     * то же словарное значение, что {@link LifecycleStatus} (by design:
     * {@code dlr-manager/internal/dlr/parser.go::NormalizeStatus} эмитит
     * буквально строки "DELIVERED"/"UNDELIVERABLE") — прямой парсинг
     * строки в enum, не отдельная таблица.
     */
    public static Optional<LifecycleStatus> fromDeliveryStatus(DeliveryStatusEvent event) {
        try {
            return Optional.of(LifecycleStatus.valueOf(event.getNormalizedStatus()));
        } catch (IllegalArgumentException e) {
            return Optional.empty();
        }
    }
}
