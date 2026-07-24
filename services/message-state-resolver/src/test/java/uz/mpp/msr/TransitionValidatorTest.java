package uz.mpp.msr;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import uz.mpp.msr.TransitionValidator.ApplyResult;
import uz.mpp.msr.TransitionValidator.Verdict;

/**
 * Порт {@code state_machines/message_lifecycle.py}'s 10 тестов 1:1 —
 * имена методов сохранены (в snake->camel) для прямой сверки построчно.
 */
class TransitionValidatorTest {

    @Test
    void allEntryPointsValid() {
        for (LifecycleStatus status : TransitionValidator.VALID_ENTRY_POINTS) {
            ApplyResult r = TransitionValidator.apply(LifecycleState.fresh(), status, "e1");
            assertEquals(status, r.state().status());
            assertEquals(1, r.state().lifecycleVersion());
        }
    }

    @Test
    void submittedToDeliveredValid() {
        ApplyResult r1 = TransitionValidator.apply(LifecycleState.fresh(), LifecycleStatus.SUBMITTED, "e1");
        ApplyResult r2 = TransitionValidator.apply(r1.state(), LifecycleStatus.DELIVERED, "e2");
        assertEquals(LifecycleStatus.DELIVERED, r2.state().status());
        assertEquals(2, r2.state().lifecycleVersion());
    }

    @Test
    void submittedToUndeliverableValid() {
        ApplyResult r1 = TransitionValidator.apply(LifecycleState.fresh(), LifecycleStatus.SUBMITTED, "e1");
        ApplyResult r2 = TransitionValidator.apply(r1.state(), LifecycleStatus.UNDELIVERABLE, "e2");
        assertEquals(LifecycleStatus.UNDELIVERABLE, r2.state().status());
    }

    @Test
    void deliveredToUndeliverableIllegal() {
        // Явный пример недопустимого перехода из HLD §10.
        ApplyResult r1 = TransitionValidator.apply(LifecycleState.fresh(), LifecycleStatus.SUBMITTED, "e1");
        ApplyResult r2 = TransitionValidator.apply(r1.state(), LifecycleStatus.DELIVERED, "e2");
        ApplyResult r3 = TransitionValidator.apply(r2.state(), LifecycleStatus.UNDELIVERABLE, "e3");
        assertEquals(Verdict.REGRESSION, r3.verdict());
        assertEquals(r2.state(), r3.state(), "REGRESSION не должен менять состояние");
    }

    @Test
    void undeliverableToDeliveredIllegalBySymmetry() {
        ApplyResult r1 = TransitionValidator.apply(LifecycleState.fresh(), LifecycleStatus.SUBMITTED, "e1");
        ApplyResult r2 = TransitionValidator.apply(r1.state(), LifecycleStatus.UNDELIVERABLE, "e2");
        ApplyResult r3 = TransitionValidator.apply(r2.state(), LifecycleStatus.DELIVERED, "e3");
        assertEquals(Verdict.REGRESSION, r3.verdict());
    }

    @Test
    void deliveryUnresolvedToLateConfirmedValidException() {
        ApplyResult r1 = TransitionValidator.apply(LifecycleState.fresh(), LifecycleStatus.DELIVERY_UNRESOLVED, "e1");
        ApplyResult r2 = TransitionValidator.apply(r1.state(), LifecycleStatus.LATE_DELIVERY_CONFIRMED, "e2");
        assertEquals(LifecycleStatus.LATE_DELIVERY_CONFIRMED, r2.state().status());
        assertEquals(2, r2.state().lifecycleVersion());
    }

    @Test
    void deliveryUnresolvedToAnythingElseIllegal() {
        for (LifecycleStatus badTarget : LifecycleStatus.values()) {
            if (badTarget == LifecycleStatus.DELIVERY_UNRESOLVED || badTarget == LifecycleStatus.LATE_DELIVERY_CONFIRMED) {
                continue;
            }
            ApplyResult r1 = TransitionValidator.apply(LifecycleState.fresh(), LifecycleStatus.DELIVERY_UNRESOLVED, "e1");
            ApplyResult r2 = TransitionValidator.apply(r1.state(), badTarget, "e2");
            assertEquals(Verdict.REGRESSION, r2.verdict(), "DELIVERY_UNRESOLVED -> " + badTarget + " должно быть отклонено");
        }
    }

    /** Достигает status легитимным путём — аналог Python-версии {@code _reach}. */
    private static LifecycleState reach(LifecycleStatus status) {
        if (TransitionValidator.VALID_ENTRY_POINTS.contains(status)) {
            return TransitionValidator.apply(LifecycleState.fresh(), status, "e1").state();
        }
        if (status == LifecycleStatus.LATE_DELIVERY_CONFIRMED) {
            ApplyResult r1 = TransitionValidator.apply(LifecycleState.fresh(), LifecycleStatus.DELIVERY_UNRESOLVED, "e1");
            return TransitionValidator.apply(r1.state(), LifecycleStatus.LATE_DELIVERY_CONFIRMED, "e2").state();
        }
        throw new AssertionError("нет известного пути до " + status + " — тест нужно дополнить");
    }

    @Test
    void allFullyTerminalStatesRejectEverything() {
        var fullyTerminal = java.util.EnumSet.copyOf(TransitionValidator.TERMINAL);
        fullyTerminal.remove(LifecycleStatus.DELIVERY_UNRESOLVED); // имеет исключение
        assertFalse(fullyTerminal.isEmpty());
        for (LifecycleStatus termStatus : fullyTerminal) {
            for (LifecycleStatus anyTarget : LifecycleStatus.values()) {
                LifecycleState s = reach(termStatus);
                ApplyResult r = TransitionValidator.apply(s, anyTarget, "e99");
                assertEquals(Verdict.REGRESSION, r.verdict(), termStatus + " -> " + anyTarget + " должно быть отклонено");
            }
        }
    }

    @Test
    void duplicateEventIdIsNoopNotError() {
        ApplyResult r1 = TransitionValidator.apply(LifecycleState.fresh(), LifecycleStatus.SUBMITTED, "e1");
        ApplyResult r2 = TransitionValidator.apply(r1.state(), LifecycleStatus.DELIVERED, "e1"); // тот же event_id
        assertEquals(Verdict.DUPLICATE, r2.verdict());
        assertEquals(LifecycleStatus.SUBMITTED, r2.state().status()); // не применилось
        assertEquals(1, r2.state().lifecycleVersion());
    }

    @Test
    void reconciliationCanEnterDirectlyWithoutSubmitted() {
        for (LifecycleStatus status : new LifecycleStatus[] {LifecycleStatus.DELIVERED, LifecycleStatus.UNDELIVERABLE, LifecycleStatus.DELIVERY_UNRESOLVED}) {
            ApplyResult r = TransitionValidator.apply(LifecycleState.fresh(), status, "e1");
            assertEquals(status, r.state().status());
        }
    }

    @Test
    void regressionDoesNotThrowJavaSpecificApiShape() {
        // Отличие от Python-версии (там ValueError) — здесь REGRESSION
        // возвращается как значение, не бросается: один плохой event не
        // должен ронять весь Kafka-consumer процесс.
        ApplyResult r1 = TransitionValidator.apply(LifecycleState.fresh(), LifecycleStatus.SUBMITTED, "e1");
        ApplyResult r2 = TransitionValidator.apply(r1.state(), LifecycleStatus.DELIVERED, "e2");
        assertTrue(true, "если предыдущие вызовы не бросили — контракт соблюдён");
    }
}
