package uz.mpp.msr;

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

/**
 * Порт {@code state_machines/message_lifecycle.py} 1:1 — таблица переходов
 * это контракт (state_machines.md §1), не переизобретается здесь.
 * {@code validateTransition}/{@code apply} — те же имена и та же логика,
 * что в Python-версии, для прямой сверки построчно.
 */
public final class TransitionValidator {

    private TransitionValidator() {
    }

    public enum Verdict {
        VALID,
        REGRESSION,
        DUPLICATE
    }

    /** Терминальные статусы — не принимают дальнейших переходов, КРОМЕ
     * явно перечисленных исключений в {@link #ALLOWED_FROM_TERMINAL}. */
    public static final Set<LifecycleStatus> TERMINAL = EnumSet.of(
        LifecycleStatus.DELIVERED,
        LifecycleStatus.UNDELIVERABLE,
        LifecycleStatus.REJECTED,
        LifecycleStatus.FAILED,
        LifecycleStatus.SYSTEM_UNAVAILABLE,
        LifecycleStatus.DELIVERY_UNRESOLVED, // терминален, кроме одного исключения
        LifecycleStatus.LATE_DELIVERY_CONFIRMED
    );

    /** Допустимые записи для сообщения без текущего статуса — не всегда
     * SUBMITTED, Reconciliation может дать первый статус напрямую. */
    public static final Set<LifecycleStatus> VALID_ENTRY_POINTS = EnumSet.of(
        LifecycleStatus.SUBMITTED,
        LifecycleStatus.REJECTED,
        LifecycleStatus.FAILED,
        LifecycleStatus.SYSTEM_UNAVAILABLE,
        LifecycleStatus.DELIVERED,
        LifecycleStatus.UNDELIVERABLE,
        LifecycleStatus.DELIVERY_UNRESOLVED
    );

    private static final Map<LifecycleStatus, Set<LifecycleStatus>> ALLOWED_TRANSITIONS = new EnumMap<>(LifecycleStatus.class);
    static {
        ALLOWED_TRANSITIONS.put(LifecycleStatus.SUBMITTED, EnumSet.of(LifecycleStatus.DELIVERED, LifecycleStatus.UNDELIVERABLE));
    }

    /** Единственное исключение из "терминальное состояние = конец": поздний
     * DLR после DELIVERY_UNRESOLVED (HLD §10). Осознанно НЕ включает
     * DELIVERED/UNDELIVERABLE — противоречащий поздний DLR считается
     * аномалией оператора, логируется, но не применяется. */
    private static final Map<LifecycleStatus, Set<LifecycleStatus>> ALLOWED_FROM_TERMINAL = new EnumMap<>(LifecycleStatus.class);
    static {
        ALLOWED_FROM_TERMINAL.put(LifecycleStatus.DELIVERY_UNRESOLVED, EnumSet.of(LifecycleStatus.LATE_DELIVERY_CONFIRMED));
    }

    public static Verdict validateTransition(LifecycleState current, LifecycleStatus candidateStatus, String eventId) {
        if (eventId.equals(current.lastAppliedEventId())) {
            return Verdict.DUPLICATE;
        }
        if (current.status() == null) {
            return VALID_ENTRY_POINTS.contains(candidateStatus) ? Verdict.VALID : Verdict.REGRESSION;
        }
        if (TERMINAL.contains(current.status())) {
            Set<LifecycleStatus> allowed = ALLOWED_FROM_TERMINAL.getOrDefault(current.status(), Set.of());
            return allowed.contains(candidateStatus) ? Verdict.VALID : Verdict.REGRESSION;
        }
        Set<LifecycleStatus> allowed = ALLOWED_TRANSITIONS.getOrDefault(current.status(), Set.of());
        return allowed.contains(candidateStatus) ? Verdict.VALID : Verdict.REGRESSION;
    }

    public record ApplyResult(LifecycleState state, Verdict verdict) {
    }

    /**
     * В отличие от Python-версии (бросает {@code ValueError} на REGRESSION),
     * здесь REGRESSION возвращается как часть результата, не exception —
     * вызывающая сторона (KafkaIo) должна залогировать и продолжить
     * обработку следующей записи, не уронить процесс на одном аномальном
     * событии (тот же принцип "одно плохое сообщение не должно ронять
     * весь сервис", что уже применялся в других сервисах этой сессии).
     */
    public static ApplyResult apply(LifecycleState current, LifecycleStatus candidateStatus, String eventId) {
        Verdict verdict = validateTransition(current, candidateStatus, eventId);
        if (verdict == Verdict.DUPLICATE || verdict == Verdict.REGRESSION) {
            return new ApplyResult(current, verdict);
        }
        return new ApplyResult(
            new LifecycleState(candidateStatus, current.lifecycleVersion() + 1, eventId),
            Verdict.VALID
        );
    }
}
