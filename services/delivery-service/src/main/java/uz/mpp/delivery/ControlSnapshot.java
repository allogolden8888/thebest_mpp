package uz.mpp.delivery;

/**
 * {@code check_control_state} (service_internal_methods.md §1.8) — повторная
 * проверка {@code execution.control} по {@code OPERATOR_ROUTE} непосредственно
 * перед submit (HLD §8). Тот же паттерн, что {@code ControlState} в
 * routing-service и {@code AlwaysAdmit} в partner-rest-receiver: ни один
 * сервис в этом срезе не реализует реальное потребление {@code
 * execution.control} — пустой снапшот, fail-open по всем scope.
 */
public final class ControlSnapshot {

    public enum Decision {
        ADMIT,
        HOLD
    }

    public Decision check(String routeId) {
        return Decision.ADMIT;
    }
}
