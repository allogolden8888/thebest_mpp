package uz.mpp.scheduler.standard.core;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Локальный immutable snapshot execution.control (compacted,
 * service_io_contracts.md), скармливаемый GlobalKTable в топологии
 * ({@link uz.mpp.scheduler.standard.topology.StandardLaneTopology}).
 * on_control_state_change (service_internal_methods.md §2.2) читает этот
 * снапшот, чтобы решить release для затронутых hold.
 */
public final class ControlSnapshot {

    public enum State { ACTIVE, DEGRADED, PAUSED }

    private final Map<String, Entry> byKey = new ConcurrentHashMap<>();

    public record Entry(State state, double admissionRate, double dispatchRate) {
    }

    private static String key(String scope, String scopeId) {
        return scope + ":" + scopeId;
    }

    public void apply(String scope, String scopeId, State state, double admissionRate, double dispatchRate) {
        byKey.put(key(scope, scopeId), new Entry(state, admissionRate, dispatchRate));
    }

    /**
     * isPaused — PAUSED побеждает по всей scope-иерархии: если GLOBAL или
     * STAGE(stageName) в PAUSED, released не должен идти.
     */
    public boolean isPaused(String stageName) {
        Entry global = byKey.get(key("GLOBAL", ""));
        if (global != null && global.state() == State.PAUSED) {
            return true;
        }
        Entry stage = byKey.get(key("STAGE", stageName));
        return stage != null && stage.state() == State.PAUSED;
    }

    /**
     * rampRate — текущий admission_rate для scope, доминирующий (min) по
     * применимой иерархии GLOBAL/STAGE. 1.0 при отсутствии записей (ACTIVE
     * по умолчанию, ничего не ограничивает release).
     */
    public double rampRate(String stageName) {
        double rate = 1.0;
        Entry global = byKey.get(key("GLOBAL", ""));
        if (global != null) {
            rate = Math.min(rate, global.admissionRate());
        }
        Entry stage = byKey.get(key("STAGE", stageName));
        if (stage != null) {
            rate = Math.min(rate, stage.admissionRate());
        }
        return rate;
    }

    public boolean isActive(String stageName) {
        return !isPaused(stageName);
    }
}
