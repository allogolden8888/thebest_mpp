package uz.mpp.scheduler.standard.core;

import java.util.ArrayList;
import java.util.List;
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
     * isPaused(stageName) — только GLOBAL/STAGE-иерархия, без scope конкретного
     * held-item'а. Сохранён как overload для существующих вызовов/тестов, где
     * scope held-item'а не имеет значения (например GLOBAL сам по себе).
     * Эквивалентно {@code isPaused(stageName, "", "")}.
     */
    public boolean isPaused(String stageName) {
        return isPaused(stageName, "", "");
    }

    /**
     * isPaused(stageName, scope, scopeId) — CODE_REVIEW.md Critical #2
     * (ControlSnapshot.java:34-59, до фикса): предыдущая версия проверяла
     * только GLOBAL/STAGE, поэтому PARTNER/PARTNER_STAGE/OPERATOR_ROUTE hold
     * никогда не блокировали release, хотя {@code byKey} уже хранит эти записи
     * (store-update сторона всегда была generic по всем 5 scope из
     * {@code hld.md} §8.1 — не хватало только чтения на release-стороне).
     *
     * <p>{@code scope}/{@code scopeId} — это собственный scope held-item'а
     * (см. {@link HeldItem#scope()}/{@link HeldItem#scopeId()}, тот же scope,
     * что вызвал hold на Pipeline Engine). Проверяются все применимые ключи:
     * GLOBAL, STAGE(stageName), собственный (scope, scopeId) held-item'а
     * (если это PARTNER/PARTNER_STAGE/OPERATOR_ROUTE), и — если scope=PARTNER —
     * дополнительно производный PARTNER_STAGE-ключ {@code scopeId:stageName}
     * (конвенция scope_id для PARTNER_STAGE, см. billing-reconciliation
     * ExecutionControlClient: {@code scope_id="{partner_id}:{stage}"}), чтобы
     * partner+stage-специфичный freeze (например Billing Reconciliation §15.5)
     * тоже блокировал release, даже если исходный hold был STAGE-scoped.
     * PAUSED побеждает, если он есть хоть в одном применимом scope (HLD §8.2:
     * "PAUSED &gt; DEGRADED &gt; ACTIVE" по всей иерархии одновременно).
     */
    public boolean isPaused(String stageName, String scope, String scopeId) {
        for (Entry e : applicableEntries(stageName, scope, scopeId)) {
            if (e.state() == State.PAUSED) {
                return true;
            }
        }
        return false;
    }

    /**
     * rampRate(stageName) — GLOBAL/STAGE-иерархия, overload для существующих
     * вызовов, эквивалентно {@code rampRate(stageName, "", "")}.
     */
    public double rampRate(String stageName) {
        return rampRate(stageName, "", "");
    }

    /**
     * rampRate(stageName, scope, scopeId) — CODE_REVIEW.md Critical #2, тот же
     * пробел, что у isPaused: min(admission_rate) по ВСЕМ применимым scope
     * (hld.md §8.1 "effective_rate = min(all applicable rate limits)"), не
     * только GLOBAL/STAGE. 1.0 при отсутствии применимых записей (ACTIVE по
     * умолчанию, ничего не ограничивает).
     */
    public double rampRate(String stageName, String scope, String scopeId) {
        double rate = 1.0;
        for (Entry e : applicableEntries(stageName, scope, scopeId)) {
            rate = Math.min(rate, e.admissionRate());
        }
        return rate;
    }

    private List<Entry> applicableEntries(String stageName, String scope, String scopeId) {
        List<Entry> entries = new ArrayList<>(3);
        Entry global = byKey.get(key("GLOBAL", ""));
        if (global != null) {
            entries.add(global);
        }
        Entry stage = byKey.get(key("STAGE", stageName));
        if (stage != null) {
            entries.add(stage);
        }
        if (scope != null && !scope.isEmpty() && !"GLOBAL".equals(scope) && !"STAGE".equals(scope)) {
            Entry own = byKey.get(key(scope, scopeId));
            if (own != null) {
                entries.add(own);
            }
            if ("PARTNER".equals(scope) && scopeId != null && !scopeId.isEmpty()) {
                Entry partnerStage = byKey.get(key("PARTNER_STAGE", scopeId + ":" + stageName));
                if (partnerStage != null) {
                    entries.add(partnerStage);
                }
            }
        }
        return entries;
    }

    public boolean isActive(String stageName) {
        return !isPaused(stageName);
    }

    public boolean isActive(String stageName, String scope, String scopeId) {
        return !isPaused(stageName, scope, scopeId);
    }
}
