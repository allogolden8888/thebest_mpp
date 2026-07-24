package uz.mpp.msr;

/**
 * {@code status == null} — сообщение ещё не имеет ни одного применённого
 * перехода (свежее состояние, "message_id никогда раньше не видели").
 */
public record LifecycleState(LifecycleStatus status, long lifecycleVersion, String lastAppliedEventId) {

    public static LifecycleState fresh() {
        return new LifecycleState(null, 0, null);
    }
}
