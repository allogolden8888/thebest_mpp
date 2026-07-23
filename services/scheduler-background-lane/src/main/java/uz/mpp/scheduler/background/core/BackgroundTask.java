package uz.mpp.scheduler.background.core;

/**
 * Одна зарегистрированная фоновая задача (on_background_command,
 * service_internal_methods.md §2.3) — DLR_CORRELATION_RETRY или
 * NOTIFICATION_RETRY, из scheduler.background.commands
 * (SchedulerBackgroundTask, HLD §9.3/§14/§19).
 */
public record BackgroundTask(
    String taskType,       // "DLR_CORRELATION_RETRY" | "NOTIFICATION_RETRY"
    String sourceEventId,
    int attempt,
    long dueAtEpochMs,
    long deadlineEpochMs,
    String targetTopic     // "operator.dlr.unresolved" | "notification.retry" — разрешённый список
) {
    public boolean isDue(long nowEpochMs) {
        return dueAtEpochMs <= nowEpochMs;
    }

    public boolean isExpired(long nowEpochMs) {
        return deadlineEpochMs > 0 && deadlineEpochMs <= nowEpochMs;
    }
}
