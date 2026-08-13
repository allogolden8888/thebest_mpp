package uz.mpp.scheduler.background.core;

/**
 * Одна зарегистрированная фоновая задача (on_background_command,
 * service_internal_methods.md §2.3) — DLR_CORRELATION_RETRY,
 * NOTIFICATION_RETRY или STAGE_RETRY, из scheduler.background.commands
 * (SchedulerBackgroundTask, HLD §9.3/§14/§19).
 */
public record BackgroundTask(
    String taskType,       // "DLR_CORRELATION_RETRY" | "NOTIFICATION_RETRY" | "STAGE_RETRY"
    // Для STAGE_RETRY сюда кладётся message_id (см. SchedulerBackgroundTask.message_id,
    // не source_event_id — тот для STAGE_RETRY не заполняется), не event_id
    // чужого события — то же самое поле переиспользуется под ключ стора
    // (BackgroundCommandProcessor.process/tick), не отдельное новое поле.
    String sourceEventId,
    int attempt,
    long dueAtEpochMs,
    long deadlineEpochMs,
    String targetTopic     // "operator.dlr.unresolved" | "notification.retry" | "pipeline.retry.triggers" — разрешённый список
) {
    public boolean isDue(long nowEpochMs) {
        return dueAtEpochMs <= nowEpochMs;
    }

    public boolean isExpired(long nowEpochMs) {
        return deadlineEpochMs > 0 && deadlineEpochMs <= nowEpochMs;
    }
}
