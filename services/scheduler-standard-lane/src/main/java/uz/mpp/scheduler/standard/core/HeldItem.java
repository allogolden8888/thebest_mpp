package uz.mpp.scheduler.standard.core;

/**
 * Одна зарегистрированная hold-запись (on_hold_command,
 * service_internal_methods.md §2.2) — Pipeline Engine публикует
 * SchedulerHoldCommand при обнаружении PAUSED на диспетчеризации (HLD §9.2).
 *
 * <p>Известное ограничение (см. README.md "Открытый вопрос", то же, что у
 * scheduler-critical-sweep): SchedulerHoldCommand не несёт полный
 * StageExecuteCommand (payload_ref/stage_extension) — только координаты
 * (message_id, stage_execution_id, scope, stage_name). publish_release в
 * этом срезе публикует частичную команду.
 */
public record HeldItem(
    String messageId,
    String stageExecutionId,
    String scope,
    String scopeId,
    String stageName,
    long heldAtEpochMs
) {
    /** Ключ для fair scheduling — приоритет группировки round-robin. */
    public String fairnessKey() {
        return scope + ":" + scopeId;
    }
}
