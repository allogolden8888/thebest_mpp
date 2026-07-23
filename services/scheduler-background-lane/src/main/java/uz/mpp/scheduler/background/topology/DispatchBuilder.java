package uz.mpp.scheduler.background.topology;

import com.google.protobuf.Timestamp;
import uz.mpp.platformcontracts.common.v1.BackgroundTaskType;
import uz.mpp.platformcontracts.events.v1.NotificationRetryTask;
import uz.mpp.platformcontracts.events.v1.SchedulerBackgroundTask;
import uz.mpp.scheduler.background.core.BackgroundTask;

import java.time.Instant;

/**
 * dispatch_dlr_retry / dispatch_notification_retry (service_internal_methods.md
 * §2.3) — чистая сборка payload, без сети.
 */
public final class DispatchBuilder {

    private DispatchBuilder() {
    }

    /**
     * dispatch_dlr_retry — publish на operator.dlr.unresolved. Нет
     * отдельного protobuf-сообщения "unresolved wake-up" в
     * platform-contracts (только OperatorDlr — сырое событие от оператора,
     * и SchedulerBackgroundTask — сама задача) — republish'ится сама задача
     * с attempt+1, DLR Manager (source_event_id = event_id исходного
     * operator.dlr) читает её и повторяет lookup_correlation. См. README
     * "Открытый вопрос".
     */
    public static SchedulerBackgroundTask buildDlrRetryWakeup(BackgroundTask task, Instant now) {
        return SchedulerBackgroundTask.newBuilder()
            .setTaskType(BackgroundTaskType.BACKGROUND_TASK_TYPE_DLR_CORRELATION_RETRY)
            .setSourceEventId(task.sourceEventId())
            .setAttempt(task.attempt() + 1)
            .setDueAt(toTimestamp(now))
            .setDeadline(task.deadlineEpochMs() > 0 ? toTimestamp(Instant.ofEpochMilli(task.deadlineEpochMs())) : Timestamp.getDefaultInstance())
            .setTargetTopic(task.targetTopic())
            .build();
    }

    /**
     * dispatch_notification_retry — publish NotificationRetryTask на
     * notification.retry. **Известное ограничение**: SchedulerBackgroundTask
     * не несёт message_id (только source_event_id = event_id исходного
     * message.lifecycle) — NotificationRetryTask.message_id остаётся пустым
     * здесь; Notification Service должен уметь резолвить message_id из
     * lifecycle_event_id самостоятельно. См. README "Открытый вопрос".
     */
    public static NotificationRetryTask buildNotificationRetry(BackgroundTask task, Instant now) {
        return NotificationRetryTask.newBuilder()
            .setLifecycleEventId(task.sourceEventId())
            .setAttempt(task.attempt() + 1)
            .setRetryAt(toTimestamp(now))
            .build();
    }

    private static Timestamp toTimestamp(Instant instant) {
        return Timestamp.newBuilder().setSeconds(instant.getEpochSecond()).setNanos(instant.getNano()).build();
    }
}
