package uz.mpp.scheduler.background.topology;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import uz.mpp.platformcontracts.events.v1.NotificationRetryTask;
import uz.mpp.platformcontracts.events.v1.SchedulerBackgroundTask;
import uz.mpp.scheduler.background.core.BackgroundTask;
import uz.mpp.scheduler.background.core.DueTaskSelector;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * on_background_command + tick_delay_queue + dispatch_dlr_retry/
 * dispatch_notification_retry (service_internal_methods.md §2.3).
 *
 * <p>Два именованных sink-child'а ("dlr-sink" -> operator.dlr.unresolved,
 * "notification-sink" -> notification.retry) — фиксированные топики, не
 * dynamic TopicNameExtractor, поскольку у двух типов задач разная схема
 * payload (SchedulerBackgroundTask vs NotificationRetryTask), а не только
 * разное имя топика.
 */
public final class BackgroundCommandProcessor implements Processor<String, byte[], String, byte[]> {

    public static final String STORE_NAME = "background-tasks-store";
    public static final String DLR_SINK = "dlr-sink";
    public static final String NOTIFICATION_SINK = "notification-sink";
    public static final String STAGE_RETRY_SINK = "stage-retry-sink";

    private ProcessorContext<String, byte[]> context;
    private KeyValueStore<String, BackgroundTask> store;

    @Override
    public void init(ProcessorContext<String, byte[]> context) {
        this.context = context;
        this.store = context.getStateStore(STORE_NAME);
        context.schedule(Duration.ofSeconds(1), PunctuationType.WALL_CLOCK_TIME, this::tick);
    }

    @Override
    public void process(Record<String, byte[]> record) {
        try {
            SchedulerBackgroundTask cmd = SchedulerBackgroundTask.parseFrom(record.value());
            String taskType = Topics.taskTypeFromEnumName(cmd.getTaskType().name());
            // STAGE_RETRY несёт message_id, не source_event_id (см.
            // BackgroundTask.sourceEventId javadoc) — переиспользуем то же
            // поле-слот под ключ стора, не заводим отдельную ветку хранения.
            String correlationId = "STAGE_RETRY".equals(taskType) ? cmd.getMessageId() : cmd.getSourceEventId();
            BackgroundTask task = new BackgroundTask(
                taskType,
                correlationId,
                cmd.getAttempt(),
                cmd.getDueAt().getSeconds() * 1000,
                cmd.getDeadline().getSeconds() * 1000,
                cmd.getTargetTopic()
            );
            store.put(task.sourceEventId(), task);
        } catch (InvalidProtocolBufferException e) {
            System.err.println("не удалось разобрать SchedulerBackgroundTask: " + e.getMessage());
        }
    }

    /** tick_delay_queue -> DueTasks[] -> dispatch. */
    void tick(long timestampMs) {
        List<BackgroundTask> all = new ArrayList<>();
        try (KeyValueIterator<String, BackgroundTask> it = store.all()) {
            while (it.hasNext()) {
                KeyValue<String, BackgroundTask> kv = it.next();
                all.add(kv.value);
            }
        }

        List<BackgroundTask> due = DueTaskSelector.selectDue(all, timestampMs);
        Instant now = Instant.ofEpochMilli(timestampMs);

        for (BackgroundTask task : due) {
            switch (task.taskType()) {
                case "DLR_CORRELATION_RETRY" -> {
                    SchedulerBackgroundTask wakeup = DispatchBuilder.buildDlrRetryWakeup(task, now);
                    context.forward(new Record<>(task.sourceEventId(), wakeup.toByteArray(), timestampMs), DLR_SINK);
                }
                case "NOTIFICATION_RETRY" -> {
                    NotificationRetryTask retry = DispatchBuilder.buildNotificationRetry(task, now);
                    context.forward(new Record<>(task.sourceEventId(), retry.toByteArray(), timestampMs), NOTIFICATION_SINK);
                }
                case "STAGE_RETRY" -> {
                    SchedulerBackgroundTask trigger = DispatchBuilder.buildStageRetryTrigger(task, now);
                    context.forward(new Record<>(task.sourceEventId(), trigger.toByteArray(), timestampMs), STAGE_RETRY_SINK);
                }
                default -> System.err.println("неизвестный task_type: " + task.taskType());
            }
            store.delete(task.sourceEventId());
        }
    }

    public static ProcessorSupplier<String, byte[], String, byte[]> supplier() {
        return BackgroundCommandProcessor::new;
    }
}
