package uz.mpp.scheduler.standard.topology;

import com.google.protobuf.Timestamp;
import uz.mpp.platformcontracts.common.v1.StageExecuteCommand;
import uz.mpp.scheduler.standard.core.HeldItem;

import java.time.Instant;
import java.util.UUID;

/**
 * publish_release (service_internal_methods.md §2.2): собирает
 * StageExecuteCommand для republish на исходный stage.* топик.
 *
 * <p><b>Известное ограничение</b> (тот же открытый вопрос, что в
 * services/scheduler-critical-sweep/README.md): SchedulerHoldCommand несёт
 * только координаты (message_id, stage_execution_id, scope, stage_name), не
 * payload_ref/stage_extension исходной команды — собранная здесь команда
 * частичная. Стадия-потребитель должна дочитать контекст сообщения
 * самостоятельно (msgctx:{message_id}).
 */
public final class ReleaseCommandBuilder {

    private ReleaseCommandBuilder() {
    }

    public static StageExecuteCommand build(HeldItem item, Instant now) {
        return StageExecuteCommand.newBuilder()
            .setEventId(UUID.randomUUID().toString())
            .setMessageId(item.messageId())
            .setStageExecutionId(item.stageExecutionId())
            .setStageName(Topics.stageNameFromString(item.stageName()))
            .setDeadline(toTimestamp(now))
            .build();
    }

    private static Timestamp toTimestamp(Instant instant) {
        return Timestamp.newBuilder()
            .setSeconds(instant.getEpochSecond())
            .setNanos(instant.getNano())
            .build();
    }
}
