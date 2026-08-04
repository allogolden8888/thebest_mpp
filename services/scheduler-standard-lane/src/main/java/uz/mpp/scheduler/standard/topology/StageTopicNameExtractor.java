package uz.mpp.scheduler.standard.topology;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.kafka.streams.processor.RecordContext;
import org.apache.kafka.streams.processor.TopicNameExtractor;
import uz.mpp.platformcontracts.common.v1.StageExecuteCommand;

/**
 * Dynamic sink routing — publish_release должен уйти на исходный stage.*
 * топик конкретной стадии, не на один фиксированный output. Разбирает
 * stage_name из уже сериализованной StageExecuteCommand (значение записи).
 */
public final class StageTopicNameExtractor implements TopicNameExtractor<String, byte[]> {

    @Override
    public String extract(String key, byte[] value, RecordContext recordContext) {
        try {
            StageExecuteCommand cmd = StageExecuteCommand.parseFrom(value);
            return Topics.stageTopic(cmd.getStageName());
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalStateException("не удалось разобрать StageExecuteCommand для маршрутизации sink", e);
        }
    }
}
