package uz.mpp.scheduler.background.topology;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import uz.mpp.scheduler.background.core.BackgroundTask;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;

/** Serde для {@link BackgroundTask} в KeyValueStore — внутреннее представление, не platform-contracts тип. */
public final class BackgroundTaskSerde implements Serde<BackgroundTask> {

    @Override
    public Serializer<BackgroundTask> serializer() {
        return (topic, task) -> {
            if (task == null) {
                return null;
            }
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            try (DataOutputStream out = new DataOutputStream(bos)) {
                out.writeUTF(task.taskType());
                out.writeUTF(task.sourceEventId());
                out.writeInt(task.attempt());
                out.writeLong(task.dueAtEpochMs());
                out.writeLong(task.deadlineEpochMs());
                out.writeUTF(task.targetTopic());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return bos.toByteArray();
        };
    }

    @Override
    public Deserializer<BackgroundTask> deserializer() {
        return (topic, bytes) -> {
            if (bytes == null) {
                return null;
            }
            try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
                String taskType = in.readUTF();
                String sourceEventId = in.readUTF();
                int attempt = in.readInt();
                long dueAt = in.readLong();
                long deadline = in.readLong();
                String targetTopic = in.readUTF();
                return new BackgroundTask(taskType, sourceEventId, attempt, dueAt, deadline, targetTopic);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
    }

    public static Serde<String> keySerde() {
        return Serdes.String();
    }
}
