package uz.mpp.scheduler.standard.topology;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import uz.mpp.scheduler.standard.core.HeldItem;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * Serde для {@link HeldItem} в KeyValueStore ("standard-holds-store") — не
 * platform-contracts protobuf-тип (HeldItem — внутреннее представление
 * Standard Lane, не пересекает границу сервиса), поэтому простая
 * length-prefixed бинарная форма, без protobuf-схемы.
 */
public final class HeldItemSerde implements Serde<HeldItem> {

    @Override
    public Serializer<HeldItem> serializer() {
        return (topic, item) -> {
            if (item == null) {
                return null;
            }
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            try (DataOutputStream out = new DataOutputStream(bos)) {
                out.writeUTF(item.messageId());
                out.writeUTF(item.stageExecutionId());
                out.writeUTF(item.scope());
                out.writeUTF(item.scopeId());
                out.writeUTF(item.stageName());
                out.writeLong(item.heldAtEpochMs());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return bos.toByteArray();
        };
    }

    @Override
    public Deserializer<HeldItem> deserializer() {
        return (topic, bytes) -> {
            if (bytes == null) {
                return null;
            }
            try (DataInputStream in = new DataInputStream(new java.io.ByteArrayInputStream(bytes))) {
                String messageId = in.readUTF();
                String stageExecutionId = in.readUTF();
                String scope = in.readUTF();
                String scopeId = in.readUTF();
                String stageName = in.readUTF();
                long heldAt = in.readLong();
                return new HeldItem(messageId, stageExecutionId, scope, scopeId, stageName, heldAt);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
    }

    public static Serde<String> keySerde() {
        return Serdes.String();
    }
}
