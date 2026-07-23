package uz.mpp.scheduler.standard.topology;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import uz.mpp.platformcontracts.events.v1.ExecutionControlRecord;
import uz.mpp.scheduler.standard.core.ControlSnapshot;

/**
 * Global store processor для execution.control (on_control_state_change,
 * service_internal_methods.md §2.2). Kafka Streams использует этот же
 * processor и для restore-replay при старте (весь compacted топик с
 * начала), и для live-обновлений — поэтому ControlSnapshot воссоздаётся
 * корректно после рестарта без отдельного кода восстановления.
 */
public final class ControlUpdateProcessor implements Processor<String, byte[], Void, Void> {

    public static final String STORE_NAME = "control-snapshot-store";

    private final ControlSnapshot snapshot;
    private KeyValueStore<String, byte[]> store;

    public ControlUpdateProcessor(ControlSnapshot snapshot) {
        this.snapshot = snapshot;
    }

    @Override
    public void init(ProcessorContext<Void, Void> context) {
        store = context.getStateStore(STORE_NAME);
    }

    @Override
    public void process(Record<String, byte[]> record) {
        if (record.value() == null) {
            return; // tombstone — запись execution.control удалена/архивирована
        }
        try {
            ExecutionControlRecord rec = ExecutionControlRecord.parseFrom(record.value());
            snapshot.apply(
                rec.getScope().name().replace("EXECUTION_CONTROL_SCOPE_", ""),
                rec.getScopeId(),
                ControlSnapshot.State.valueOf(rec.getState().name().replace("EXECUTION_CONTROL_STATE_", "")),
                rec.getAdmissionRate(),
                rec.getDispatchRate()
            );
            store.put(record.key(), record.value());
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
            // Одна повреждённая запись не должна ронять global store restore.
            System.err.println("не удалось разобрать ExecutionControlRecord: " + e.getMessage());
        }
    }

    public static ProcessorSupplier<String, byte[], Void, Void> supplier(ControlSnapshot snapshot) {
        return () -> new ControlUpdateProcessor(snapshot);
    }
}
