package uz.mpp.scheduler.standard.topology;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import uz.mpp.platformcontracts.common.v1.ExecutionControlState;
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

            // CODE_REVIEW.md Critical #1 (ControlUpdateProcessor.java:39-52, до фикса):
            // ControlSnapshot.State.valueOf(...) на "UNSPECIFIED"/"UNRECOGNIZED" бросал
            // непойманный IllegalArgumentException (Enum.valueOf), который НЕ ловится
            // окружающим catch (InvalidProtocolBufferException). Это работает на
            // единственном GlobalStreamThread без registered uncaught-exception handler'а
            // (Main.java) — непойманное исключение здесь убивает весь инстанс, а
            // addGlobalStore реплеит execution.control с начала при каждом рестарте, так
            // что одна запись с UNSPECIFIED/version-skewed состоянием даёт ПОСТОЯННЫЙ
            // crash loop. Тот же defensive паттерн, что у scheduler-background-lane
            // (BackgroundCommandProcessor.java:80-92, "log + skip", не throw) — toState()
            // ниже не бросает вообще, просто возвращает null для незнакомого значения, и
            // такая запись логируется и пропускается (не применяется к snapshot, не
            // пишется в store), а обработка остальных записей продолжается.
            ControlSnapshot.State state = toState(rec.getState());
            if (state == null) {
                System.err.println("неизвестное ExecutionControlState " + rec.getState()
                    + " (scope=" + rec.getScope() + ", scopeId=" + rec.getScopeId()
                    + ") — запись пропущена, без применения к ControlSnapshot");
                return;
            }

            snapshot.apply(
                rec.getScope().name().replace("EXECUTION_CONTROL_SCOPE_", ""),
                rec.getScopeId(),
                state,
                rec.getAdmissionRate(),
                rec.getDispatchRate()
            );
            store.put(record.key(), record.value());
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
            // Одна повреждённая запись не должна ронять global store restore.
            System.err.println("не удалось разобрать ExecutionControlRecord: " + e.getMessage());
        }
    }

    /**
     * Безопасное отображение proto {@link ExecutionControlState} -&gt;
     * {@link ControlSnapshot.State}, без throw на незнакомых значениях
     * (EXECUTION_CONTROL_STATE_UNSPECIFIED, а также UNRECOGNIZED — неизвестное
     * численное значение из более новой версии контракта, version skew).
     */
    private static ControlSnapshot.State toState(ExecutionControlState protoState) {
        return switch (protoState) {
            case EXECUTION_CONTROL_STATE_ACTIVE -> ControlSnapshot.State.ACTIVE;
            case EXECUTION_CONTROL_STATE_DEGRADED -> ControlSnapshot.State.DEGRADED;
            case EXECUTION_CONTROL_STATE_PAUSED -> ControlSnapshot.State.PAUSED;
            default -> null; // EXECUTION_CONTROL_STATE_UNSPECIFIED, UNRECOGNIZED
        };
    }

    public static ProcessorSupplier<String, byte[], Void, Void> supplier(ControlSnapshot snapshot) {
        return () -> new ControlUpdateProcessor(snapshot);
    }
}
