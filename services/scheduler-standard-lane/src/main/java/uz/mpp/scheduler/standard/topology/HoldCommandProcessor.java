package uz.mpp.scheduler.standard.topology;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import uz.mpp.platformcontracts.common.v1.StageExecuteCommand;
import uz.mpp.platformcontracts.events.v1.SchedulerHoldCommand;
import uz.mpp.scheduler.standard.core.ControlSnapshot;
import uz.mpp.scheduler.standard.core.HeldItem;
import uz.mpp.scheduler.standard.core.ReleaseBatchSelector;
import uz.mpp.scheduler.standard.core.TokenBucket;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * on_hold_command + release_backlog_batch/apply_fair_scheduling/
 * apply_token_bucket/publish_release + on_reconciliation_deadline
 * (service_internal_methods.md §2.2).
 */
public final class HoldCommandProcessor implements Processor<String, byte[], String, byte[]> {

    public static final String STORE_NAME = "standard-holds-store";

    /** Токены/сек и ёмкость per-stage bucket — не задокументированы отдельной
     *  JSON Schema (см. README "Открытый вопрос"), рабочее предположение. */
    private static final double BUCKET_CAPACITY = 50;
    private static final double BUCKET_REFILL_PER_SECOND = 10;

    private final ControlSnapshot snapshot;
    private final Map<String, TokenBucket> bucketsByStage = new HashMap<>();

    private ProcessorContext<String, byte[]> context;
    private KeyValueStore<String, HeldItem> store;

    public HoldCommandProcessor(ControlSnapshot snapshot) {
        this.snapshot = snapshot;
    }

    @Override
    public void init(ProcessorContext<String, byte[]> context) {
        this.context = context;
        this.store = context.getStateStore(STORE_NAME);
        context.schedule(Duration.ofSeconds(1), PunctuationType.WALL_CLOCK_TIME, this::releaseTick);
        context.schedule(Duration.ofSeconds(30), PunctuationType.WALL_CLOCK_TIME, this::reconciliationDeadlineTick);
    }

    @Override
    public void process(Record<String, byte[]> record) {
        try {
            SchedulerHoldCommand cmd = SchedulerHoldCommand.parseFrom(record.value());
            HeldItem item = new HeldItem(
                cmd.getMessageId(),
                cmd.getStageExecutionId(),
                cmd.getScope().name().replace("EXECUTION_CONTROL_SCOPE_", ""),
                cmd.getScopeId(),
                cmd.getStageName().name().replace("STAGE_NAME_", ""),
                cmd.getHeldAt().getSeconds() * 1000
            );
            store.put(item.stageExecutionId(), item);
        } catch (InvalidProtocolBufferException e) {
            System.err.println("не удалось разобрать SchedulerHoldCommand: " + e.getMessage());
        }
    }

    /** release_backlog_batch поверх текущего содержимого стора, сгруппированного по stage. */
    void releaseTick(long timestampMs) {
        Map<String, List<HeldItem>> byStage = groupByStage(null);

        for (Map.Entry<String, List<HeldItem>> entry : byStage.entrySet()) {
            String stageName = entry.getKey();
            if (snapshot.isPaused(stageName)) {
                continue; // check_execution_control: Hold — не публикуем в эту стадию
            }

            TokenBucket bucket = bucketsByStage.computeIfAbsent(stageName,
                k -> new TokenBucket(BUCKET_CAPACITY, BUCKET_REFILL_PER_SECOND, timestampMs));
            double rampRate = snapshot.rampRate(stageName);

            List<HeldItem> batch = ReleaseBatchSelector.selectBatch(entry.getValue(), rampRate, bucket, timestampMs);
            for (HeldItem item : batch) {
                publishRelease(item, timestampMs);
                store.delete(item.stageExecutionId());
            }
        }
    }

    /**
     * on_reconciliation_deadline — периодический wake-up для held-записей
     * DELIVERY_RECONCILIATION, независимо от execution control (Reconciliation
     * должен переопрашивать оператора по таймеру, а не только когда ramp
     * позволяет release, см. README "Интерпретация" — метод в LLD
     * специфицирован отдельно от обычного release, но не детализирован).
     * Не удаляет из store — это нудж, не release.
     */
    void reconciliationDeadlineTick(long timestampMs) {
        List<HeldItem> pending = groupByStage("DELIVERY_RECONCILIATION").getOrDefault("DELIVERY_RECONCILIATION", List.of());
        for (HeldItem item : pending) {
            publishRelease(item, timestampMs);
        }
    }

    private void publishRelease(HeldItem item, long timestampMs) {
        StageExecuteCommand cmd = ReleaseCommandBuilder.build(item, Instant.ofEpochMilli(timestampMs));
        context.forward(new Record<>(item.messageId(), cmd.toByteArray(), timestampMs));
    }

    private Map<String, List<HeldItem>> groupByStage(String onlyStage) {
        Map<String, List<HeldItem>> byStage = new HashMap<>();
        try (KeyValueIterator<String, HeldItem> it = store.all()) {
            while (it.hasNext()) {
                KeyValue<String, HeldItem> kv = it.next();
                HeldItem item = kv.value;
                if (onlyStage != null && !onlyStage.equals(item.stageName())) {
                    continue;
                }
                byStage.computeIfAbsent(item.stageName(), k -> new ArrayList<>()).add(item);
            }
        }
        return byStage;
    }

    public static ProcessorSupplier<String, byte[], String, byte[]> supplier(ControlSnapshot snapshot) {
        return () -> new HoldCommandProcessor(snapshot);
    }
}
