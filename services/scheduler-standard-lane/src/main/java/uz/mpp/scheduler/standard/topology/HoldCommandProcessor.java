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
    private static final double BUCKET_CAPACITY_UNDIVIDED = 50;
    private static final double BUCKET_REFILL_PER_SECOND_UNDIVIDED = 10;

    /**
     * CODE_REVIEW.md High #5 (bucketsByStage — HoldCommandProcessor.java:41):
     * bucketsByStage — обычное instance-поле, а Kafka Streams создаёт один
     * HoldCommandProcessor НА КАЖДУЮ assigned-партицию scheduler.standard.commands
     * — топик партиционирован по message_id (data_infrastructure_spec.md: "|
     * scheduler.standard.commands | message_id | 8 | delete | ..."), НЕ по
     * stage_name. Значит задуманный "один bucket на stage" лимит существует
     * ОТДЕЛЬНО НА КАЖДУЮ ПАРТИЦИЮ — при 8 партициях на одном инстансе (один
     * реплика, наихудший случай single-instance) эффективный aggregate rate до
     * 8x выше документированного.
     *
     * <p>Полноценный фикс требует rate limiter, разделяемого МЕЖДУ партициями/
     * репликами (например Redis CAS — тот же паттерн, что pipeline-engine уже
     * использует для ExecutionState против реального Redis, см. `cas_transition_
     * and_track_deadline` в pipeline-engine, или billing-service/partner-rest-
     * receiver Redis-Lua паттерн для distributed rate limit) — архитектурно
     * самая сложная находка в этом срезе, не сделан в рамках этого прохода (см.
     * README "Проверено кодревью").
     *
     * <p>Как задокументированная ЧАСТИЧНАЯ митигация (не полный фикс):
     * capacity и refill делятся на реальное число партиций топика (8, источник
     * — data_infrastructure_spec.md, не догадка), чтобы в наихудшем случае
     * (все 8 партиций на одном инстансе, один реплика) aggregate оставался
     * близко к изначально задуманной per-stage ставке. Это НЕ чинит баг: при
     * нескольких репликах (партиции размазаны по инстансам) или при частичном
     * переназначении партиций во время rebalance aggregate rate всё ещё
     * отклоняется (недо- или пере-лимитирует) — эта константа только сужает
     * задокументированный выше наихудший single-instance случай, а не убирает
     * саму проблему "лимит не разделяется между партициями/репликами".
     */
    private static final int STANDARD_COMMANDS_PARTITION_COUNT = 8; // data_infrastructure_spec.md: scheduler.standard.commands
    private static final double BUCKET_CAPACITY = BUCKET_CAPACITY_UNDIVIDED / STANDARD_COMMANDS_PARTITION_COUNT;
    private static final double BUCKET_REFILL_PER_SECOND = BUCKET_REFILL_PER_SECOND_UNDIVIDED / STANDARD_COMMANDS_PARTITION_COUNT;

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

            // CODE_REVIEW.md High #3 (poison-pill: Topics.stageTopic() has no
            // UNSPECIFIED case and throws inside publishRelease()'s
            // TopicNameExtractor, AFTER store.delete() would have run, so the bad
            // item is never cleared and re-triggers the same crash on every
            // releaseTick + restart). Validate stage_name against the exact same
            // switch stageTopic() uses BEFORE the item ever enters
            // standard-holds-store, so a poison item can never be created here.
            if (!Topics.hasTopic(cmd.getStageName())) {
                System.err.println("отклонена SchedulerHoldCommand с неизвестным stage_name="
                    + cmd.getStageName() + " (stageExecutionId=" + cmd.getStageExecutionId() + ")");
                return;
            }

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

            // CODE_REVIEW.md Critical #2 (ControlSnapshot.java:34-59 /
            // HoldCommandProcessor.java:82,88, до фикса): раньше здесь проверялся
            // только snapshot.isPaused(stageName)/rampRate(stageName) — GLOBAL/STAGE
            // только, scope конкретного held-item'а (PARTNER/PARTNER_STAGE/
            // OPERATOR_ROUTE, уже сохранённый на HeldItem при hold) никогда не
            // читался обратно, поэтому scoped-паузы никогда не блокировали release.
            // Теперь isPaused/rampRate вызываются per-item с item.scope()/scopeId()
            // (hld.md §8.1: effective_rate = min по всем применимым scope) — item,
            // чей собственный applicable scope PAUSED, остаётся held; rampRate
            // batch'а — минимум (наиболее строгий) среди всех eligible-item'ов этой
            // стадии, что консервативно (никогда не отпускает быстрее, чем позволяет
            // самый строгий применимый scope среди присутствующих в batch).
            List<HeldItem> eligible = new ArrayList<>();
            double rampRate = 1.0;
            for (HeldItem item : entry.getValue()) {
                if (snapshot.isPaused(stageName, item.scope(), item.scopeId())) {
                    continue; // check_execution_control: Hold — не публикуем для этого item'а
                }
                eligible.add(item);
                rampRate = Math.min(rampRate, snapshot.rampRate(stageName, item.scope(), item.scopeId()));
            }
            if (eligible.isEmpty()) {
                continue;
            }

            TokenBucket bucket = bucketsByStage.computeIfAbsent(stageName,
                k -> new TokenBucket(BUCKET_CAPACITY, BUCKET_REFILL_PER_SECOND, timestampMs));

            List<HeldItem> batch = ReleaseBatchSelector.selectBatch(eligible, rampRate, bucket, timestampMs);
            for (HeldItem item : batch) {
                // CODE_REVIEW.md High #3, defense-in-depth: delete BEFORE forward, не
                // после — если publishRelease() всё же бросит (например будущий баг
                // в ReleaseCommandBuilder/TopicNameExtractor), запись уже не в store и
                // не re-triggers тот же краш на каждом следующем тике/рестарте.
                store.delete(item.stageExecutionId());
                publishRelease(item, timestampMs);
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

        // CODE_REVIEW.md High #4 (FairScheduler's javadoc assumes pre-sorted
        // input, but store.all() over a RocksDB-backed KeyValueStore iterates in
        // KEY order — keyed by stageExecutionId, unrelated to held_at — so
        // release order within a stage was effectively arbitrary w.r.t. hold
        // time, not FIFO. Sort each stage's list by heldAtEpochMs here, before
        // FairScheduler/ReleaseBatchSelector ever see it, so hold time — not
        // RocksDB key order — drives release order.
        for (List<HeldItem> items : byStage.values()) {
            items.sort(java.util.Comparator.comparingLong(HeldItem::heldAtEpochMs));
        }
        return byStage;
    }

    public static ProcessorSupplier<String, byte[], String, byte[]> supplier(ControlSnapshot snapshot) {
        return () -> new HoldCommandProcessor(snapshot);
    }
}
