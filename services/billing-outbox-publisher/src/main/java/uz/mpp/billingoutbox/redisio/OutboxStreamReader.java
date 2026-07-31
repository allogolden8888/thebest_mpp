package uz.mpp.billingoutbox.redisio;

import io.lettuce.core.Consumer;
import io.lettuce.core.RedisClient;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XAutoClaimArgs;
import io.lettuce.core.XGroupCreateArgs;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.models.stream.ClaimedMessages;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import uz.mpp.billingoutbox.core.StreamEntry;

/**
 * poll_redis_stream + ack_stream_entry (service_internal_methods.md §5.1) —
 * Lettuce XREADGROUP/XACK против billing:outbox:{shard}
 * (data_infrastructure_spec.md §2.3, consumer group).
 */
public final class OutboxStreamReader implements AutoCloseable {

    private final RedisClient client;
    private final StatefulRedisConnection<String, String> connection;
    private final RedisCommands<String, String> commands;
    private final String groupName;
    private final String consumerName;
    private final int numShards;

    public OutboxStreamReader(String redisUri, String groupName, String consumerName, int numShards) {
        this.client = RedisClient.create(redisUri);
        this.connection = client.connect();
        this.commands = connection.sync();
        this.groupName = groupName;
        this.consumerName = consumerName;
        this.numShards = numShards;
        ensureGroupsExist();
    }

    OutboxStreamReader(RedisCommands<String, String> commands, String groupName, String consumerName, int numShards) {
        this.client = null;
        this.connection = null;
        this.commands = commands;
        this.groupName = groupName;
        this.consumerName = consumerName;
        this.numShards = numShards;
        ensureGroupsExist();
    }

    private static String shardKey(int shard) {
        return "billing:outbox:" + shard;
    }

    private void ensureGroupsExist() {
        for (int shard = 0; shard < numShards; shard++) {
            try {
                commands.xgroupCreate(io.lettuce.core.XReadArgs.StreamOffset.from(shardKey(shard), "0"), groupName,
                    XGroupCreateArgs.Builder.mkstream());
            } catch (Exception e) {
                // BUSYGROUP — группа уже существует, ожидаемо при рестарте.
                if (!e.getMessage().contains("BUSYGROUP")) {
                    throw e;
                }
            }
        }
    }

    /** poll_redis_stream — обходит все шарды, читает pending entries для этого consumer'а. */
    public List<StreamEntry> pollAllShards(int countPerShard) {
        List<StreamEntry> entries = new ArrayList<>();
        for (int shard = 0; shard < numShards; shard++) {
            List<StreamMessage<String, String>> messages = commands.xreadgroup(
                Consumer.from(groupName, consumerName),
                XReadArgs.Builder.count(countPerShard),
                XReadArgs.StreamOffset.lastConsumed(shardKey(shard))
            );
            for (StreamMessage<String, String> msg : messages) {
                entries.add(toStreamEntry(shard, msg));
            }
        }
        return entries;
    }

    /** ack_stream_entry. */
    public void ack(int shard, String redisEntryId) {
        commands.xack(shardKey(shard), groupName, redisEntryId);
    }

    /**
     * reclaim_stale_pending — закрывает CRITICAL находку кодревью
     * (CODE_REVIEW.md, "billing-outbox-publisher" #1): {@link #pollAllShards}
     * читает ТОЛЬКО через {@code >} (никогда не доставленные записи) —
     * запись, чей publish/ack не завершился (сбой Kafka, краш процесса между
     * {@code XREADGROUP} и {@code XACK}), оставалась в PEL (pending entries
     * list) этого consumer group навсегда: ни один код в сервисе никогда её
     * не перечитывал. Поскольку charge уже атомарно применён к hot-балансу в
     * Redis ДО записи в outbox stream ({@code hld.md §15.4}), это означало
     * перманентную потерю ledger-события без единого сигнала до того, как
     * reconciliation заметит расхождение — возможно, днями позже.
     *
     * <p>{@code XAUTOCLAIM} переносит записи, простаивающие в PEL дольше
     * {@code minIdleTime}, на ЭТОГО consumer'а — заявляя их заново независимо
     * от исходного consumer'а, поэтому естественно переживает рестарт под
     * новым {@code HOSTNAME} (consumer name), не только транзиентный сбой
     * publish. Начинать скан всегда с {@code "0-0"} безопасно и идемпотентно:
     * запись, уже не простаивающая (< minIdleTime, например реально
     * обрабатывается прямо сейчас), просто не возвращается этим вызовом.
     */
    public List<StreamEntry> reclaimStalePending(int shard, Duration minIdleTime, int count) {
        ClaimedMessages<String, String> claimed = commands.xautoclaim(shardKey(shard),
            XAutoClaimArgs.Builder.<String>xautoclaim(Consumer.from(groupName, consumerName), minIdleTime, "0-0").count(count));
        List<StreamEntry> entries = new ArrayList<>();
        for (StreamMessage<String, String> msg : claimed.getMessages()) {
            entries.add(toStreamEntry(shard, msg));
        }
        return entries;
    }

    private static StreamEntry toStreamEntry(int shard, StreamMessage<String, String> msg) {
        Map<String, String> body = msg.getBody();
        return new StreamEntry(
            shard,
            msg.getId(),
            body.get("charge_id"),
            body.get("account_id"),
            body.get("partner_id"),
            Long.parseLong(body.getOrDefault("amount_minor_units", "0")),
            body.get("currency_code"),
            body.get("entry_type"),
            body.get("source_charge_id"),
            body.getOrDefault("reason", ""),
            Long.parseLong(body.getOrDefault("created_at_epoch_ms", "0"))
        );
    }

    @Override
    public void close() {
        if (connection != null) {
            connection.close();
        }
        if (client != null) {
            client.shutdown();
        }
    }
}