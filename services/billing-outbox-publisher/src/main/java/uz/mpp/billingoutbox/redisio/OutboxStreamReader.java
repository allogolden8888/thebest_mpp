package uz.mpp.billingoutbox.redisio;

import io.lettuce.core.Consumer;
import io.lettuce.core.RedisClient;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XGroupCreateArgs;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;

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