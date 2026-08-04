package uz.mpp.partnersmpp.registry;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;

import java.time.Duration;
import java.util.Map;

/**
 * register_session + heartbeat_tick (service_internal_methods.md §1.2) в
 * Runtime Redis — `smpp:partner_session:{partner_id}:{system_id}` HASH
 * (data_infrastructure_spec.md §2.1: session_id, gateway_instance_id,
 * endpoint, session_epoch, heartbeat; TTL = 3× heartbeat interval).
 */
public final class SessionRedisRegistry implements AutoCloseable {

    private final RedisClient client;
    private final StatefulRedisConnection<String, String> connection;
    private final RedisCommands<String, String> commands;
    private final String gatewayInstanceId;
    private final Duration heartbeatInterval;

    public SessionRedisRegistry(String redisUri, String gatewayInstanceId, Duration heartbeatInterval) {
        this.client = RedisClient.create(redisUri);
        this.connection = client.connect();
        this.commands = connection.sync();
        this.gatewayInstanceId = gatewayInstanceId;
        this.heartbeatInterval = heartbeatInterval;
    }

    private static String key(String partnerId, String systemId) {
        return "smpp:partner_session:" + partnerId + ":" + systemId;
    }

    /** register_session. */
    public void register(String partnerId, String systemId, String sessionId, long sessionEpoch, String endpoint) {
        String key = key(partnerId, systemId);
        commands.hset(key, Map.of(
            "session_id", sessionId,
            "gateway_instance_id", gatewayInstanceId,
            "endpoint", endpoint,
            "session_epoch", Long.toString(sessionEpoch),
            "heartbeat", Long.toString(System.currentTimeMillis())
        ));
        commands.expire(key, heartbeatInterval.multipliedBy(3));
    }

    /** heartbeat_tick — обновляет heartbeat и продлевает TTL. */
    public void heartbeat(String partnerId, String systemId) {
        String key = key(partnerId, systemId);
        commands.hset(key, "heartbeat", Long.toString(System.currentTimeMillis()));
        commands.expire(key, heartbeatInterval.multipliedBy(3));
    }

    public void unregister(String partnerId, String systemId) {
        commands.del(key(partnerId, systemId));
    }

    public Map<String, String> lookup(String partnerId, String systemId) {
        return commands.hgetall(key(partnerId, systemId));
    }

    @Override
    public void close() {
        connection.close();
        client.shutdown();
    }
}