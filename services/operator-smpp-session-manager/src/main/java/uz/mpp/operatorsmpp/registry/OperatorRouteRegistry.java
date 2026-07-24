package uz.mpp.operatorsmpp.registry;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;

import java.time.Duration;
import java.util.Map;

/**
 * register_route (service_internal_methods.md §1.3) в Runtime Redis —
 * `operator_route:{operator_id}:{route_id}` HASH, общий с Operator HTTP
 * Gateway (data_infrastructure_spec.md §2.1, HLD §11.4):
 * protocol, owning_instance_id, endpoint, route_epoch, heartbeat.
 */
public final class OperatorRouteRegistry implements AutoCloseable {

    private final RedisClient client;
    private final StatefulRedisConnection<String, String> connection;
    private final RedisCommands<String, String> commands;
    private final String owningInstanceId;
    private final Duration heartbeatInterval;

    public OperatorRouteRegistry(String redisUri, String owningInstanceId, Duration heartbeatInterval) {
        this.client = RedisClient.create(redisUri);
        this.connection = client.connect();
        this.commands = connection.sync();
        this.owningInstanceId = owningInstanceId;
        this.heartbeatInterval = heartbeatInterval;
    }

    private static String key(String operatorId, String routeId) {
        return "operator_route:" + operatorId + ":" + routeId;
    }

    public void register(String operatorId, String routeId, long routeEpoch, String endpoint) {
        String key = key(operatorId, routeId);
        commands.hset(key, Map.of(
            "protocol", "SMPP",
            "owning_instance_id", owningInstanceId,
            "endpoint", endpoint,
            "route_epoch", Long.toString(routeEpoch),
            "heartbeat", Long.toString(System.currentTimeMillis())
        ));
        commands.expire(key, heartbeatInterval.multipliedBy(3));
    }

    public void heartbeat(String operatorId, String routeId) {
        String key = key(operatorId, routeId);
        commands.hset(key, "heartbeat", Long.toString(System.currentTimeMillis()));
        commands.expire(key, heartbeatInterval.multipliedBy(3));
    }

    public void unregister(String operatorId, String routeId) {
        commands.del(key(operatorId, routeId));
    }

    public Map<String, String> lookup(String operatorId, String routeId) {
        return commands.hgetall(key(operatorId, routeId));
    }

    @Override
    public void close() {
        connection.close();
        client.shutdown();
    }
}
