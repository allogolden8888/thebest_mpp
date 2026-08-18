package uz.mpp.operatorsmpp.registry;

import io.lettuce.core.RedisClient;
import io.lettuce.core.ScriptOutputType;
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

    // Реальная находка (дважды воспроизведена на практике, не гипотетическая):
    // graceful restart мог гоняться так, что СТАРЫЙ инстанс (shutdown hook,
    // Main.java) вызывал unregister() ПОСЛЕ того, как НОВЫЙ инстанс уже успел
    // выполнить свежий register() — старый unregister() был безусловным DEL,
    // сносил свежую регистрацию нового инстанса целиком. Следующий heartbeat()
    // (30с таймер, работает независимо от того, кто "владеет" ключом) находил
    // ключ отсутствующим и молча ПЕРЕСОЗДАВАЛ его голым HSET одного поля
    // (heartbeat) — TTL продлевался, ключ выглядел "живым" в Redis, но без
    // endpoint/protocol/owning_instance_id/route_epoch. GatewayRegistry.resolve
    // (delivery-service) не находил endpoint и возвращал GATEWAY_INSTANCE_NOT_FOUND
    // для 100% сообщений, при том что SMPP-сессия у оператора была реально
    // жива и bound. И unregister(), и heartbeat() теперь скоуплены атомарным
    // Lua-скриптом: трогают ключ только если owning_instance_id всё ещё
    // совпадает с ЭТИМ инстансом (или ключ вообще не существует — для
    // heartbeat это значит "не пересоздавать", не "создать голый").
    private static final String UNREGISTER_IF_OWNER_SCRIPT =
        "if redis.call('HGET', KEYS[1], 'owning_instance_id') == ARGV[1] then "
        + "return redis.call('DEL', KEYS[1]) else return 0 end";
    private static final String HEARTBEAT_IF_OWNER_SCRIPT =
        "if redis.call('HGET', KEYS[1], 'owning_instance_id') == ARGV[1] then "
        + "redis.call('HSET', KEYS[1], 'heartbeat', ARGV[2]) "
        + "redis.call('EXPIRE', KEYS[1], ARGV[3]) return 1 else return 0 end";

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
        commands.eval(HEARTBEAT_IF_OWNER_SCRIPT, ScriptOutputType.INTEGER,
            new String[] {key},
            owningInstanceId, Long.toString(System.currentTimeMillis()), Long.toString(heartbeatInterval.multipliedBy(3).toSeconds()));
    }

    public void unregister(String operatorId, String routeId) {
        String key = key(operatorId, routeId);
        commands.eval(UNREGISTER_IF_OWNER_SCRIPT, ScriptOutputType.INTEGER, new String[] {key}, owningInstanceId);
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
