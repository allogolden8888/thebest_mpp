package uz.mpp.partnersmpp.registry;

import io.lettuce.core.RedisClient;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * register_session + heartbeat_tick (service_internal_methods.md §1.2) в
 * Runtime Redis — `smpp:partner_session:{partner_id}:{system_id}` HASH
 * (data_infrastructure_spec.md §2.1: session_id, gateway_instance_id,
 * endpoint, session_epoch, heartbeat; TTL = 3× heartbeat interval).
 */
public final class SessionRedisRegistry implements AutoCloseable {

    private static final String REGISTER_SCRIPT = """
        redis.call('HSET', KEYS[1],
          'session_id', ARGV[1],
          'gateway_instance_id', ARGV[2],
          'endpoint', ARGV[3],
          'session_epoch', ARGV[4],
          'heartbeat', ARGV[5])
        redis.call('PEXPIRE', KEYS[1], ARGV[6])
        return 1
        """;

    private static final String HEARTBEAT_IF_CURRENT_OR_MISSING_SCRIPT = """
        if redis.call('EXISTS', KEYS[1]) == 0
           or (redis.call('HGET', KEYS[1], 'gateway_instance_id') == ARGV[2]
               and redis.call('HGET', KEYS[1], 'session_epoch') == ARGV[4]) then
          redis.call('HSET', KEYS[1],
            'session_id', ARGV[1],
            'gateway_instance_id', ARGV[2],
            'endpoint', ARGV[3],
            'session_epoch', ARGV[4],
            'heartbeat', ARGV[5])
          redis.call('PEXPIRE', KEYS[1], ARGV[6])
          return 1
        end
        return 0
        """;

    private static final String UNREGISTER_IF_CURRENT_SCRIPT = """
        if redis.call('HGET', KEYS[1], 'gateway_instance_id') == ARGV[1]
           and redis.call('HGET', KEYS[1], 'session_epoch') == ARGV[2] then
          return redis.call('DEL', KEYS[1])
        end
        return 0
        """;

    private final RedisClient client;
    private final StatefulRedisConnection<String, String> connection;
    private final RedisCommands<String, String> commands;
    private final String gatewayInstanceId;
    private final Duration heartbeatInterval;
    private final Map<String, Registration> activeRegistrations = new ConcurrentHashMap<>();
    private final Object[] keyLocks = new Object[64];

    private record Registration(String sessionId, long sessionEpoch, String endpoint) {
    }

    public SessionRedisRegistry(String redisUri, String gatewayInstanceId, Duration heartbeatInterval) {
        if (heartbeatInterval.isZero() || heartbeatInterval.isNegative()) {
            throw new IllegalArgumentException("heartbeatInterval must be positive");
        }
        this.client = RedisClient.create(redisUri);
        this.connection = client.connect();
        this.commands = connection.sync();
        this.gatewayInstanceId = gatewayInstanceId;
        this.heartbeatInterval = heartbeatInterval;
        for (int i = 0; i < keyLocks.length; i++) {
            keyLocks[i] = new Object();
        }
    }

    private static String key(String partnerId, String systemId) {
        return "smpp:partner_session:" + partnerId + ":" + systemId;
    }

    private Object lockFor(String key) {
        return keyLocks[(key.hashCode() & Integer.MAX_VALUE) % keyLocks.length];
    }

    private long ttlMillis() {
        return heartbeatInterval.multipliedBy(3).toMillis();
    }

    /** register_session. */
    public void register(String partnerId, String systemId, String sessionId, long sessionEpoch, String endpoint) {
        String key = key(partnerId, systemId);
        Registration registration = new Registration(sessionId, sessionEpoch, endpoint);
        synchronized (lockFor(key)) {
            Registration current = activeRegistrations.get(key);
            if (current != null && current.sessionEpoch() > sessionEpoch) {
                // Два bind callback могут завершиться не по порядку. Epoch
                // выдаётся ChannelRegistry монотонно: запоздавшая старая
                // регистрация не имеет права перезаписать уже принятую новую.
                return;
            }
            activeRegistrations.put(key, registration);
            try {
                commands.eval(
                    REGISTER_SCRIPT,
                    ScriptOutputType.INTEGER,
                    new String[]{key},
                    sessionId,
                    gatewayInstanceId,
                    endpoint,
                    Long.toString(sessionEpoch),
                    Long.toString(System.currentTimeMillis()),
                    Long.toString(ttlMillis())
                );
            } catch (RuntimeException error) {
                activeRegistrations.remove(key, registration);
                throw error;
            }
        }
    }

    /**
     * heartbeat_tick — атомарно обновляет heartbeat и продлевает TTL, только
     * если Redis всё ещё содержит именно эту epoch данного инстанса. Если TTL
     * истёк во время сбоя Redis, запись текущей локальной сессии создаётся
     * заново. Локальная блокировка сериализует это с unregister, поэтому
     * запоздавший tick не воскресит уже закрытую запись и не затронет reconnect.
     *
     * @return true, если текущая сессия была обновлена
     */
    public boolean heartbeat(String partnerId, String systemId, long sessionEpoch) {
        String key = key(partnerId, systemId);
        synchronized (lockFor(key)) {
            Registration registration = activeRegistrations.get(key);
            if (registration == null || registration.sessionEpoch() != sessionEpoch) {
                return false;
            }
            Long updated = commands.eval(
                HEARTBEAT_IF_CURRENT_OR_MISSING_SCRIPT,
                ScriptOutputType.INTEGER,
                new String[]{key},
                registration.sessionId(),
                gatewayInstanceId,
                registration.endpoint(),
                Long.toString(sessionEpoch),
                Long.toString(System.currentTimeMillis()),
                Long.toString(ttlMillis())
            );
            return updated != null && updated == 1L;
        }
    }

    /** Removes only the session that initiated the unregister. */
    public boolean unregister(String partnerId, String systemId, long sessionEpoch) {
        String key = key(partnerId, systemId);
        synchronized (lockFor(key)) {
            activeRegistrations.computeIfPresent(
                key,
                (ignored, current) -> current.sessionEpoch() == sessionEpoch ? null : current
            );
            Long deleted = commands.eval(
                UNREGISTER_IF_CURRENT_SCRIPT,
                ScriptOutputType.INTEGER,
                new String[]{key},
                gatewayInstanceId,
                Long.toString(sessionEpoch)
            );
            return deleted != null && deleted == 1L;
        }
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
