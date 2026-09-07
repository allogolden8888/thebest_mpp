package uz.mpp.delivery;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.sync.RedisCommands;
import java.util.Map;

/**
 * {@code resolve_gateway_instance} (service_internal_methods.md §1.8) —
 * {@code operator_route:{operator_id}:{route_id}} HASH в Runtime Redis
 * (`data_infrastructure_spec.md` §287): {@code protocol, owning_instance_id,
 * endpoint, route_epoch, heartbeat} — единый registry для Operator SMPP
 * Session Manager (protocol=SMPP) и Operator HTTP Gateway (protocol=HTTP),
 * тот же принцип protocol-aware dispatch, что уже задокументирован в
 * `routing-service` для выбора протокола маршрута (другой шаг того же
 * pipeline). Ни один из двух реальных owner-сервисов этого registry ещё не
 * реализован в этом репозитории (владелец — Субагент 1) — запись здесь
 * никогда не появится живьём, live не проверено.
 *
 * MEDIUM находка кодревью: то же соединение-на-вызов, что было в
 * {@code MessageContextStore} — исправлено тем же способом, один общий
 * {@code StatefulRedisConnection} на весь жизненный цикл сервиса.
 */
public final class GatewayRegistry {

    public record GatewayEndpoint(String protocol, String owningInstanceId, String endpoint, String routeEpoch) {
    }

    private final RedisClient client;
    private final StatefulRedisConnection<String, String> connection;

    public GatewayRegistry(String redisUrl) {
        this.client = RedisClient.create(redisUrl);
        this.connection = client.connect();
    }

    /** Асинхронный вариант — см. обоснование в MessageContextStore.fetchAsync. */
    public RedisFuture<java.util.Map<String, String>> resolveAsync(String operatorId, String routeId) {
        return connection.async().hgetall("operator_route:" + operatorId + ":" + routeId);
    }

    /** Разбор результата {@link #resolveAsync} — та же логика, что в {@link #resolve}. */
    public static GatewayEndpoint toEndpoint(Map<String, String> fields) {
        if (fields == null || fields.isEmpty() || !fields.containsKey("endpoint")) {
            return null;
        }
        return new GatewayEndpoint(
            fields.get("protocol"), fields.get("owning_instance_id"), fields.get("endpoint"), fields.get("route_epoch"));
    }

    public GatewayEndpoint resolve(String operatorId, String routeId) {
        RedisCommands<String, String> commands = connection.sync();
        Map<String, String> fields = commands.hgetall("operator_route:" + operatorId + ":" + routeId);
        if (fields.isEmpty() || !fields.containsKey("endpoint")) {
            return null;
        }
        return new GatewayEndpoint(
            fields.get("protocol"), fields.get("owning_instance_id"), fields.get("endpoint"), fields.get("route_epoch"));
    }

    public void close() {
        connection.close();
        client.shutdown();
    }
}
