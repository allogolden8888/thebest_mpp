package uz.mpp.delivery;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
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
 */
public final class GatewayRegistry {

    public record GatewayEndpoint(String protocol, String owningInstanceId, String endpoint, String routeEpoch) {
    }

    private final RedisClient client;

    public GatewayRegistry(String redisUrl) {
        this.client = RedisClient.create(redisUrl);
    }

    public GatewayEndpoint resolve(String operatorId, String routeId) {
        try (StatefulRedisConnection<String, String> connection = client.connect()) {
            RedisCommands<String, String> commands = connection.sync();
            Map<String, String> fields = commands.hgetall("operator_route:" + operatorId + ":" + routeId);
            if (fields.isEmpty() || !fields.containsKey("endpoint")) {
                return null;
            }
            return new GatewayEndpoint(
                fields.get("protocol"), fields.get("owning_instance_id"), fields.get("endpoint"), fields.get("route_epoch"));
        }
    }

    public void close() {
        client.shutdown();
    }
}
