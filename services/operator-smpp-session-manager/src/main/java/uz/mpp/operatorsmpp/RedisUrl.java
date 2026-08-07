package uz.mpp.operatorsmpp;

import java.util.function.Function;

/**
 * Реальная находка (только реальным прогоном через локальный docker-compose
 * — не статичным чтением, не раньше, потому что Docker daemon был недоступен
 * весь основной срез сессии): {@code Main.java} собирал Redis URI как
 * {@code "redis://" + host + ":" + port} — вообще без пароля, в отличие от
 * КАЖДОГО другого сервиса в этом репозитории, который поддерживает
 * {@code REDIS_RUNTIME_PASSWORD} (тот же класс находки, что уже
 * задокументирован в {@code dlr-manager/README.md} "Реальная находка
 * (систематическая...)"). `OperatorRouteRegistry` не смог бы
 * зарегистрировать маршрут против любого Redis с включённым `requirepass`
 * (в т.ч. `redis-runtime` в `infra/docker/docker-compose.yml`) —
 * `connectAndBindWithRetry` ловил бы `AUTH`-ошибку тем же catch, что и
 * реальный сбой SMPP-бинда, и уходил в бесконечный повторный bind к самому
 * SMSC без всякой необходимости.
 */
public final class RedisUrl {

    private RedisUrl() {
    }

    public static String buildRuntimeUrl() {
        return buildRuntimeUrl(System::getenv);
    }

    static String buildRuntimeUrl(Function<String, String> env) {
        String override = env.apply("REDIS_RUNTIME_URL");
        if (override != null && !override.isEmpty()) {
            return override;
        }
        String host = orDefault(env.apply("REDIS_RUNTIME_HOST"), "redis-runtime.mpp.svc");
        String port = orDefault(env.apply("REDIS_RUNTIME_PORT"), "6379");
        String password = env.apply("REDIS_RUNTIME_PASSWORD");
        if (password == null || password.isEmpty()) {
            return "redis://" + host + ":" + port;
        }
        return "redis://:" + password + "@" + host + ":" + port;
    }

    private static String orDefault(String value, String fallback) {
        return (value == null || value.isEmpty()) ? fallback : value;
    }
}
