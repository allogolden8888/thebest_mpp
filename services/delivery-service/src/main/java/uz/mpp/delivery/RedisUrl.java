package uz.mpp.delivery;

import java.util.function.Function;

/**
 * Реальная находка (найдена при реализации {@code dlr-manager}, полный
 * разбор — {@code services/dlr-manager/README.md}, "Реальная находка
 * (систематическая...)"): k8s инжектит {@code REDIS_RUNTIME_HOST}/{@code
 * REDIS_RUNTIME_PORT}/{@code REDIS_RUNTIME_PASSWORD} дискретно ({@code
 * envFrom: secretRef}), не единую {@code REDIS_RUNTIME_URL}, которую этот
 * сервис читал раньше — в реальном кластере он никогда бы не подключился.
 * {@code REDIS_RUNTIME_URL} оставлена как явный override.
 *
 * <p>Принимает lookup-функцию параметром (не {@code System.getenv()}
 * напрямую) — тестируется без reflection-хаков, тот же паттерн, что
 * {@code billing-service/RedisUrl.java}.
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
            return "redis://" + host + ":" + port + "/0";
        }
        return "redis://:" + password + "@" + host + ":" + port + "/0";
    }

    private static String orDefault(String value, String fallback) {
        return (value == null || value.isEmpty()) ? fallback : value;
    }
}
