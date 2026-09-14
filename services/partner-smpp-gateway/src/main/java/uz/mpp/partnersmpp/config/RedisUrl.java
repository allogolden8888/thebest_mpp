package uz.mpp.partnersmpp.config;

import java.util.function.Function;

/**
 * Configuration/Runtime Redis connection URL — зеркалит {@code
 * billing-service/RedisUrl.buildConfigurationUrl()} 1:1 (тот же дискретный
 * набор env vars, что k8s реально инжектит через {@code envFrom:
 * secretRef} — {@code REDIS_CONFIGURATION_URL}/{@code REDIS_RUNTIME_URL}
 * остаются explicit override только для локальной разработки/тестов, см.
 * billing-service javadoc для полного разбора находки).
 *
 * <p>Этот сервис раньше вообще не подключался к Configuration Redis — до
 * {@link PartnerConfigStore} партнёрский SMPP-конфиг читался один раз из
 * статического {@code PARTNER_CONFIG_PATH} файла при старте (см. {@code
 * Main.java} до этой правки), тот же пробел, что билинг закрыл в Фазе 5a.
 *
 * <p><b>{@link #buildRuntimeUrl()} — найдено при реальном live-прогоне
 * против {@code docker-compose}, не статичным чтением.</b> {@code
 * Main.java} до этой правки собирал Runtime Redis URI как {@code "redis://"
 * + host + ":" + port} — вообще без пароля, тот же класс систематической
 * находки, что уже задокументирован и исправлен в {@code
 * operator-smpp-session-manager/RedisUrl.java}/{@code
 * delivery-service/RedisUrl.java}/{@code dlr-manager/README.md} (несколько
 * независимых сервисов в этом репозитории делали одну и ту же ошибку).
 * {@code SessionRedisRegistry} не смог бы вообще подключиться к {@code
 * redis-runtime} с включённым {@code requirepass} (в т.ч. в {@code
 * infra/docker/docker-compose.yml}) — под падал бы на самом первом
 * {@code register()} на реальном bind'е.
 */
public final class RedisUrl {

    private RedisUrl() {
    }

    public static String buildConfigurationUrl() {
        return buildConfigurationUrl(System::getenv);
    }

    static String buildConfigurationUrl(Function<String, String> env) {
        String override = env.apply("REDIS_CONFIGURATION_URL");
        if (override != null && !override.isEmpty()) {
            return override;
        }
        String host = orDefault(env.apply("REDIS_CONFIGURATION_HOST"), "redis-configuration.mpp.svc");
        String port = orDefault(env.apply("REDIS_CONFIGURATION_PORT"), "6379");
        String password = env.apply("REDIS_CONFIGURATION_PASSWORD");
        if (password == null || password.isEmpty()) {
            return "redis://" + host + ":" + port + "/0";
        }
        return "redis://:" + password + "@" + host + ":" + port + "/0";
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
