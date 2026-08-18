package uz.mpp.billing;

import java.util.function.Function;

/**
 * Реальная находка (найдена при реализации {@code dlr-manager}, полный
 * разбор — {@code services/dlr-manager/README.md}, "Реальная находка
 * (систематическая...)"): k8s инжектит {@code REDIS_BILLING_HOST}/{@code
 * REDIS_BILLING_PORT}/{@code REDIS_BILLING_PASSWORD} дискретно ({@code
 * envFrom: secretRef}), не единую {@code REDIS_BILLING_URL}, которую этот
 * сервис читал раньше — в реальном кластере он никогда бы не подключился.
 * {@code REDIS_BILLING_URL} оставлена как явный override для локальной
 * разработки/тестов.
 *
 * <p>Принимает lookup-функцию параметром (не читает {@code System.getenv()}
 * напрямую) — {@code System.getenv()} неизменяем из JUnit без reflection-хаков,
 * так тестируются все ветки (с паролем/без/explicit override), не только
 * дефолтный путь.
 */
public final class RedisUrl {

    private RedisUrl() {
    }

    public static String buildBillingUrl() {
        return buildBillingUrl(System::getenv);
    }

    static String buildBillingUrl(Function<String, String> env) {
        return build(env, "REDIS_BILLING_URL", "REDIS_BILLING_HOST", "REDIS_BILLING_PORT", "REDIS_BILLING_PASSWORD", "redis-billing.mpp.svc");
    }

    /**
     * Configuration Redis (третий Redis-инстанс платформы, {@code redis-configuration}
     * в {@code infra/docker/docker-compose.yml}) — до Фазы 5a (multi-tenancy в
     * Billing Service) billing-service к нему вообще не подключался, только к
     * Billing Redis. Читает {@code config:current:billing_tariff:{partner_id}}/
     * {@code config:version:billing_tariff:{partner_id}:{version}} — те же ключи,
     * что уже пишет {@code config-cache-projector} (см. {@link TariffCache}).
     */
    public static String buildConfigurationUrl() {
        return buildConfigurationUrl(System::getenv);
    }

    static String buildConfigurationUrl(Function<String, String> env) {
        return build(env, "REDIS_CONFIGURATION_URL", "REDIS_CONFIGURATION_HOST", "REDIS_CONFIGURATION_PORT", "REDIS_CONFIGURATION_PASSWORD", "redis-configuration.mpp.svc");
    }

    private static String build(Function<String, String> env, String urlVar, String hostVar, String portVar, String passwordVar, String defaultHost) {
        String override = env.apply(urlVar);
        if (override != null && !override.isEmpty()) {
            return override;
        }
        String host = orDefault(env.apply(hostVar), defaultHost);
        String port = orDefault(env.apply(portVar), "6379");
        String password = env.apply(passwordVar);
        if (password == null || password.isEmpty()) {
            return "redis://" + host + ":" + port + "/0";
        }
        return "redis://:" + password + "@" + host + ":" + port + "/0";
    }

    private static String orDefault(String value, String fallback) {
        return (value == null || value.isEmpty()) ? fallback : value;
    }
}
