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
        String override = env.apply("REDIS_BILLING_URL");
        if (override != null && !override.isEmpty()) {
            return override;
        }
        String host = orDefault(env.apply("REDIS_BILLING_HOST"), "redis-billing.mpp.svc");
        String port = orDefault(env.apply("REDIS_BILLING_PORT"), "6379");
        String password = env.apply("REDIS_BILLING_PASSWORD");
        if (password == null || password.isEmpty()) {
            return "redis://" + host + ":" + port + "/0";
        }
        return "redis://:" + password + "@" + host + ":" + port + "/0";
    }

    private static String orDefault(String value, String fallback) {
        return (value == null || value.isEmpty()) ? fallback : value;
    }
}
