package uz.mpp.billingoutbox;

import java.util.function.Function;

/**
 * Тот же хелпер, что уже есть в billing-service/dlr-manager (осознанное
 * дублирование — общего Java-модуля между этими сервисами в репозитории
 * нет, тот же принятый паттерн, что и для credential_ref_to_env_var в
 * Rust/Go/Java).
 *
 * <p><b>Реальный баг, ради которого это здесь появилось:</b> Main.java
 * собирал URI как {@code "redis://" + host + ":" + port} — без пароля
 * вообще. Redis этой платформы защищён {@code requirepass} и в локальном
 * docker-compose, и в проде (infra/terraform/redis.tf), поэтому сервис не
 * мог аутентифицироваться ни в одном окружении: биллинговый outbox не
 * вычитывался, топик {@code billing.ledger} оставался пустым, и
 * {@code billing.billing_ledger} никогда не наполнялся реальными
 * списаниями — в админской ленте биллинга были видны только старые
 * строки, записанные тестами напрямую в Postgres.
 *
 * <p>{@code REDIS_BILLING_URL} оставлен как явный override для локальной
 * разработки; в k8s инжектятся дискретные HOST/PORT/PASSWORD
 * ({@code envFrom: secretRef}), не единый URL.
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
