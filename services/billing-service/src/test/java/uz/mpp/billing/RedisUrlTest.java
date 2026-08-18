package uz.mpp.billing;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Регрессия на находку задокументированную в services/dlr-manager/README.md:
 * k8s инжектит REDIS_BILLING_HOST/PORT/PASSWORD дискретно, не единую
 * REDIS_BILLING_URL. Тестируется через package-private
 * {@link RedisUrl#buildBillingUrl(java.util.function.Function)} с фейковой
 * lookup-функцией — System.getenv() неизменяем из JUnit.
 */
class RedisUrlTest {

    private static Map<String, String> env(Map<String, String> vars) {
        return vars;
    }

    @Test
    void composesFromDiscreteVarsWithPassword() {
        String url = RedisUrl.buildBillingUrl(env(Map.of(
            "REDIS_BILLING_HOST", "redis.example.internal",
            "REDIS_BILLING_PORT", "6380",
            "REDIS_BILLING_PASSWORD", "r3d1s"
        ))::get);
        assertEquals("redis://:r3d1s@redis.example.internal:6380/0", url);
    }

    @Test
    void composesWithoutPassword() {
        String url = RedisUrl.buildBillingUrl(env(Map.of("REDIS_BILLING_HOST", "redis.example.internal"))::get);
        assertEquals("redis://redis.example.internal:6379/0", url);
    }

    @Test
    void defaultsToInClusterHostnameWithoutAnyVars() {
        String url = RedisUrl.buildBillingUrl(env(Map.of())::get);
        assertEquals("redis://redis-billing.mpp.svc:6379/0", url);
    }

    @Test
    void explicitOverrideTakesPriority() {
        String url = RedisUrl.buildBillingUrl(env(Map.of(
            "REDIS_BILLING_URL", "redis://explicit-override/0",
            "REDIS_BILLING_HOST", "should-be-ignored"
        ))::get);
        assertEquals("redis://explicit-override/0", url);
    }

    // buildConfigurationUrl — Фаза 5a (multi-tenancy в Billing Service):
    // тот же дискретный host/port/password паттерн, третий Redis-инстанс
    // платформы (Configuration Redis), до этой фазы billing-service к нему
    // вообще не подключался.
    @Test
    void configurationUrlComposesFromDiscreteVarsWithPassword() {
        String url = RedisUrl.buildConfigurationUrl(env(Map.of(
            "REDIS_CONFIGURATION_HOST", "redis-config.example.internal",
            "REDIS_CONFIGURATION_PORT", "6381",
            "REDIS_CONFIGURATION_PASSWORD", "cfgpass"
        ))::get);
        assertEquals("redis://:cfgpass@redis-config.example.internal:6381/0", url);
    }

    @Test
    void configurationUrlDefaultsToInClusterHostnameWithoutAnyVars() {
        String url = RedisUrl.buildConfigurationUrl(env(Map.of())::get);
        assertEquals("redis://redis-configuration.mpp.svc:6379/0", url);
    }
}
