package uz.mpp.scheduler.standard.core;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Тот же паттерн, что {@code uz.mpp.billing.RedisUrlTest} в billing-service
 * — k8s инжектит REDIS_RUNTIME_HOST/PORT/PASSWORD дискретно, не единую
 * REDIS_RUNTIME_URL. Package-private {@link RedisUrl#buildRuntimeUrl(java.util.function.Function)}
 * с фейковой lookup-функцией — System.getenv() неизменяем из JUnit.
 */
class RedisUrlTest {

    private static Map<String, String> env(Map<String, String> vars) {
        return vars;
    }

    @Test
    void composesFromDiscreteVarsWithPassword() {
        String url = RedisUrl.buildRuntimeUrl(env(Map.of(
            "REDIS_RUNTIME_HOST", "redis.example.internal",
            "REDIS_RUNTIME_PORT", "6380",
            "REDIS_RUNTIME_PASSWORD", "r3d1s"
        ))::get);
        assertEquals("redis://:r3d1s@redis.example.internal:6380/0", url);
    }

    @Test
    void composesWithoutPassword() {
        String url = RedisUrl.buildRuntimeUrl(env(Map.of("REDIS_RUNTIME_HOST", "redis.example.internal"))::get);
        assertEquals("redis://redis.example.internal:6379/0", url);
    }

    @Test
    void defaultsToInClusterHostnameWithoutAnyVars() {
        String url = RedisUrl.buildRuntimeUrl(env(Map.of())::get);
        assertEquals("redis://redis-runtime.mpp.svc:6379/0", url);
    }

    @Test
    void explicitOverrideTakesPriority() {
        String url = RedisUrl.buildRuntimeUrl(env(Map.of(
            "REDIS_RUNTIME_URL", "redis://explicit-override/0",
            "REDIS_RUNTIME_HOST", "should-be-ignored"
        ))::get);
        assertEquals("redis://explicit-override/0", url);
    }
}
