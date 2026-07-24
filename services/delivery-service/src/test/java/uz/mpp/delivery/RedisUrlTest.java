package uz.mpp.delivery;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Регрессия на находку задокументированную в services/dlr-manager/README.md:
 * k8s инжектит REDIS_RUNTIME_HOST/PORT/PASSWORD дискретно, не единую
 * REDIS_RUNTIME_URL. Тот же паттерн, что billing-service/RedisUrlTest.java.
 */
class RedisUrlTest {

    @Test
    void composesFromDiscreteVarsWithPassword() {
        String url = RedisUrl.buildRuntimeUrl(Map.of(
            "REDIS_RUNTIME_HOST", "redis.example.internal",
            "REDIS_RUNTIME_PORT", "6380",
            "REDIS_RUNTIME_PASSWORD", "r3d1s"
        )::get);
        assertEquals("redis://:r3d1s@redis.example.internal:6380/0", url);
    }

    @Test
    void composesWithoutPassword() {
        String url = RedisUrl.buildRuntimeUrl(Map.of("REDIS_RUNTIME_HOST", "redis.example.internal")::get);
        assertEquals("redis://redis.example.internal:6379/0", url);
    }

    @Test
    void defaultsToInClusterHostnameWithoutAnyVars() {
        String url = RedisUrl.buildRuntimeUrl(Map.<String, String>of()::get);
        assertEquals("redis://redis-runtime.mpp.svc:6379/0", url);
    }

    @Test
    void explicitOverrideTakesPriority() {
        String url = RedisUrl.buildRuntimeUrl(Map.of(
            "REDIS_RUNTIME_URL", "redis://explicit-override/0",
            "REDIS_RUNTIME_HOST", "should-be-ignored"
        )::get);
        assertEquals("redis://explicit-override/0", url);
    }
}
