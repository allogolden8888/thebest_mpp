package uz.mpp.operatorsmpp;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Регрессия на реальную находку (docker-compose прогон, не статичное чтение):
 * {@code Main.java} собирал Redis URI без пароля вообще, единственный сервис
 * в репозитории, который этого не делал — см. {@link RedisUrl}.
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
        assertEquals("redis://:r3d1s@redis.example.internal:6380", url);
    }

    @Test
    void composesWithoutPassword() {
        String url = RedisUrl.buildRuntimeUrl(env(Map.of("REDIS_RUNTIME_HOST", "redis.example.internal"))::get);
        assertEquals("redis://redis.example.internal:6379", url);
    }

    @Test
    void defaultsToInClusterHostnameWithoutAnyVars() {
        String url = RedisUrl.buildRuntimeUrl(env(Map.of())::get);
        assertEquals("redis://redis-runtime.mpp.svc:6379", url);
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
