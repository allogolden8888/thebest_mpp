package uz.mpp.partnersmpp.config;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * {@link RedisUrl} нигде не был протестирован до этой правки, хотя уже
 * содержал в себе (для {@code buildRuntimeUrl}) регрессию того же класса
 * находки, что {@code operator-smpp-session-manager/RedisUrlTest} уже
 * покрывает для своего сервиса: {@code Main.java} до правки собирал
 * Runtime Redis URI без пароля вообще (реальный live-прогон против
 * docker-compose, не статичное чтение, обнаружил бы это на первом же
 * {@code register()} — {@code SessionRedisRegistry} не смог бы
 * аутентифицироваться против {@code redis-runtime} с {@code requirepass}).
 */
class RedisUrlTest {

    @Test
    void configurationUrlComposesFromDiscreteVarsWithPassword() {
        String url = RedisUrl.buildConfigurationUrl(Map.of(
            "REDIS_CONFIGURATION_HOST", "redis-cfg.example.internal",
            "REDIS_CONFIGURATION_PORT", "6380",
            "REDIS_CONFIGURATION_PASSWORD", "r3d1s"
        )::get);
        assertEquals("redis://:r3d1s@redis-cfg.example.internal:6380/0", url);
    }

    @Test
    void configurationUrlComposesWithoutPassword() {
        String url = RedisUrl.buildConfigurationUrl(Map.of("REDIS_CONFIGURATION_HOST", "redis-cfg.example.internal")::get);
        assertEquals("redis://redis-cfg.example.internal:6379/0", url);
    }

    @Test
    void configurationUrlDefaultsToInClusterHostnameWithoutAnyVars() {
        String url = RedisUrl.buildConfigurationUrl(Map.<String, String>of()::get);
        assertEquals("redis://redis-configuration.mpp.svc:6379/0", url);
    }

    @Test
    void configurationUrlExplicitOverrideTakesPriority() {
        String url = RedisUrl.buildConfigurationUrl(Map.of(
            "REDIS_CONFIGURATION_URL", "redis://explicit-override/0",
            "REDIS_CONFIGURATION_HOST", "should-be-ignored"
        )::get);
        assertEquals("redis://explicit-override/0", url);
    }

    @Test
    void runtimeUrlComposesFromDiscreteVarsWithPassword() {
        String url = RedisUrl.buildRuntimeUrl(Map.of(
            "REDIS_RUNTIME_HOST", "redis-runtime.example.internal",
            "REDIS_RUNTIME_PORT", "6380",
            "REDIS_RUNTIME_PASSWORD", "r3d1s"
        )::get);
        assertEquals("redis://:r3d1s@redis-runtime.example.internal:6380", url);
    }

    @Test
    void runtimeUrlComposesWithoutPassword() {
        String url = RedisUrl.buildRuntimeUrl(Map.of("REDIS_RUNTIME_HOST", "redis-runtime.example.internal")::get);
        assertEquals("redis://redis-runtime.example.internal:6379", url);
    }

    @Test
    void runtimeUrlDefaultsToInClusterHostnameWithoutAnyVars() {
        String url = RedisUrl.buildRuntimeUrl(Map.<String, String>of()::get);
        assertEquals("redis://redis-runtime.mpp.svc:6379", url);
    }

    @Test
    void runtimeUrlExplicitOverrideTakesPriority() {
        String url = RedisUrl.buildRuntimeUrl(Map.of(
            "REDIS_RUNTIME_URL", "redis://explicit-override/0",
            "REDIS_RUNTIME_HOST", "should-be-ignored"
        )::get);
        assertEquals("redis://explicit-override/0", url);
    }
}
