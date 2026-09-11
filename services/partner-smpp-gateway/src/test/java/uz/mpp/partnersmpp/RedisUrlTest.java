package uz.mpp.partnersmpp;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RedisUrlTest {

    @Test
    void composesExternalSecretFieldsAndEscapesPassword() {
        Map<String, String> env = Map.of(
            "REDIS_RUNTIME_HOST", "redis.internal",
            "REDIS_RUNTIME_PORT", "6380",
            "REDIS_RUNTIME_PASSWORD", "p@ss:/ word"
        );
        assertEquals(
            "redis://:p%40ss%3A%2F%20word@redis.internal:6380",
            RedisUrl.buildRuntimeUrl(env::get)
        );
    }

    @Test
    void supportsNoPasswordForLocalDevelopment() {
        assertEquals("redis://localhost:6379", RedisUrl.buildRuntimeUrl(Map.of(
            "REDIS_RUNTIME_HOST", "localhost",
            "REDIS_RUNTIME_PORT", "6379"
        )::get));
    }

    @Test
    void explicitUrlOverrideWins() {
        assertEquals("rediss://override:6380/0", RedisUrl.buildRuntimeUrl(Map.of(
            "REDIS_RUNTIME_URL", "rediss://override:6380/0",
            "REDIS_RUNTIME_HOST", "ignored"
        )::get));
    }
}
