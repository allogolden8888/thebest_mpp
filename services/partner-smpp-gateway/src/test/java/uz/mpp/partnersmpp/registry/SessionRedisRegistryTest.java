package uz.mpp.partnersmpp.registry;

import io.lettuce.core.RedisClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Реальный Redis (brew, локально — тот же приём, что PostgreSQL в
 * migrations/README.md) — не мок. Пропускается (Assumption), если Redis
 * недоступен на localhost:6379 в этой песочнице.
 */
class SessionRedisRegistryTest {

    private SessionRedisRegistry registry;

    @BeforeEach
    void setUp() {
        String uri = System.getenv().getOrDefault("PARTNER_SMPP_GATEWAY_TEST_REDIS_URI", "redis://localhost:6379");
        try {
            RedisClient probe = RedisClient.create(uri);
            probe.connect().close();
            probe.shutdown();
        } catch (Exception e) {
            assumeTrue(false, "Redis недоступен на " + uri + " (" + e.getMessage() + ") — пропуск");
        }
        registry = new SessionRedisRegistry(uri, "partner-smpp-gateway-test-0", Duration.ofSeconds(5));
        registry.unregister("acme", "click_uz_main"); // очистка после предыдущих прогонов
    }

    @AfterEach
    void tearDown() {
        if (registry != null) {
            registry.unregister("acme", "click_uz_main");
            registry.close();
        }
    }

    @Test
    void registerWritesAllExpectedFields() {
        registry.register("acme", "click_uz_main", "session-1", 7L, "10.0.0.5:2775");

        Map<String, String> fields = registry.lookup("acme", "click_uz_main");
        assertEquals("session-1", fields.get("session_id"));
        assertEquals("partner-smpp-gateway-test-0", fields.get("gateway_instance_id"));
        assertEquals("10.0.0.5:2775", fields.get("endpoint"));
        assertEquals("7", fields.get("session_epoch"));
        assertNotNull(fields.get("heartbeat"));
    }

    @Test
    void heartbeatUpdatesTimestampWithoutChangingOtherFields() throws InterruptedException {
        registry.register("acme", "click_uz_main", "session-1", 1L, "10.0.0.5:2775");
        String firstHeartbeat = registry.lookup("acme", "click_uz_main").get("heartbeat");

        Thread.sleep(10);
        registry.heartbeat("acme", "click_uz_main");

        Map<String, String> fields = registry.lookup("acme", "click_uz_main");
        assertNotEquals(firstHeartbeat, fields.get("heartbeat"));
        assertEquals("session-1", fields.get("session_id"), "heartbeat не должен менять session_id");
    }

    @Test
    void unregisterRemovesEntry() {
        registry.register("acme", "click_uz_main", "session-1", 1L, "10.0.0.5:2775");
        registry.unregister("acme", "click_uz_main");
        assertTrue(registry.lookup("acme", "click_uz_main").isEmpty());
    }
}
