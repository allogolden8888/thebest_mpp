package uz.mpp.partnersmpp.registry;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Реальный Redis (brew, локально — тот же приём, что PostgreSQL в
 * migrations/README.md) — не мок. Пропускается (Assumption), если Redis
 * недоступен на localhost:6379 в этой песочнице.
 */
class SessionRedisRegistryTest {

    private SessionRedisRegistry registry;
    private String systemId;
    private String redisUri;

    @BeforeEach
    void setUp() {
        redisUri = System.getenv().getOrDefault("PARTNER_SMPP_GATEWAY_TEST_REDIS_URI", "redis://localhost:6379");
        try {
            RedisClient probe = RedisClient.create(redisUri);
            probe.connect().close();
            probe.shutdown();
        } catch (Exception e) {
            assumeTrue(false, "Redis недоступен на " + redisUri + " (" + e.getMessage() + ") — пропуск");
        }
        registry = new SessionRedisRegistry(redisUri, "partner-smpp-gateway-test-0", Duration.ofSeconds(5));
        systemId = "click_uz_main_" + UUID.randomUUID();
    }

    @AfterEach
    void tearDown() {
        if (registry != null) {
            registry.close();
        }
    }

    @Test
    void registerWritesAllExpectedFields() {
        registry.register("acme", systemId, "session-1", 7L, "10.0.0.5:2775");

        Map<String, String> fields = registry.lookup("acme", systemId);
        assertEquals("session-1", fields.get("session_id"));
        assertEquals("partner-smpp-gateway-test-0", fields.get("gateway_instance_id"));
        assertEquals("10.0.0.5:2775", fields.get("endpoint"));
        assertEquals("7", fields.get("session_epoch"));
        assertNotNull(fields.get("heartbeat"));
    }

    @Test
    void heartbeatUpdatesTimestampWithoutChangingOtherFields() throws InterruptedException {
        registry.register("acme", systemId, "session-1", 1L, "10.0.0.5:2775");
        String firstHeartbeat = registry.lookup("acme", systemId).get("heartbeat");

        Thread.sleep(10);
        assertTrue(registry.heartbeat("acme", systemId, 1L));

        Map<String, String> fields = registry.lookup("acme", systemId);
        assertNotEquals(firstHeartbeat, fields.get("heartbeat"));
        assertEquals("session-1", fields.get("session_id"), "heartbeat не должен менять session_id");
    }

    @Test
    void unregisterRemovesEntry() {
        registry.register("acme", systemId, "session-1", 1L, "10.0.0.5:2775");
        assertTrue(registry.unregister("acme", systemId, 1L));
        assertTrue(registry.lookup("acme", systemId).isEmpty());
    }

    @Test
    void staleHeartbeatCannotTouchReconnectedSession() throws InterruptedException {
        registry.register("acme", systemId, "session-new", 2L, "10.0.0.6:2775");
        String firstHeartbeat = registry.lookup("acme", systemId).get("heartbeat");

        Thread.sleep(10);
        assertFalse(registry.heartbeat("acme", systemId, 1L));

        Map<String, String> fields = registry.lookup("acme", systemId);
        assertEquals(firstHeartbeat, fields.get("heartbeat"));
        assertEquals("session-new", fields.get("session_id"));
        assertEquals("2", fields.get("session_epoch"));
    }

    @Test
    void heartbeatCannotResurrectMissingSession() {
        assertFalse(registry.heartbeat("acme", systemId, 1L));
        assertTrue(registry.lookup("acme", systemId).isEmpty());
    }

    @Test
    void heartbeatRecreatesCurrentRegistrationLostDuringRedisOutage() {
        registry.register("acme", systemId, "session-current", 3L, "10.0.0.7:2775");
        RedisClient external = RedisClient.create(redisUri);
        try (StatefulRedisConnection<String, String> connection = external.connect()) {
            connection.sync().del("smpp:partner_session:acme:" + systemId);
        } finally {
            external.shutdown();
        }

        assertTrue(registry.heartbeat("acme", systemId, 3L));

        Map<String, String> fields = registry.lookup("acme", systemId);
        assertEquals("session-current", fields.get("session_id"));
        assertEquals("3", fields.get("session_epoch"));
        assertEquals("10.0.0.7:2775", fields.get("endpoint"));
    }

    @Test
    void staleUnregisterCannotDeleteReconnectedSession() {
        registry.register("acme", systemId, "session-new", 2L, "10.0.0.6:2775");

        assertFalse(registry.unregister("acme", systemId, 1L));

        assertEquals("session-new", registry.lookup("acme", systemId).get("session_id"));
    }

    @Test
    void lateOldRegisterCannotOverwriteNewerLocalEpoch() {
        registry.register("acme", systemId, "session-new", 2L, "10.0.0.6:2775");
        registry.register("acme", systemId, "session-old", 1L, "10.0.0.5:2775");

        Map<String, String> fields = registry.lookup("acme", systemId);
        assertEquals("session-new", fields.get("session_id"));
        assertEquals("2", fields.get("session_epoch"));
        assertFalse(registry.heartbeat("acme", systemId, 1L));
    }
}
