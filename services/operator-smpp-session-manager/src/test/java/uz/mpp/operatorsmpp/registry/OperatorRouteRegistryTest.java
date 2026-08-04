package uz.mpp.operatorsmpp.registry;

import io.lettuce.core.RedisClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Реальный Redis (brew, локально) — та же инфраструктура, что services/partner-smpp-gateway. */
class OperatorRouteRegistryTest {

    private OperatorRouteRegistry registry;

    @BeforeEach
    void setUp() {
        String uri = System.getenv().getOrDefault("OPERATOR_SMPP_SESSION_MANAGER_TEST_REDIS_URI", "redis://localhost:6379");
        try {
            RedisClient probe = RedisClient.create(uri);
            probe.connect().close();
            probe.shutdown();
        } catch (Exception e) {
            assumeTrue(false, "Redis недоступен на " + uri + " — пропуск");
        }
        registry = new OperatorRouteRegistry(uri, "operator-smpp-session-manager-test-0", Duration.ofSeconds(5));
        registry.unregister("beeline_uz", "route-1");
    }

    @AfterEach
    void tearDown() {
        if (registry != null) {
            registry.unregister("beeline_uz", "route-1");
            registry.close();
        }
    }

    @Test
    void registerWritesExpectedFields() {
        registry.register("beeline_uz", "route-1", 3L, "10.0.0.9:2775");
        Map<String, String> fields = registry.lookup("beeline_uz", "route-1");
        assertEquals("SMPP", fields.get("protocol"));
        assertEquals("operator-smpp-session-manager-test-0", fields.get("owning_instance_id"));
        assertEquals("10.0.0.9:2775", fields.get("endpoint"));
        assertEquals("3", fields.get("route_epoch"));
        assertNotNull(fields.get("heartbeat"));
    }

    @Test
    void unregisterRemovesEntry() {
        registry.register("beeline_uz", "route-1", 1L, "10.0.0.9:2775");
        registry.unregister("beeline_uz", "route-1");
        assertTrue(registry.lookup("beeline_uz", "route-1").isEmpty());
    }
}
