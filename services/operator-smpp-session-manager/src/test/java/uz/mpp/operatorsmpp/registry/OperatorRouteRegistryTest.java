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

    /** Реальная находка (дважды воспроизведена вживую) — см. javadoc-комментарий
     * над UNREGISTER_IF_OWNER_SCRIPT в OperatorRouteRegistry: старый (уходящий)
     * инстанс не должен иметь возможности снести свежую регистрацию нового. */
    @Test
    void unregisterFromDifferentInstanceIsNoOp() {
        registry.register("beeline_uz", "route-1", 1L, "10.0.0.9:2775");

        try (OperatorRouteRegistry otherInstance = new OperatorRouteRegistry(
                System.getenv().getOrDefault("OPERATOR_SMPP_SESSION_MANAGER_TEST_REDIS_URI", "redis://localhost:6379"),
                "operator-smpp-session-manager-OTHER-instance", Duration.ofSeconds(5))) {
            otherInstance.unregister("beeline_uz", "route-1");
        }

        Map<String, String> fields = registry.lookup("beeline_uz", "route-1");
        assertEquals("10.0.0.9:2775", fields.get("endpoint"), "чужой unregister() не должен был снести регистрацию");
    }

    @Test
    void heartbeatFromDifferentInstanceDoesNotRecreateOrTouchKey() {
        try (OperatorRouteRegistry otherInstance = new OperatorRouteRegistry(
                System.getenv().getOrDefault("OPERATOR_SMPP_SESSION_MANAGER_TEST_REDIS_URI", "redis://localhost:6379"),
                "operator-smpp-session-manager-OTHER-instance", Duration.ofSeconds(5))) {
            // Ключ ещё не существует вообще — heartbeat от кого угодно не должен
            // его создавать "голым" (только с полем heartbeat, без endpoint и т.д.).
            otherInstance.heartbeat("beeline_uz", "route-1");
        }
        assertTrue(registry.lookup("beeline_uz", "route-1").isEmpty(), "heartbeat не должен создавать голый ключ на пустом месте");

        registry.register("beeline_uz", "route-1", 1L, "10.0.0.9:2775");
        try (OperatorRouteRegistry otherInstance = new OperatorRouteRegistry(
                System.getenv().getOrDefault("OPERATOR_SMPP_SESSION_MANAGER_TEST_REDIS_URI", "redis://localhost:6379"),
                "operator-smpp-session-manager-OTHER-instance", Duration.ofSeconds(5))) {
            otherInstance.heartbeat("beeline_uz", "route-1");
        }
        Map<String, String> fields = registry.lookup("beeline_uz", "route-1");
        assertEquals("10.0.0.9:2775", fields.get("endpoint"), "чужой heartbeat() не должен трогать запись другого владельца");
    }
}
