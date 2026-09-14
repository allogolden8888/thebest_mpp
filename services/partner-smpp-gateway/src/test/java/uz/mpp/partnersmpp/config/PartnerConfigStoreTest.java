package uz.mpp.partnersmpp.config;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Реальный Redis (тот же приём, что {@code SessionRedisRegistryTest}) — не
 * мок. Пишет вручную ровно те ключи, что {@code config-cache-projector}
 * реально пишет для {@code entity_type=PARTNER} ({@code
 * config:current:partner:{id}} / {@code config:version:partner:{id}:{v}},
 * см. её README/{@code projector.go}), не порождает config-cache-projector
 * целиком — эта пара классов ({@link PartnerConfigStore}) — потребитель тех
 * ключей, не сам проектор, так что достаточно писать в тех же терминах.
 */
class PartnerConfigStoreTest {

    private RedisClient rawClient;
    private PartnerConfigStore store;
    private String uri;
    private boolean redisAvailable;

    @BeforeEach
    void setUp() {
        uri = System.getenv().getOrDefault("PARTNER_SMPP_GATEWAY_TEST_REDIS_URI", "redis://localhost:6379");
        try {
            rawClient = RedisClient.create(uri);
            rawClient.connect().close();
            redisAvailable = true;
        } catch (Exception e) {
            assumeTrue(false, "Redis недоступен на " + uri + " (" + e.getMessage() + ") — пропуск");
        }
        cleanUp("acme");
        cleanUp("beta");
        store = new PartnerConfigStore(uri);
    }

    @AfterEach
    void tearDown() {
        if (redisAvailable) {
            cleanUp("acme");
            cleanUp("beta");
        }
        if (store != null) {
            store.close();
        }
        if (rawClient != null) {
            rawClient.shutdown();
        }
    }

    private void cleanUp(String partnerId) {
        if (rawClient == null || !redisAvailable) {
            return;
        }
        try (StatefulRedisConnection<String, String> conn = rawClient.connect()) {
            RedisCommands<String, String> cmd = conn.sync();
            cmd.del("config:current:partner:" + partnerId);
            for (int v = 1; v <= 5; v++) {
                cmd.del("config:version:partner:" + partnerId + ":" + v);
            }
        }
    }

    private void writePartner(String partnerId, int version, String json) {
        try (StatefulRedisConnection<String, String> conn = rawClient.connect()) {
            RedisCommands<String, String> cmd = conn.sync();
            cmd.set("config:version:partner:" + partnerId + ":" + version, json);
            cmd.set("config:current:partner:" + partnerId, String.valueOf(version));
        }
    }

    private static String smppPartnerJson(String partnerId, String status, String applicationId, String credentialRef) {
        return """
            {
              "partner_id": "%s",
              "version": 1,
              "status": "%s",
              "applications": [
                { "application_id": "%s", "display_name": "x",
                  "auth": { "type": "SMPP_BIND", "credential_ref": "%s" },
                  "ip_allowlist": [], "rate_limit_tps": 10, "allowed_channels": ["SMS"] }
              ]
            }
            """.formatted(partnerId, status, applicationId, credentialRef);
    }

    @Test
    void bootstrapFindsAllPartnersViaScan() {
        writePartner("acme", 1, smppPartnerJson("acme", "active", "acme_smpp", "vault://partners/acme/smpp/pw"));
        writePartner("beta", 3, smppPartnerJson("beta", "active", "beta_smpp", "vault://partners/beta/smpp/pw"));

        store.bootstrap();

        Map<String, PartnerConfigLoader.SmppBindCredential> creds = store.currentCredentials();
        assertEquals("acme", creds.get("acme_smpp").partnerId());
        assertEquals("beta", creds.get("beta_smpp").partnerId());
    }

    @Test
    void bootstrapOnEmptyRedisYieldsEmptyMap() {
        store.bootstrap();
        assertTrue(store.currentCredentials().isEmpty());
    }

}
