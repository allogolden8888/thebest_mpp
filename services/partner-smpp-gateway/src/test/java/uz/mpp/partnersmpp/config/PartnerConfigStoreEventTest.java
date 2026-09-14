package uz.mpp.partnersmpp.config;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Pure live-event tests: RedisClient is created but no network connection is opened. */
class PartnerConfigStoreEventTest {

    private PartnerConfigStore store;

    @BeforeEach
    void setUp() {
        store = new PartnerConfigStore("redis://localhost:6379");
    }

    @AfterEach
    void tearDown() {
        store.close();
    }

    @Test
    void activePayloadIsAppliedDirectlyWithoutWaitingForRedisProjection() {
        assertTrue(store.applyEvent("acme", 2, "active", payload("acme", "active", "smpp-v2")));

        var credential = store.currentCredentials().get("smpp-v2");
        assertEquals("acme", credential.partnerId());
        assertEquals("vault://partners/acme/smpp/password", credential.credentialRef());
    }

    @Test
    void staleReplayCannotRollBackNewerPartnerVersion() {
        assertTrue(store.applyEvent("acme", 5, "active", payload("acme", "active", "smpp-v5")));
        assertFalse(store.applyEvent("acme", 4, "active", payload("acme", "active", "smpp-v4")));

        assertNull(store.currentCredentials().get("smpp-v4"));
        assertEquals("smpp-v5", store.currentCredentials().get("smpp-v5").applicationId());
    }

    @Test
    void archiveIsVersionedAndOlderActiveEventCannotRevivePartner() {
        store.applyEvent("acme", 1, "active", payload("acme", "active", "smpp-v1"));
        assertTrue(store.applyEvent("acme", 2, "archived", new byte[0]));
        assertTrue(store.currentCredentials().isEmpty());

        assertFalse(store.applyEvent("acme", 1, "active", payload("acme", "active", "smpp-v1")));
        assertTrue(store.currentCredentials().isEmpty());
    }

    @Test
    void kafkaKeyIdentityCannotReplaceAnotherPartner() {
        store.applyEvent("acme", 1, "active", payload("acme", "active", "acme-smpp"));

        assertThrows(
            IllegalArgumentException.class,
            () -> store.applyEvent("victim", 2, "active", payload("acme", "active", "evil"))
        );
        assertEquals("acme", store.currentCredentials().get("acme-smpp").partnerId());
        assertNull(store.currentCredentials().get("evil"));
    }

    @Test
    void duplicateSystemIdAcrossPartnersIsRejectedWithoutChangingSnapshot() {
        store.applyEvent("acme", 1, "active", payload("acme", "active", "shared-smpp"));

        assertThrows(
            IllegalArgumentException.class,
            () -> store.applyEvent("beta", 1, "active", payload("beta", "active", "shared-smpp"))
        );
        assertEquals("acme", store.currentCredentials().get("shared-smpp").partnerId());
    }

    private static byte[] payload(String partnerId, String status, String applicationId) {
        return ("""
            {
              "partner_id": "%s",
              "version": 1,
              "status": "%s",
              "applications": [
                {
                  "application_id": "%s",
                  "display_name": "SMPP",
                  "auth": {
                    "type": "SMPP_BIND",
                    "credential_ref": "vault://partners/%s/smpp/password"
                  },
                  "ip_allowlist": [],
                  "rate_limit_tps": 10,
                  "allowed_channels": ["SMS"]
                }
              ]
            }
            """).formatted(partnerId, status, applicationId, partnerId).getBytes(StandardCharsets.UTF_8);
    }
}
