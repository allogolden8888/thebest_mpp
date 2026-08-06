package uz.mpp.billing;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PartnerSendersResolverTest {

    private static PartnerSendersResolver.PartnerSenders realExample() {
        Path path = Path.of(System.getProperty("user.dir"), "..", "..", "config_schemas", "examples", "partner.valid.json");
        return PartnerSendersResolver.fromFile(path);
    }

    @Test
    void readsRealPartnerIdAndSenders() {
        // config_schemas/examples/partner.valid.json: click_uz, 2 senders.
        PartnerSendersResolver.PartnerSenders result = realExample();
        assertEquals("click_uz", result.partnerId());
        assertEquals(2, result.senders().size());
        assertTrue(result.senders().stream().anyMatch(s -> s.senderId().equals("CLICK") && s.type().equals("ALPHANAME")));
        assertTrue(result.senders().stream().anyMatch(s -> s.senderId().equals("5252") && s.type().equals("SHORT_NUMBER")));
    }

    @Test
    void missingSendersArrayYieldsEmptyListNotError() {
        String json = """
            {"partner_id": "no_senders_partner", "version": 1, "status": "active", "applications": []}
            """;
        PartnerSendersResolver.PartnerSenders result = PartnerSendersResolver.fromJson(json);
        assertEquals("no_senders_partner", result.partnerId());
        assertEquals(0, result.senders().size());
    }

    @Test
    void missingPartnerIdIsRejected() {
        String json = """
            {"version": 1, "status": "active", "applications": []}
            """;
        assertThrows(IllegalArgumentException.class, () -> PartnerSendersResolver.fromJson(json));
    }
}
