package uz.mpp.billingoutbox.kafkaio;

import org.junit.jupiter.api.Test;
import uz.mpp.billingoutbox.core.StreamEntry;
import uz.mpp.platformcontracts.common.v1.LedgerEntryType;
import uz.mpp.platformcontracts.events.v1.LedgerEvent;

import static org.junit.jupiter.api.Assertions.*;

class LedgerEventBuilderTest {

    @Test
    void buildsChargeEntry() {
        StreamEntry entry = new StreamEntry(0, "1-0", "charge-1", "acc-1", "acme", 150_000, "UZS", "charge", null, "", System.currentTimeMillis());
        LedgerEvent event = LedgerEventBuilder.build(entry);

        assertEquals(LedgerEntryType.LEDGER_ENTRY_TYPE_CHARGE, event.getEntryType());
        assertEquals("charge-1", event.getChargeId());
        assertEquals(150_000, event.getAmount().getMinorUnits());
        assertEquals("UZS", event.getAmount().getCurrencyCode());
        assertEquals("", event.getSourceChargeId());
    }

    @Test
    void buildsCompensatingEntryWithSourceChargeId() {
        StreamEntry entry = new StreamEntry(0, "2-0", "comp-1", "acc-1", "acme", 50_000, "UZS", "compensating", "charge-1", "refund", System.currentTimeMillis());
        LedgerEvent event = LedgerEventBuilder.build(entry);

        assertEquals(LedgerEntryType.LEDGER_ENTRY_TYPE_COMPENSATING, event.getEntryType());
        assertEquals("charge-1", event.getSourceChargeId());
        assertEquals("refund", event.getReason());
    }

    @Test
    void rejectsCompensatingWithoutSourceChargeId() {
        StreamEntry entry = new StreamEntry(0, "3-0", "comp-2", "acc-1", "acme", 50_000, "UZS", "compensating", null, "refund", System.currentTimeMillis());
        assertThrows(IllegalArgumentException.class, () -> LedgerEventBuilder.build(entry));
    }

    @Test
    void rejectsUnknownEntryType() {
        StreamEntry entry = new StreamEntry(0, "4-0", "x", "acc-1", "acme", 1, "UZS", "bogus", null, "", System.currentTimeMillis());
        assertThrows(IllegalArgumentException.class, () -> LedgerEventBuilder.build(entry));
    }
}