package uz.mpp.billingledgerwriter.store;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

class LedgerEntryTest {

    @Test
    void chargeWithoutSourceChargeIdIsValid() {
        assertDoesNotThrow(() -> new LedgerEntry(UUID.randomUUID(), "acc-1", "acme", BigDecimal.ONE, "UZS", "charge", null));
    }

    @Test
    void compensatingWithSourceChargeIdIsValid() {
        assertDoesNotThrow(() -> new LedgerEntry(UUID.randomUUID(), "acc-1", "acme", BigDecimal.ONE, "UZS", "compensating", UUID.randomUUID()));
    }

    @Test
    void chargeWithSourceChargeIdIsRejected() {
        assertThrows(IllegalArgumentException.class, () ->
            new LedgerEntry(UUID.randomUUID(), "acc-1", "acme", BigDecimal.ONE, "UZS", "charge", UUID.randomUUID()));
    }

    @Test
    void compensatingWithoutSourceChargeIdIsRejected() {
        assertThrows(IllegalArgumentException.class, () ->
            new LedgerEntry(UUID.randomUUID(), "acc-1", "acme", BigDecimal.ONE, "UZS", "compensating", null));
    }

    @Test
    void unknownEntryTypeIsRejected() {
        assertThrows(IllegalArgumentException.class, () ->
            new LedgerEntry(UUID.randomUUID(), "acc-1", "acme", BigDecimal.ONE, "UZS", "bogus", null));
    }
}