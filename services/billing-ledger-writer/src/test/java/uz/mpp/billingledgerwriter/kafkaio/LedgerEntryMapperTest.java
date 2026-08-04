package uz.mpp.billingledgerwriter.kafkaio;

import org.junit.jupiter.api.Test;
import uz.mpp.billingledgerwriter.store.LedgerEntry;
import uz.mpp.platformcontracts.common.v1.LedgerEntryType;
import uz.mpp.platformcontracts.common.v1.Money;
import uz.mpp.platformcontracts.events.v1.LedgerEvent;

import java.math.BigDecimal;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class LedgerEntryMapperTest {

    @Test
    void mapsChargeEventConvertingMinorUnitsToDecimal() {
        UUID chargeId = UUID.randomUUID();
        LedgerEvent event = LedgerEvent.newBuilder()
            .setChargeId(chargeId.toString())
            .setAccountId("acc-1")
            .setPartnerId("acme")
            .setAmount(Money.newBuilder().setCurrencyCode("UZS").setMinorUnits(150000).build())
            .setEntryType(LedgerEntryType.LEDGER_ENTRY_TYPE_CHARGE)
            .build();

        LedgerEntry entry = LedgerEntryMapper.fromProto(event);

        assertEquals(chargeId, entry.chargeId());
        assertEquals(0, new BigDecimal("1500.0000").compareTo(entry.amount()));
        assertEquals("charge", entry.entryType());
        assertNull(entry.sourceChargeId());
    }

    @Test
    void mapsCompensatingEventWithSourceChargeId() {
        UUID chargeId = UUID.randomUUID();
        UUID sourceChargeId = UUID.randomUUID();
        LedgerEvent event = LedgerEvent.newBuilder()
            .setChargeId(chargeId.toString())
            .setAccountId("acc-1")
            .setPartnerId("acme")
            .setAmount(Money.newBuilder().setCurrencyCode("UZS").setMinorUnits(5000).build())
            .setEntryType(LedgerEntryType.LEDGER_ENTRY_TYPE_COMPENSATING)
            .setSourceChargeId(sourceChargeId.toString())
            .build();

        LedgerEntry entry = LedgerEntryMapper.fromProto(event);

        assertEquals("compensating", entry.entryType());
        assertEquals(sourceChargeId, entry.sourceChargeId());
    }
}