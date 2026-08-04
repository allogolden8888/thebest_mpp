package uz.mpp.billingledgerwriter.kafkaio;

import uz.mpp.billingledgerwriter.store.LedgerEntry;
import uz.mpp.platformcontracts.common.v1.LedgerEntryType;
import uz.mpp.platformcontracts.events.v1.LedgerEvent;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.UUID;

/** on_ledger_event — чистая функция, LedgerEvent (proto) -&gt; LedgerEntry (store model). */
public final class LedgerEntryMapper {

    private LedgerEntryMapper() {
    }

    // minor_units -> decimal с 4 знаками (billing_ledger.amount NUMERIC(18,4)) —
    // предполагает 2 знака после запятой у валюты (UZS = tiyin, 100 tiyin = 1 UZS);
    // для валют с другим количеством знаков потребовалась бы отдельная таблица
    // экспонент по currency_code — не реализовано, см. README.
    private static final BigInteger MINOR_UNITS_PER_MAJOR = BigInteger.valueOf(100);

    public static LedgerEntry fromProto(LedgerEvent event) {
        String entryType = switch (event.getEntryType()) {
            case LEDGER_ENTRY_TYPE_CHARGE -> "charge";
            case LEDGER_ENTRY_TYPE_COMPENSATING -> "compensating";
            default -> throw new IllegalArgumentException("неизвестный entry_type: " + event.getEntryType());
        };

        UUID sourceChargeId = event.getEntryType() == LedgerEntryType.LEDGER_ENTRY_TYPE_COMPENSATING
            ? UUID.fromString(event.getSourceChargeId())
            : null;

        BigDecimal amount = new BigDecimal(event.getAmount().getMinorUnits())
            .divide(new BigDecimal(MINOR_UNITS_PER_MAJOR), 4, java.math.RoundingMode.UNNECESSARY);

        return new LedgerEntry(
            UUID.fromString(event.getChargeId()),
            event.getAccountId(),
            event.getPartnerId(),
            amount,
            event.getAmount().getCurrencyCode(),
            entryType,
            sourceChargeId
        );
    }
}