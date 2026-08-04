package uz.mpp.billingledgerwriter.store;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * on_ledger_event (service_internal_methods.md §5.2) выход — валидирует те
 * же два инварианта, что CHECK-ограничения migrations/V008
 * (billing_ledger_compensating_source_required/_charge_source_forbidden),
 * в коде, а не только полагаясь на отказ БД при вставке.
 */
public record LedgerEntry(
    UUID chargeId,
    String accountId,
    String partnerId,
    BigDecimal amount,
    String currency,
    String entryType, // "charge" | "compensating"
    UUID sourceChargeId // null для "charge"
) {
    public LedgerEntry {
        if (!entryType.equals("charge") && !entryType.equals("compensating")) {
            throw new IllegalArgumentException("неизвестный entry_type: " + entryType);
        }
        if (entryType.equals("compensating") && sourceChargeId == null) {
            throw new IllegalArgumentException("compensating-запись обязана иметь source_charge_id");
        }
        if (entryType.equals("charge") && sourceChargeId != null) {
            throw new IllegalArgumentException("charge-запись не должна иметь source_charge_id");
        }
    }
}
