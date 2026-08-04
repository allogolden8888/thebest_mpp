package uz.mpp.billingoutbox.kafkaio;

import com.google.protobuf.Timestamp;
import uz.mpp.billingoutbox.core.StreamEntry;
import uz.mpp.platformcontracts.common.v1.LedgerEntryType;
import uz.mpp.platformcontracts.common.v1.Money;
import uz.mpp.platformcontracts.events.v1.LedgerEvent;

import java.time.Instant;

/** publish_ledger_event (service_internal_methods.md §5.1) — чистая сборка, без сети. */
public final class LedgerEventBuilder {

    private LedgerEventBuilder() {
    }

    public static LedgerEvent build(StreamEntry entry) {
        LedgerEntryType entryType = switch (entry.entryType()) {
            case "charge" -> LedgerEntryType.LEDGER_ENTRY_TYPE_CHARGE;
            case "compensating" -> LedgerEntryType.LEDGER_ENTRY_TYPE_COMPENSATING;
            default -> throw new IllegalArgumentException("неизвестный entry_type: " + entry.entryType());
        };

        LedgerEvent.Builder builder = LedgerEvent.newBuilder()
            .setChargeId(entry.chargeId())
            .setAccountId(entry.accountId())
            .setPartnerId(entry.partnerId())
            .setAmount(Money.newBuilder().setCurrencyCode(entry.currencyCode()).setMinorUnits(entry.amountMinorUnits()).build())
            .setEntryType(entryType)
            .setReason(entry.reason() == null ? "" : entry.reason())
            .setCreatedAt(toTimestamp(Instant.ofEpochMilli(entry.createdAtEpochMs())));

        if (entryType == LedgerEntryType.LEDGER_ENTRY_TYPE_COMPENSATING) {
            if (entry.sourceChargeId() == null || entry.sourceChargeId().isEmpty()) {
                throw new IllegalArgumentException("compensating-запись без source_charge_id (billing_ledger_compensating_source_required)");
            }
            builder.setSourceChargeId(entry.sourceChargeId());
        }

        return builder.build();
    }

    private static Timestamp toTimestamp(Instant instant) {
        return Timestamp.newBuilder().setSeconds(instant.getEpochSecond()).setNanos(instant.getNano()).build();
    }
}