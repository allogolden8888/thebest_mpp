package uz.mpp.billingledgerwriter.store;

import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Table;

import java.math.BigDecimal;
import java.util.UUID;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;

/**
 * insert_double_entry + apply_compensating_entry (service_internal_methods.md
 * §5.2) — jOOQ DSL (без codegen) против billing.billing_ledger
 * (migrations/V008__billing_ledger.sql). Оба метода — один и тот же INSERT:
 * "compensating" отличается только entry_type + обязательным
 * source_charge_id, CHECK-ограничения в БД (billing_ledger_compensating_source_required/
 * billing_ledger_charge_source_forbidden) — последняя линия защиты, этот
 * код не полагается только на них (см. LedgerEntry.validate()).
 */
public final class LedgerStore {

    private static final Table<org.jooq.Record> LEDGER = table("billing.billing_ledger");
    private static final Field<UUID> CHARGE_ID = field("charge_id", UUID.class);
    private static final Field<String> ACCOUNT_ID = field("account_id", String.class);
    private static final Field<String> PARTNER_ID = field("partner_id", String.class);
    private static final Field<BigDecimal> AMOUNT = field("amount", BigDecimal.class);
    private static final Field<String> CURRENCY = field("currency", String.class);
    private static final Field<String> ENTRY_TYPE = field("entry_type", String.class);
    private static final Field<UUID> SOURCE_CHARGE_ID = field("source_charge_id", UUID.class);

    private final DSLContext dsl;

    public LedgerStore(DSLContext dsl) {
        this.dsl = dsl;
    }

    /**
     * insert_double_entry — INSERT ... ON CONFLICT (charge_id) DO NOTHING.
     * Idempotency-ключ = charge_id (LedgerEntry уже провалидирован
     * конструктором, entry_type/source_charge_id согласованы).
     *
     * @return true, если строка реально вставлена; false — уже существовала
     *         (idempotent replay, не ошибка).
     */
    public boolean insert(LedgerEntry entry) {
        int inserted = dsl.insertInto(LEDGER)
            .columns(CHARGE_ID, ACCOUNT_ID, PARTNER_ID, AMOUNT, CURRENCY, ENTRY_TYPE, SOURCE_CHARGE_ID)
            .values(entry.chargeId(), entry.accountId(), entry.partnerId(), entry.amount(), entry.currency(),
                entry.entryType(), entry.sourceChargeId())
            .onConflict(CHARGE_ID).doNothing()
            .execute();
        return inserted > 0;
    }

    public boolean exists(UUID chargeId) {
        return dsl.fetchExists(dsl.selectOne().from(LEDGER).where(CHARGE_ID.eq(chargeId)));
    }
}
