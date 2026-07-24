package uz.mpp.billingreconciliation.store;

import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Table;
import org.jooq.impl.DSL;

import java.math.BigDecimal;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;

/**
 * recompute_balance (service_internal_methods.md §5.3): ledger-записи из
 * PostgreSQL -&gt; RecomputedBalance.
 *
 * <p><b>Открытый вопрос</b> (см. README): `billing.billing_ledger`
 * (migrations/V008) моделирует только `charge`/`compensating` — нет
 * entry_type для начального пополнения счёта/top-up. Поэтому здесь
 * вычисляется **дельта** (`SUM(compensating) - SUM(charge)`), не абсолютный
 * баланс с нуля — сверка с Redis идёт по дельте за окно, не по полному
 * пересчёту с начала времён аккаунта.
 */
public final class BalanceRecomputer {

    private static final Table<org.jooq.Record> LEDGER = table("billing.billing_ledger");
    private static final Field<String> ACCOUNT_ID = field("account_id", String.class);
    private static final Field<BigDecimal> AMOUNT = field("amount", BigDecimal.class);
    private static final Field<String> ENTRY_TYPE = field("entry_type", String.class);

    private final DSLContext dsl;

    public BalanceRecomputer(DSLContext dsl) {
        this.dsl = dsl;
    }

    /** @return дельта в минорных единицах (умножено на 100, см. billing-ledger-writer inverse conversion). */
    public long recomputeDeltaMinorUnits(String accountId) {
        BigDecimal chargeSum = dsl.select(DSL.coalesce(DSL.sum(AMOUNT), BigDecimal.ZERO))
            .from(LEDGER)
            .where(ACCOUNT_ID.eq(accountId).and(ENTRY_TYPE.eq("charge")))
            .fetchOne(0, BigDecimal.class);
        BigDecimal compensatingSum = dsl.select(DSL.coalesce(DSL.sum(AMOUNT), BigDecimal.ZERO))
            .from(LEDGER)
            .where(ACCOUNT_ID.eq(accountId).and(ENTRY_TYPE.eq("compensating")))
            .fetchOne(0, BigDecimal.class);

        BigDecimal deltaMajorUnits = compensatingSum.subtract(chargeSum);
        return deltaMajorUnits.multiply(BigDecimal.valueOf(100)).longValueExact();
    }
}
