package uz.mpp.billingreconciliation.store;

import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Table;

import java.time.Instant;
import java.sql.Timestamp;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;

/**
 * persist_audit (service_internal_methods.md §5.3): "вся последовательность
 * recovery" -&gt; запись в PostgreSQL.
 *
 * <p><b>Обновление (CODE_REVIEW.md Critical #6 fix):</b> таблица
 * {@code billing.reconciliation_audit} раньше не была мигрирована — этот
 * класс компилировался (реальный jOOQ, реальные типы), но не был
 * протестирован против реальной БД, и {@code Main.reconcileOne} не имел
 * рабочего аудита для recompute-then-unfreeze пути. Теперь мигрирована —
 * {@code migrations/V021__billing_reconciliation_audit.sql} (схема ровно та,
 * что была здесь задокументирована как предложение, не выдумана заново) —
 * см. `MainTest`, реальный PostgreSQL round-trip.
 */
public final class ReconciliationAuditStore {

    private static final Table<org.jooq.Record> AUDIT = table("billing.reconciliation_audit");
    private static final Field<String> ACCOUNT_ID = field("account_id", String.class);
    private static final Field<Long> DRIFT = field("drift_minor_units", Long.class);
    private static final Field<String> SEVERITY = field("severity", String.class);
    private static final Field<String> ACTION = field("action", String.class);
    private static final Field<Long> RECOMPUTED_BALANCE = field("recomputed_balance_minor_units", Long.class);
    private static final Field<Long> EPOCH_BEFORE = field("account_epoch_before", Long.class);
    private static final Field<Long> EPOCH_AFTER = field("account_epoch_after", Long.class);
    private static final Field<Timestamp> CREATED_AT = field("created_at", Timestamp.class);

    private final DSLContext dsl;

    public ReconciliationAuditStore(DSLContext dsl) {
        this.dsl = dsl;
    }

    public void persist(String accountId, long driftMinorUnits, String severity, String action,
                         Long recomputedBalanceMinorUnits, long epochBefore, long epochAfter, Instant now) {
        dsl.insertInto(AUDIT)
            .columns(ACCOUNT_ID, DRIFT, SEVERITY, ACTION, RECOMPUTED_BALANCE, EPOCH_BEFORE, EPOCH_AFTER, CREATED_AT)
            .values(accountId, driftMinorUnits, severity, action, recomputedBalanceMinorUnits, epochBefore, epochAfter, Timestamp.from(now))
            .execute();
    }
}