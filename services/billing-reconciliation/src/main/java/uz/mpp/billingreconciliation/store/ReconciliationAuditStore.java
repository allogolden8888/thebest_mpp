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
 * <p><b>Открытый вопрос — таблица не мигрирована в этой сессии.</b>
 * `migrations/` — общий артефакт (та же категория, что `platform-contracts/`),
 * последняя миграция на момент этого среза — `V017__backoffice_stub.sql`.
 * Добавление `V018__billing_reconciliation_audit.sql` в одностороннем
 * порядке рискует конфликтом нумерации с Главным агентом, если он тоже
 * добавляет миграцию параллельно — этот класс реализован (реальный jOOQ,
 * реальные типы), но **не протестирован против реальной БД**, в отличие от
 * `LedgerStore`/`BalanceRecomputer`/`BillingRedisClient` в этом же сервисе.
 * Предлагаемая схема (для координации с Главным агентом):
 *
 * <pre>
 * CREATE TABLE billing.reconciliation_audit (
 *     id             BIGSERIAL PRIMARY KEY,
 *     account_id     TEXT NOT NULL,
 *     drift_minor_units BIGINT NOT NULL,
 *     severity       TEXT NOT NULL,
 *     action         TEXT NOT NULL, -- "FREEZE" | "UNFREEZE" | "NO_ACTION"
 *     recomputed_balance_minor_units BIGINT NULL,
 *     account_epoch_before BIGINT NOT NULL,
 *     account_epoch_after  BIGINT NOT NULL,
 *     created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
 * );
 * </pre>
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