package uz.mpp.deliveryreconciliation.store;

import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.JSONB;
import org.jooq.Record;
import org.jooq.Table;
import org.jooq.impl.DSL;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;

/**
 * handle_reconciliation_execute (create/load) + persist_case
 * (service_internal_methods.md §1.9), jOOQ DSL против
 * reconciliation.reconciliation_cases (migrations/V010, без codegen —
 * таблица/поля объявлены явно через DSL.table/DSL.field, стандартный
 * jOOQ-паттерн для схем без сгенerированных классов).
 */
public final class ReconciliationStore {

    private static final Table<Record> CASES = table("reconciliation.reconciliation_cases");
    private static final Field<UUID> CASE_ID = field("case_id", UUID.class);
    private static final Field<UUID> MESSAGE_ID = field("message_id", UUID.class);
    private static final Field<UUID> STAGE_EXECUTION_ID = field("stage_execution_id", UUID.class);
    private static final Field<String> OPERATOR_ID = field("operator_id", String.class);
    private static final Field<String> STATUS = field("status", String.class);
    private static final Field<Timestamp> OPENED_AT = field("opened_at", Timestamp.class);
    private static final Field<Timestamp> RESOLVED_AT = field("resolved_at", Timestamp.class);
    private static final Field<Timestamp> DEADLINE_AT = field("deadline_at", Timestamp.class);
    // CODE_REVIEW.md #11 сопутствующая находка: колонка объявлена
    // migrations/V010__reconciliation_cases.sql как JSONB, а не TEXT/VARCHAR —
    // связывание через Field<String> заставляло jOOQ отправлять параметр как
    // varchar, что реальный PostgreSQL отклонял ("column \"evidence\" is of
    // type jsonb but expression is of type character varying") на каждом
    // insert/update. До сих пор не было замечено, потому что ни одна запись
    // evidence не выполнялась в проде (сам предмет находки #11) — только в
    // ReconciliationStoreTest, где 4 из 4 relevant-тестов падали. Найдено при
    // сквозном прогоне тестов с реальным Postgres при работе над #11.
    private static final Field<JSONB> EVIDENCE = field("evidence", JSONB.class);

    private final DSLContext dsl;

    public ReconciliationStore(DSLContext dsl) {
        this.dsl = dsl;
    }

    /** evaluate_deadline sweep — open case'ы с истёкшим deadline_at, использует reconciliation_cases_status_deadline_idx. */
    public java.util.List<ReconciliationCase> findExpiredOpenCases(java.time.Instant now) {
        return dsl.select(CASE_ID, MESSAGE_ID, STAGE_EXECUTION_ID, OPERATOR_ID, STATUS, OPENED_AT, RESOLVED_AT, DEADLINE_AT, EVIDENCE)
            .from(CASES)
            .where(STATUS.eq("open").and(DEADLINE_AT.le(Timestamp.from(now))))
            .fetch()
            .map(ReconciliationStore::toCase);
    }

    /** handle_reconciliation_execute — загрузка существующего case по message_id, если есть. */
    public Optional<ReconciliationCase> loadByMessageId(UUID messageId) {
        return dsl.select(CASE_ID, MESSAGE_ID, STAGE_EXECUTION_ID, OPERATOR_ID, STATUS, OPENED_AT, RESOLVED_AT, DEADLINE_AT, EVIDENCE)
            .from(CASES)
            .where(MESSAGE_ID.eq(messageId))
            .fetchOptional()
            .map(ReconciliationStore::toCase);
    }

    /** handle_reconciliation_execute — создание нового case. */
    public ReconciliationCase create(UUID messageId, UUID stageExecutionId, String operatorId, Instant deadlineAt) {
        UUID caseId = UUID.randomUUID();
        dsl.insertInto(CASES)
            .columns(CASE_ID, MESSAGE_ID, STAGE_EXECUTION_ID, OPERATOR_ID, STATUS, DEADLINE_AT, EVIDENCE)
            .values(caseId, messageId, stageExecutionId, operatorId, "open", Timestamp.from(deadlineAt), JSONB.jsonb("{}"))
            .execute();
        return loadByMessageId(messageId).orElseThrow();
    }

    /** persist_case — обновление evidence и, опционально, финального статуса/resolved_at. */
    public void persistEvidence(UUID caseId, String evidenceJson) {
        dsl.update(CASES)
            .set(EVIDENCE, JSONB.jsonb(evidenceJson))
            .where(CASE_ID.eq(caseId))
            .execute();
    }

    /** persist_case — закрытие case с финальным статусом ("resolved" | "unresolved"). */
    public void closeCase(UUID caseId, String finalStatus, Instant resolvedAt) {
        dsl.update(CASES)
            .set(STATUS, finalStatus)
            .set(RESOLVED_AT, Timestamp.from(resolvedAt))
            .where(CASE_ID.eq(caseId))
            .execute();
    }

    private static ReconciliationCase toCase(Record r) {
        return new ReconciliationCase(
            r.get(CASE_ID), r.get(MESSAGE_ID), r.get(STAGE_EXECUTION_ID), r.get(OPERATOR_ID), r.get(STATUS),
            r.get(OPENED_AT) == null ? null : r.get(OPENED_AT).toInstant(),
            r.get(RESOLVED_AT) == null ? null : r.get(RESOLVED_AT).toInstant(),
            r.get(DEADLINE_AT) == null ? null : r.get(DEADLINE_AT).toInstant(),
            r.get(EVIDENCE) == null ? null : r.get(EVIDENCE).data()
        );
    }
}
