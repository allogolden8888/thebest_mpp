package uz.mpp.deliveryreconciliation.store;

import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.JSONB;
import org.jooq.Record;
import org.jooq.Table;
import org.jooq.impl.DSL;
import uz.mpp.deliveryreconciliation.core.Evidence;

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
public final class ReconciliationStore implements CaseStore {

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

    // reconciliation.early_evidence (migrations/V032) — посадочная площадка
    // для свидетельства, обогнавшего создание case'а. По колонке на вид
    // свидетельства, а не JSONB: каждый источник обновляет ТОЛЬКО свою
    // колонку, поэтому накопление атомарно на уровне строки и не требует
    // read-modify-write (а значит, не теряет обновления между репликами —
    // JVM-локи Main.caseLocks там не помогают).
    private static final Table<Record> EARLY_EVIDENCE = table("reconciliation.early_evidence");
    private static final Field<UUID> EARLY_MESSAGE_ID = field("message_id", UUID.class);
    private static final Field<Boolean> EARLY_SUBMIT_ACCEPTED = field("submit_accepted", Boolean.class);
    private static final Field<String> EARLY_DELIVERY_STATUS = field("delivery_status", String.class);
    private static final Field<String> EARLY_QUERY_SM = field("query_sm", String.class);
    private static final Field<Timestamp> EARLY_FIRST_SEEN_AT = field("first_seen_at", Timestamp.class);

    private static final int CLOSE_CHUNK = 1000;

    private final DSLContext dsl;

    public ReconciliationStore(DSLContext dsl) {
        this.dsl = dsl;
    }

    /**
     * evaluate_deadline sweep — open case'ы с истёкшим deadline_at, использует
     * reconciliation_cases_status_deadline_idx.
     *
     * <p><b>LIMIT обязателен.</b> До этого выборка была неограниченной: при
     * бэклоге весь набор просроченных case'ов (замерено на живом стенде — 76 032
     * open-case'а, истекающих в пределах одного десятиминутного окна) грузился
     * в heap одним fetch'ем, вместе с evidence JSONB каждого. На mem_limit=768m
     * это прямой риск OOM ровно в тот момент, когда сервис и так отстаёт.
     * Ограничение делает потребление памяти одного прогона предсказуемым;
     * догон бэклога обеспечивается тем, что вызывающая сторона
     * ({@code Main.sweepDeadlines}) выбирает чанк за чанком, пока они не
     * кончатся, а не тем, что один SELECT забирает всё.
     *
     * <p>ORDER BY deadline_at — самые просроченные первыми: при бэклоге
     * догоняем в порядке возраста, а не в произвольном порядке хранения.
     */
    @Override
    public java.util.List<ReconciliationCase> findExpiredOpenCases(java.time.Instant now, int limit) {
        return dsl.select(CASE_ID, MESSAGE_ID, STAGE_EXECUTION_ID, OPERATOR_ID, STATUS, OPENED_AT, RESOLVED_AT, DEADLINE_AT, EVIDENCE)
            .from(CASES)
            .where(STATUS.eq("open").and(DEADLINE_AT.le(Timestamp.from(now))))
            .orderBy(DEADLINE_AT.asc())
            .limit(limit)
            .fetch()
            .map(ReconciliationStore::toCase);
    }

    /** handle_reconciliation_execute — загрузка существующего case по message_id, если есть. */
    @Override
    public Optional<ReconciliationCase> loadByMessageId(UUID messageId) {
        return dsl.select(CASE_ID, MESSAGE_ID, STAGE_EXECUTION_ID, OPERATOR_ID, STATUS, OPENED_AT, RESOLVED_AT, DEADLINE_AT, EVIDENCE)
            .from(CASES)
            .where(MESSAGE_ID.eq(messageId))
            .fetchOptional()
            .map(ReconciliationStore::toCase);
    }

    /**
     * handle_reconciliation_execute — создание нового case.
     *
     * <p>{@code ON CONFLICT (message_id) DO NOTHING} (опирается на
     * {@code reconciliation_cases_message_uniq}, migrations/V032): раньше
     * создание было check-then-act (loadByMessageId -> create), и при
     * at-least-once передоставке {@code stage.delivery-reconciliation} или
     * при ребалансе между двумя репликами два процесса могли вставить два
     * case'а на один message_id. Последствие было хуже дубля: после этого
     * {@link #loadByMessageId} (fetchOptional) бросал бы TooManyRows на
     * КАЖДОЕ свидетельство этого сообщения, т.е. терялось бы всё
     * свидетельство, а не только раннее — ровно тот класс порчи данных,
     * который и разбирается этой правкой.
     *
     * <p>Возвращается победившая строка (своя или чужая) — вызывающая
     * сторона работает с одним и тем же case'ом в любом случае.
     */
    @Override
    public ReconciliationCase create(UUID messageId, UUID stageExecutionId, String operatorId, Instant deadlineAt) {
        UUID caseId = UUID.randomUUID();
        dsl.insertInto(CASES)
            .columns(CASE_ID, MESSAGE_ID, STAGE_EXECUTION_ID, OPERATOR_ID, STATUS, DEADLINE_AT, EVIDENCE)
            .values(caseId, messageId, stageExecutionId, operatorId, "open", Timestamp.from(deadlineAt), JSONB.jsonb("{}"))
            .onConflict(MESSAGE_ID)
            .doNothing()
            .execute();
        return loadByMessageId(messageId).orElseThrow();
    }

    /**
     * collect_evidence до создания case'а — INSERT ... ON CONFLICT DO UPDATE,
     * трогающий ровно те колонки, которые несёт это событие. Именно поэтому
     * приземление раннего свидетельства не нуждается ни в каком локе: два
     * события разных видов (submit_accepted и DLR) пишут разные колонки одной
     * строки, а два события одного вида несут одно и то же значение.
     *
     * <p>Пустой delta не пишет НИЧЕГО — иначе строка появлялась бы для каждого
     * сообщения платформы, ничего при этом не неся (см. раздел «ОБЪЁМ» в
     * migrations/V032).
     */
    @Override
    public void recordEarlyEvidence(UUID messageId, Evidence delta) {
        java.util.Map<Field<?>, Object> updates = new java.util.LinkedHashMap<>();
        if (delta.submitAcceptedObserved()) {
            updates.put(EARLY_SUBMIT_ACCEPTED, true);
        }
        if (delta.deliveryStatusObserved() != Evidence.DeliveryOutcome.NONE) {
            updates.put(EARLY_DELIVERY_STATUS, delta.deliveryStatusObserved().name());
        }
        if (delta.querySmObserved() != Evidence.QuerySmOutcome.NOT_CALLED) {
            updates.put(EARLY_QUERY_SM, delta.querySmObserved().name());
        }
        if (updates.isEmpty()) {
            return;
        }
        dsl.insertInto(EARLY_EVIDENCE)
            .columns(EARLY_MESSAGE_ID, EARLY_SUBMIT_ACCEPTED, EARLY_DELIVERY_STATUS, EARLY_QUERY_SM)
            .values(messageId,
                delta.submitAcceptedObserved(),
                delta.deliveryStatusObserved() == Evidence.DeliveryOutcome.NONE ? null : delta.deliveryStatusObserved().name(),
                delta.querySmObserved() == Evidence.QuerySmOutcome.NOT_CALLED ? null : delta.querySmObserved().name())
            .onConflict(EARLY_MESSAGE_ID)
            .doUpdate()
            .set(updates)
            .execute();
    }

    @Override
    public Optional<Evidence> loadEarlyEvidence(UUID messageId) {
        return dsl.select(EARLY_SUBMIT_ACCEPTED, EARLY_DELIVERY_STATUS, EARLY_QUERY_SM)
            .from(EARLY_EVIDENCE)
            .where(EARLY_MESSAGE_ID.eq(messageId))
            .fetchOptional()
            .map(r -> new Evidence(
                Boolean.TRUE.equals(r.get(EARLY_SUBMIT_ACCEPTED)),
                parseEnum(Evidence.DeliveryOutcome.class, r.get(EARLY_DELIVERY_STATUS), Evidence.DeliveryOutcome.NONE),
                parseEnum(Evidence.QuerySmOutcome.class, r.get(EARLY_QUERY_SM), Evidence.QuerySmOutcome.NOT_CALLED)));
    }

    @Override
    public void deleteEarlyEvidence(UUID messageId) {
        dsl.deleteFrom(EARLY_EVIDENCE).where(EARLY_MESSAGE_ID.eq(messageId)).execute();
    }

    @Override
    public int purgeEarlyEvidence(Instant olderThan) {
        return dsl.deleteFrom(EARLY_EVIDENCE)
            .where(EARLY_FIRST_SEEN_AT.lt(Timestamp.from(olderThan)))
            .execute();
    }

    /** Неизвестное/NULL значение — деградируем к дефолту, как {@code EvidenceCodec.decode}, не бросаем. */
    private static <E extends Enum<E>> E parseEnum(Class<E> type, String value, E fallback) {
        if (value == null) {
            return fallback;
        }
        try {
            return Enum.valueOf(type, value);
        } catch (IllegalArgumentException e) {
            return fallback;
        }
    }

    /** persist_case — обновление evidence и, опционально, финального статуса/resolved_at. */
    @Override
    public void persistEvidence(UUID caseId, String evidenceJson) {
        dsl.update(CASES)
            .set(EVIDENCE, JSONB.jsonb(evidenceJson))
            .where(CASE_ID.eq(caseId))
            .execute();
    }

    /** persist_case — закрытие case с финальным статусом ("resolved" | "unresolved"). */
    public void closeCase(UUID caseId, String finalStatus, Instant resolvedAt) {
        closeCases(java.util.List.of(caseId), finalStatus, resolvedAt);
    }

    /**
     * persist_case пачкой — закрытие сразу многих case'ов одним UPDATE
     * (... WHERE case_id IN (...)) вместо одного round-trip'а на case.
     *
     * <p>Sweep закрывает только те case'ы, публикация которых в
     * {@code stage.completed} уже подтверждена (CODE_REVIEW.md #12), поэтому
     * список приходит сюда уже отфильтрованным — здесь остаётся только не
     * платить за каждый из них отдельным сетевым хопом в PostgreSQL. На
     * тесной машине, где park+wakeup блокирующего хопа стоит ~25мс,
     * последовательные UPDATE'ы были соизмеримы по стоимости с самой
     * реконсиляцией.
     *
     * <p>Чанк {@value #CLOSE_CHUNK} — граница по числу bind-параметров
     * PostgreSQL (максимум 65535 на запрос), а не по производительности:
     * даёт запас на порядок при любом разумном
     * {@code RECONCILIATION_SWEEP_BATCH_SIZE}.
     */
    @Override
    public void closeCases(java.util.List<UUID> caseIds, String finalStatus, Instant resolvedAt) {
        if (caseIds.isEmpty()) {
            return;
        }
        Timestamp resolvedTs = Timestamp.from(resolvedAt);
        for (int from = 0; from < caseIds.size(); from += CLOSE_CHUNK) {
            java.util.List<UUID> chunk = caseIds.subList(from, Math.min(from + CLOSE_CHUNK, caseIds.size()));
            dsl.update(CASES)
                .set(STATUS, finalStatus)
                .set(RESOLVED_AT, resolvedTs)
                .where(CASE_ID.in(chunk))
                .execute();
        }
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
