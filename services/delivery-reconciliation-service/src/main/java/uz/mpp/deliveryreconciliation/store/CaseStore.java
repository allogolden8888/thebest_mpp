package uz.mpp.deliveryreconciliation.store;

import uz.mpp.deliveryreconciliation.core.Evidence;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Порт персистентности реконсиляции: ровно те операции, которыми пользуется
 * {@code Main} (handle_reconciliation_execute / collect_evidence /
 * evaluate_deadline / persist_case, service_internal_methods.md §1.9).
 *
 * <p>Интерфейс введён не «для слоёв», а чтобы гонки, ради которых написана
 * эта правка, стали проверяемы обычным unit-тестом: обе они — про ПОРЯДОК
 * обращений к хранилищу (свидетельство пришло раньше case'а; терминальный
 * DLR закрывает case до дедлайна), и воспроизводить порядок надёжнее на
 * контролируемом фейке, чем на живом PostgreSQL, где нужное чередование
 * потоков не воспроизводится по заказу. Реальная реализация SQL —
 * {@link ReconciliationStore}, она отдельно покрыта
 * {@code ReconciliationStoreTest} против настоящего PostgreSQL.
 */
public interface CaseStore {

    /** evaluate_deadline sweep — open case'ы с истёкшим deadline_at (не более {@code limit}). */
    List<ReconciliationCase> findExpiredOpenCases(Instant now, int limit);

    /** handle_reconciliation_execute — загрузка существующего case по message_id, если есть. */
    Optional<ReconciliationCase> loadByMessageId(UUID messageId);

    /** handle_reconciliation_execute — создание case'а; идемпотентно по message_id. */
    ReconciliationCase create(UUID messageId, UUID stageExecutionId, String operatorId, Instant deadlineAt);

    /** persist_case — обновление evidence случая. */
    void persistEvidence(UUID caseId, String evidenceJson);

    /** persist_case — закрытие пачки case'ов с финальным статусом. */
    void closeCases(List<UUID> caseIds, String finalStatus, Instant resolvedAt);

    /**
     * collect_evidence для сообщения, у которого case'а ЕЩЁ НЕТ — приземление
     * свидетельства в {@code reconciliation.early_evidence} (migrations/V032).
     * Каждый вид свидетельства обновляет только свою колонку, поэтому вызов
     * идемпотентен и не затирает свидетельство другого вида даже при
     * параллельной записи из другой реплики.
     *
     * @param delta свидетельство ЭТОГО события ({@code Evidence.empty()} с
     *              одним заполненным полем); пустой delta — no-op
     */
    void recordEarlyEvidence(UUID messageId, Evidence delta);

    /** Ранее приземлённое свидетельство сообщения, если оно есть. */
    Optional<Evidence> loadEarlyEvidence(UUID messageId);

    /** Удаление приземлённого свидетельства после того, как оно влито в case. */
    void deleteEarlyEvidence(UUID messageId);

    /**
     * Удаление приземлённого свидетельства старше {@code olderThan} — для
     * сообщений, у которых case так и не появился (после исправления графа
     * пайплайна в реконсиляцию идёт только SUBMISSION_OUTCOME_UNKNOWN, т.е.
     * это подавляющее большинство). Без этого таблица растёт без границы.
     *
     * @return сколько строк удалено
     */
    int purgeEarlyEvidence(Instant olderThan);
}
