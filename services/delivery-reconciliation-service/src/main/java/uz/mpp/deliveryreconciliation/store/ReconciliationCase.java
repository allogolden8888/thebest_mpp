package uz.mpp.deliveryreconciliation.store;

import java.time.Instant;
import java.util.UUID;

/** ReconciliationCase — migrations/V010__reconciliation_cases.sql. */
public record ReconciliationCase(
    UUID caseId,
    UUID messageId,
    UUID stageExecutionId,
    String operatorId,
    String status, // "open" | "resolved" | "unresolved"
    Instant openedAt,
    Instant resolvedAt,
    Instant deadlineAt,
    String evidenceJson
) {

    /**
     * Копия case'а с подменённым evidence — нужна ровно одному вызывающему:
     * немедленному закрытию case'а по терминальному свидетельству
     * ({@code Main.mergeEvidence}). Там evidence уже посчитан и записан в БД,
     * но объект {@code fresh}, прочитанный ДО записи, несёт старый JSON, а
     * {@code sweepBatch}/{@code OutcomeResolver} принимают решение именно по
     * полю рекорда. Без этой копии case закрывался бы по устаревшему
     * evidence — то есть ровно тем ложным CONFIRMED_NOT_SUBMITTED, ради
     * устранения которого правка и делается (замерено 16 187 таких финалов,
     * у всех DELIVERY=SUCCEEDED).
     *
     * <p>Перечитывать case из БД здесь было бы лишним round trip'ом на
     * горячем пути: значение уже известно, оно только что записано под тем же
     * per-caseId локом.
     */
    public ReconciliationCase withEvidenceJson(String newEvidenceJson) {
        return new ReconciliationCase(
            caseId, messageId, stageExecutionId, operatorId, status,
            openedAt, resolvedAt, deadlineAt, newEvidenceJson);
    }
}
