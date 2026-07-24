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
}
