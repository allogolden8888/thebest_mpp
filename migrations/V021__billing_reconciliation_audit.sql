-- V021__billing_reconciliation_audit.sql
-- billing-reconciliation/store/ReconciliationAuditStore.java — persist_audit
-- (service_internal_methods.md §5.3). Схема ровно та, что была
-- задокументирована как предложение в докстринге этого класса (не
-- реализована раньше — CODE_REVIEW.md Critical #6, "freeze без
-- автоматического unfreeze": recompute-then-unfreeze путь не был подключён
-- в orchestration именно потому, что этой таблицы не существовало для
-- аудита каждой попытки).
--
-- Изначально добавлена как V018 в отдельном (от subagent-1) worktree —
-- переномерована в V021 при слиянии, т.к. subagent-1 параллельно занял
-- V018/V019/V020 для config_outbox/dlq_record/message_read_model миграций.

CREATE TABLE billing.reconciliation_audit (
    id                              BIGSERIAL PRIMARY KEY,
    account_id                      TEXT NOT NULL,
    drift_minor_units               BIGINT NOT NULL,
    severity                        TEXT NOT NULL CHECK (severity IN ('NONE', 'LOW', 'MEDIUM', 'HIGH')),
    action                          TEXT NOT NULL CHECK (action IN ('NO_ACTION', 'FREEZE', 'UNFREEZE', 'UNFREEZE_CONFLICT', 'FREEZE_PARTIAL')),
    recomputed_balance_minor_units  BIGINT NULL,
    account_epoch_before            BIGINT NOT NULL,
    account_epoch_after             BIGINT NOT NULL,
    created_at                      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX reconciliation_audit_account_idx ON billing.reconciliation_audit (account_id, created_at);
