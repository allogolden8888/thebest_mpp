-- V010__reconciliation_cases.sql
-- data_infrastructure_spec.md §1.9.

CREATE TABLE reconciliation.reconciliation_cases (
    case_id             UUID PRIMARY KEY,
    message_id          UUID NOT NULL,
    stage_execution_id  UUID NOT NULL,
    operator_id         TEXT NOT NULL,
    status              TEXT NOT NULL CHECK (status IN ('open', 'resolved', 'unresolved')),
    opened_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at         TIMESTAMPTZ NULL,
    deadline_at         TIMESTAMPTZ NOT NULL,
    evidence            JSONB NOT NULL DEFAULT '{}'::jsonb,
    CONSTRAINT reconciliation_cases_resolved_at_check
        CHECK (status = 'open' OR resolved_at IS NOT NULL)
);

CREATE INDEX reconciliation_cases_message_idx ON reconciliation.reconciliation_cases (message_id);
CREATE INDEX reconciliation_cases_status_deadline_idx ON reconciliation.reconciliation_cases (status, deadline_at);
