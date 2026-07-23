-- V016__execution_control_audit.sql
-- data_infrastructure_spec.md §1.10. scope/state — те же значения, что
-- ExecutionControlScope/ExecutionControlState в
-- platform-contracts/common/enums.proto; admission_rate 0..1 — та же
-- граница, что ExecutionControlRecord.admission_rate (double) в proto.

CREATE TABLE control.execution_control_audit (
    id              BIGSERIAL PRIMARY KEY,
    scope           TEXT NOT NULL CHECK (scope IN (
        'GLOBAL', 'STAGE', 'PARTNER', 'PARTNER_STAGE', 'OPERATOR_ROUTE'
    )),
    scope_id        TEXT NOT NULL DEFAULT '',
    state           TEXT NOT NULL CHECK (state IN ('ACTIVE', 'DEGRADED', 'PAUSED')),
    admission_rate  NUMERIC(5, 4) NOT NULL CHECK (admission_rate >= 0 AND admission_rate <= 1),
    reason          TEXT NOT NULL,
    requested_by    TEXT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at      TIMESTAMPTZ NULL
);

CREATE INDEX execution_control_audit_scope_idx ON control.execution_control_audit (scope, scope_id, created_at);
