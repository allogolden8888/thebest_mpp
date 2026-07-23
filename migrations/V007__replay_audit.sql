-- V007__replay_audit.sql
-- data_infrastructure_spec.md §1.6.

CREATE TABLE messaging.replay_audit (
    id                  BIGSERIAL PRIMARY KEY,
    stage_execution_id  UUID NOT NULL,
    requested_by        TEXT NOT NULL,
    requested_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Ключи: ttl, idempotency, billing_side_effect, delivery_ambiguity -> bool (HLD §20).
    checks_passed       JSONB NOT NULL,
    outcome             TEXT NOT NULL,
    target_topic        TEXT NOT NULL
);

CREATE INDEX replay_audit_stage_execution_idx ON messaging.replay_audit (stage_execution_id);
