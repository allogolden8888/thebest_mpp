-- V004__message_read_model.sql
-- data_infrastructure_spec.md §1.3. UPSERT-heavy (обновляется на каждый
-- message.lifecycle) — fillfactor снижен под HOT-обновления
-- (capacity_model.md §6.2).

CREATE TABLE messaging.message_read_model (
    message_id       UUID PRIMARY KEY,
    partner_id       TEXT NOT NULL,
    application_id   TEXT NOT NULL,
    trace_id         UUID NOT NULL,
    pipeline_id      TEXT NOT NULL,
    pipeline_version TEXT NOT NULL,
    current_status   TEXT NOT NULL,
    terminal         BOOLEAN NOT NULL DEFAULT false,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
) WITH (fillfactor = 70);

CREATE INDEX message_read_model_partner_idx ON messaging.message_read_model (partner_id, created_at);
CREATE INDEX message_read_model_trace_idx ON messaging.message_read_model (trace_id);
