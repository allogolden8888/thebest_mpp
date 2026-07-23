-- V006__dlq_record.sql
-- data_infrastructure_spec.md §1.5. original_command — protobuf
-- mpp.common.v1.StageExecuteCommand целиком (platform_contracts.md §3),
-- не только метаданные — нужен Replay Service для republish без
-- дополнительных запросов (HLD §20).

CREATE TABLE messaging.dlq_record (
    stage_execution_id  UUID PRIMARY KEY,
    message_id          UUID NOT NULL,
    stage_name          TEXT NOT NULL,
    attempt             INT NOT NULL,
    original_command    BYTEA NOT NULL,
    reason_code         TEXT NOT NULL,
    error_detail        TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    replay_status       TEXT NOT NULL DEFAULT 'pending'
        CHECK (replay_status IN ('pending', 'replayed', 'expired'))
);

CREATE INDEX dlq_record_message_idx ON messaging.dlq_record (message_id);
CREATE INDEX dlq_record_stage_created_idx ON messaging.dlq_record (stage_name, created_at);
