-- V009__dlr_correlation.sql
-- data_infrastructure_spec.md §1.8. Почасовые партиции (объём выше, чем
-- у lifecycle history — capacity_model.md §5), retention = operator DLR
-- SLA + safety margin, по умолчанию 48ч (нужно уточнение по реальным
-- SLA операторов, см. V015).

CREATE TABLE dlr.dlr_correlation (
    operator_id         TEXT NOT NULL,
    smsc_message_id     TEXT NOT NULL,
    segment_id          INT NOT NULL,
    message_id          UUID NOT NULL,
    stage_execution_id  UUID NOT NULL,
    submitted_at        TIMESTAMPTZ NOT NULL,
    expires_at          TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (operator_id, smsc_message_id, segment_id, submitted_at)
) PARTITION BY RANGE (submitted_at);

-- На случай ручной чистки (drop партиции — основной путь, дешевле DELETE).
CREATE INDEX dlr_correlation_expires_idx ON dlr.dlr_correlation (expires_at);
