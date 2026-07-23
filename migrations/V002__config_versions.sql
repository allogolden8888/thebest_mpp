-- V002__config_versions.sql
-- data_infrastructure_spec.md §1.1. entity_type — те же значения, что
-- ConfigEntityType в platform-contracts/common/enums.proto (нижний регистр,
-- snake_case) — держать в синхроне вручную, единой генерации схемы
-- из proto в эту версию LLD не входит.

CREATE TABLE config.config_versions (
    id          BIGSERIAL PRIMARY KEY,
    entity_type TEXT NOT NULL,
    entity_id   TEXT NOT NULL,
    version     INT NOT NULL,
    payload     JSONB NOT NULL,
    status      TEXT NOT NULL DEFAULT 'active',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_by  TEXT NOT NULL,
    CONSTRAINT config_versions_entity_type_check CHECK (entity_type IN (
        'pipeline', 'policy_ruleset', 'policy_template', 'billing_tariff',
        'routing_table', 'number_range', 'partner', 'operator', 'subscriber_consent'
    )),
    CONSTRAINT config_versions_status_check CHECK (status IN ('active', 'archived')),
    CONSTRAINT config_versions_version_positive CHECK (version > 0),
    CONSTRAINT config_versions_unique UNIQUE (entity_type, entity_id, version)
);

CREATE INDEX config_versions_lookup_idx ON config.config_versions (entity_type, entity_id, status);

COMMENT ON TABLE config.config_versions IS
    'Immutable-версии конфигурации. Hard delete не выполняется (HLD §16.1) — только status=archived.';
COMMENT ON COLUMN config.config_versions.entity_type IS
    'policy_template и subscriber_consent НЕ хранят payload здесь — у них собственные таблицы '
    '(policy.policy_template, policy.subscriber_consent). Значение entity_type для них используется '
    'только в config_outbox/config.changes ради единообразной публикации изменений, не для payload.';
