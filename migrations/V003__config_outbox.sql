-- V003__config_outbox.sql
-- data_infrastructure_spec.md §1.2.
--
-- НАХОДКА при переходе на DDL (не было в концептуальной спеке): config_version_id
-- не может быть NOT NULL FK — policy_template и subscriber_consent пишут outbox-записи,
-- не имея строки в config_versions (их payload живёт в собственных таблицах,
-- см. policy.policy_template, policy.subscriber_consent). FK сделан nullable,
-- outbox переиспользуется как общий механизм публикации в config.changes
-- независимо от того, откуда физически пришли данные.

CREATE TABLE config.config_outbox (
    id                 BIGSERIAL PRIMARY KEY,
    config_version_id  BIGINT NULL REFERENCES config.config_versions(id),
    entity_type        TEXT NOT NULL,
    entity_id          TEXT NOT NULL,
    payload            JSONB NOT NULL,
    published          BOOLEAN NOT NULL DEFAULT false,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at       TIMESTAMPTZ NULL,
    CONSTRAINT config_outbox_published_at_check
        CHECK (published = false OR published_at IS NOT NULL)
);

-- Partial index под poll_outbox (Config Event Publisher, service_internal_methods.md §3.3).
CREATE INDEX config_outbox_unpublished_idx ON config.config_outbox (created_at)
    WHERE published = false;

COMMENT ON COLUMN config.config_outbox.config_version_id IS
    'NULL для entity_type policy_template/subscriber_consent — источник этих строк не '
    'config_versions, а собственные таблицы. Заполнено для всех остальных entity_type.';
