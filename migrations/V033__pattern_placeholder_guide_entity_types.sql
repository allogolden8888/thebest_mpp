-- V033__pattern_placeholder_guide_entity_types.sql
-- luminous-hugging-charm.md, BACKOFFICE_DESIGN_SPEC.md Экраны 36/41.
-- Добавляет 'pattern_placeholder'/'guide' в config_versions_entity_type_check
-- (V002__config_versions.sql) — те же значения, что новые
-- CONFIG_ENTITY_TYPE_PATTERN_PLACEHOLDER/CONFIG_ENTITY_TYPE_GUIDE в
-- platform-contracts/common/enums.proto (нижний регистр, snake_case, та же
-- ручная синхронизация proto<->CHECK, что и для всех остальных entity_type).

ALTER TABLE config.config_versions
    DROP CONSTRAINT config_versions_entity_type_check;

ALTER TABLE config.config_versions
    ADD CONSTRAINT config_versions_entity_type_check CHECK (entity_type IN (
        'pipeline', 'policy_ruleset', 'policy_template', 'billing_tariff',
        'routing_table', 'number_range', 'partner', 'operator', 'subscriber_consent',
        'category', 'ctn', 'pattern_placeholder', 'guide'
    ));
