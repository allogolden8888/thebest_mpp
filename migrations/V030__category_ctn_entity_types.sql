-- V030__category_ctn_entity_types.sql
-- luminous-hugging-charm.md, BACKOFFICE_DESIGN_SPEC.md Экраны 32/23.
-- Добавляет 'category'/'ctn' в config_versions_entity_type_check
-- (V002__config_versions.sql) — те же значения, что новые
-- CONFIG_ENTITY_TYPE_CATEGORY/CONFIG_ENTITY_TYPE_CTN в
-- platform-contracts/common/enums.proto (нижний регистр, snake_case,
-- та же ручная синхронизация proto<->CHECK, что и для всех
-- остальных entity_type — единой генерации схемы из proto нет,
-- см. комментарий в V002).

ALTER TABLE config.config_versions
    DROP CONSTRAINT config_versions_entity_type_check;

ALTER TABLE config.config_versions
    ADD CONSTRAINT config_versions_entity_type_check CHECK (entity_type IN (
        'pipeline', 'policy_ruleset', 'policy_template', 'billing_tariff',
        'routing_table', 'number_range', 'partner', 'operator', 'subscriber_consent',
        'category', 'ctn'
    ));

-- Сид: категории существовали только как соглашение в документации
-- (BACKOFFICE_DESIGN_SPEC.md §1.4) — SERVICE/TRANSACTION/ADVERTISING/
-- UNTEMPLATED/BLOCKED. Заводим их как первые активные версии, чтобы
-- ничего не сломалось и админ увидел уже знакомый словарь, готовый к
-- редактированию/расширению. version=1, created_by='migration' — тот
-- же класс one-off системного actor'а, что уже используется в
-- V022__policy_template_demo_seed.sql.
INSERT INTO config.config_versions (entity_type, entity_id, version, payload, status, created_by)
VALUES
    ('category', 'SERVICE', 1, '{"name":"SERVICE","regex":"","count_in_cdr":true}', 'active', 'migration'),
    ('category', 'TRANSACTION', 1, '{"name":"TRANSACTION","regex":"","count_in_cdr":true}', 'active', 'migration'),
    ('category', 'ADVERTISING', 1, '{"name":"ADVERTISING","regex":"","count_in_cdr":true}', 'active', 'migration'),
    ('category', 'UNTEMPLATED', 1, '{"name":"UNTEMPLATED","regex":"","count_in_cdr":true}', 'active', 'migration'),
    ('category', 'BLOCKED', 1, '{"name":"BLOCKED","regex":"","count_in_cdr":false}', 'active', 'migration');
