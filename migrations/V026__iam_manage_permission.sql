-- V026__iam_manage_permission.sql
-- luminous-hugging-charm.md Ф0 — найдено при проектировании backoffice-api
-- REST-обвязки над IamService (GET/POST/DELETE /v1/iam/*, backoffice-ui
-- "Access Control" -> Users & Roles): управление самими ролями/назначениями
-- — операция как минимум того же уровня чувствительности, что остальные
-- 6 прав, заведённых в V025__iam.sql, но сама V025 не называла для неё
-- отдельное право (план перечислял только audit/credentials/incident/
-- compliance/support/ops). Не переписываем V025 задним числом (конвенция
-- этой сессии — миграции append-only, см. V023 vs V011), заводим отдельно.

INSERT INTO iam.permissions (name, description) VALUES
    ('iam:manage', 'Управление ролями/назначениями (backoffice-ui Access Control -> Users & Roles)');

INSERT INTO iam.role_permissions (role_id, permission_id)
SELECT r.id, p.id FROM iam.roles r, iam.permissions p
    WHERE r.name = 'backoffice-admin' AND p.name = 'iam:manage';
