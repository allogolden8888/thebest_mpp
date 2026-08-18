-- V025__iam.sql
-- luminous-hugging-charm.md Фаза 0 (Identity/RBAC/Audit, фундамент нового
-- 12-фазного плана закрытия API-пробелов). Прямая замена заглушки
-- V017__backoffice_stub.sql (backoffice.users) — та таблица никем не
-- читается в коде (только упоминается в комментариях/README), оставлена
-- как есть по конвенции "не переписывать прошлые миграции", просто больше
-- не источник истины: iam.staff_role_assignments — теперь он.

CREATE SCHEMA IF NOT EXISTS iam;

CREATE TABLE iam.permissions (
    id          BIGSERIAL PRIMARY KEY,
    name        TEXT NOT NULL UNIQUE,
    description TEXT NOT NULL
);

CREATE TABLE iam.roles (
    id          BIGSERIAL PRIMARY KEY,
    name        TEXT NOT NULL UNIQUE,
    description TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE iam.role_permissions (
    role_id       BIGINT NOT NULL REFERENCES iam.roles (id),
    permission_id BIGINT NOT NULL REFERENCES iam.permissions (id),
    PRIMARY KEY (role_id, permission_id)
);

-- external_id — Keycloak `sub` claim. Не удаляем строку на отзыв (revoked_at
-- заполняется вместо DELETE) — иначе "кто и когда имел доступ" было бы
-- невосстановимо, ровно то же обоснование, что у append-only *_audit таблиц,
-- но здесь ещё и текущее состояние читается из этой же таблицы
-- (revoked_at IS NULL), не только audit-хвост.
CREATE TABLE iam.staff_role_assignments (
    id          BIGSERIAL PRIMARY KEY,
    external_id TEXT NOT NULL,
    role_id     BIGINT NOT NULL REFERENCES iam.roles (id),
    granted_by  TEXT NOT NULL,
    granted_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    revoked_by  TEXT NULL,
    revoked_at  TIMESTAMPTZ NULL
);

CREATE INDEX staff_role_assignments_external_id_idx
    ON iam.staff_role_assignments (external_id) WHERE revoked_at IS NULL;

-- Один и тот же (external_id, role) не может быть назначен дважды
-- одновременно активно — предотвращает дублирующиеся живые гранты, не
-- мешает повторной выдаче после отзыва (частичный индекс, не CHECK).
CREATE UNIQUE INDEX staff_role_assignments_active_unique
    ON iam.staff_role_assignments (external_id, role_id) WHERE revoked_at IS NULL;

-- Учётки людей будущего партнёрского портала (Фаза 3) — заводится здесь,
-- не в Фазе 3, потому что Фаза 3 явно зависит от существования этой таблицы
-- (partner-self-service-api сверяет действие с ролью как defense-in-depth
-- поверх JWT claim). Пустая до Фазы 3, это не проблема — как и остальные
-- схемы этой сессии, заведённые заранее под будущего потребителя
-- (например policy.subscriber_consent задолго до compliance-api).
CREATE TABLE iam.partner_portal_users (
    id           BIGSERIAL PRIMARY KEY,
    external_id  TEXT NOT NULL UNIQUE,
    partner_id   TEXT NOT NULL,
    display_name TEXT NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX partner_portal_users_partner_id_idx ON iam.partner_portal_users (partner_id);

CREATE TABLE iam.partner_portal_role_assignments (
    id          BIGSERIAL PRIMARY KEY,
    external_id TEXT NOT NULL REFERENCES iam.partner_portal_users (external_id),
    role        TEXT NOT NULL CHECK (role IN ('partner-admin', 'partner-viewer')),
    granted_by  TEXT NOT NULL,
    granted_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    revoked_by  TEXT NULL,
    revoked_at  TIMESTAMPTZ NULL
);

CREATE INDEX partner_portal_role_assignments_external_id_idx
    ON iam.partner_portal_role_assignments (external_id) WHERE revoked_at IS NULL;

-- Append-only, тот же формат, что остальные *_audit таблицы этой сессии
-- (BIGSERIAL id, actor TEXT, created_at, без UPDATE/DELETE-пути в коде).
CREATE TABLE iam.identity_audit (
    id         BIGSERIAL PRIMARY KEY,
    actor      TEXT NOT NULL,
    action     TEXT NOT NULL,
    target     TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX identity_audit_created_at_idx ON iam.identity_audit (created_at);

-- Затравочные права — ровно то множество, что именует
-- luminous-hugging-charm.md Ф0 ("audit:read, credentials:issue,
-- incident:manage, compliance:write, support:trace, ops:read") плюс
-- декомпозиция существующих деструктивных операций backoffice-api,
-- которые раньше были за одной ролью backoffice-admin (config CRUD,
-- execution control override, force scheduler command, DLQ replay) —
-- без этого RequireRole("backoffice-admin") нечего было бы расширять
-- до "конкретных прав": сами права должны для начала существовать.
INSERT INTO iam.permissions (name, description) VALUES
    ('audit:read',           'Чтение объединённого Audit Log (GET /v1/audit)'),
    ('credentials:issue',    'Выпуск/ротация partner credentials (Фаза 1)'),
    ('incident:manage',      'Открытие/закрытие инцидентов, постмортемы (Фаза 7)'),
    ('compliance:write',     'Ручные записи consent/blacklist (Фаза 6)'),
    ('support:trace',        'Кросс-партнёрский поиск сообщений (Фаза 9)'),
    ('ops:read',             'Просмотр ops/инфра-видимости (Фаза 8)'),
    ('config:write',         'CRUD над config.versions (существующий backoffice-admin функционал)'),
    ('execution-control:write', 'Override/clear execution control (существующий backoffice-admin функционал)'),
    ('scheduler:force',      'Force scheduler command (существующий backoffice-admin функционал)'),
    ('replay:request',       'Запрос replay stage execution (существующий backoffice-admin функционал)'),
    ('dlq:manage',           'Просмотр/операции над DLQ (существующий backoffice-admin функционал)');

-- backoffice-admin — суперроль с полным набором прав, обеспечивает обратную
-- совместимость с текущим единственным-ролевым состоянием (не ломает
-- существующие Keycloak-токены с realm-ролью backoffice-admin, пока
-- Keycloak-сторона миграции ролей не завершена — координация вне
-- репозитория, см. допущение в начале luminous-hugging-charm.md).
INSERT INTO iam.roles (name, description) VALUES
    ('backoffice-admin', 'Полный административный доступ (суперроль, обратная совместимость)'),
    ('support-agent',    'audit:read + support:trace — саппорт без права мутировать конфигурацию'),
    ('compliance-officer', 'compliance:write — комплаенс-персонал без полного admin-доступа'),
    ('incident-manager', 'incident:manage — управление инцидентами без полного admin-доступа'),
    ('ops-viewer',       'ops:read — только просмотр ops-видимости');

INSERT INTO iam.role_permissions (role_id, permission_id)
SELECT r.id, p.id FROM iam.roles r CROSS JOIN iam.permissions p WHERE r.name = 'backoffice-admin';

INSERT INTO iam.role_permissions (role_id, permission_id)
SELECT r.id, p.id FROM iam.roles r, iam.permissions p
    WHERE r.name = 'support-agent' AND p.name IN ('audit:read', 'support:trace');

INSERT INTO iam.role_permissions (role_id, permission_id)
SELECT r.id, p.id FROM iam.roles r, iam.permissions p
    WHERE r.name = 'compliance-officer' AND p.name = 'compliance:write';

INSERT INTO iam.role_permissions (role_id, permission_id)
SELECT r.id, p.id FROM iam.roles r, iam.permissions p
    WHERE r.name = 'incident-manager' AND p.name = 'incident:manage';

INSERT INTO iam.role_permissions (role_id, permission_id)
SELECT r.id, p.id FROM iam.roles r, iam.permissions p
    WHERE r.name = 'ops-viewer' AND p.name = 'ops:read';
