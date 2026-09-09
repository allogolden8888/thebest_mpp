-- V031__staff_accounts.sql
-- luminous-hugging-charm.md, BACKOFFICE_DESIGN_SPEC.md Экран 33 "Admin users".
-- Реальный Keycloak не развёрнут нигде (LoginView.vue сегодня буквально
-- принимает вставленный JWT в textarea, HTTP-раунд-трипа при логине нет);
-- пользователь явно отклонил LDAP, настоящий Keycloak — "сильно позже".
-- До этого — полноценное локальное управление аккаунтами: логин/пароль
-- прямо из бэкофиса. Тот же стиль identity-таблицы, что уже есть для
-- партнёрских пользователей (iam.partner_portal_users, V025) — но там
-- external_id = Keycloak sub (заведён заранее под будущее использование),
-- здесь Keycloak sub взять неоткуда, поэтому external_id = username.

CREATE TABLE iam.staff_accounts (
    id            BIGSERIAL PRIMARY KEY,
    external_id   TEXT NOT NULL UNIQUE,
    username      TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    display_name  TEXT NOT NULL,
    active        BOOLEAN NOT NULL DEFAULT true,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_by    TEXT NOT NULL
);

-- Backfill: iam.staff_role_assignments.external_id (V025) — свободная
-- строка без FK, у неё уже могут быть значения, для которых в
-- iam.staff_accounts ещё нет строки (существующие Keycloak-based staff
-- назначения, сделанные до этой миграции). Заводим заглушки — случайный,
-- невозможный для bcrypt.CompareHashAndPassword хеш (не пустая строка —
-- пустой password_hash сравнивался бы с чем угодно неопределённо, лучше
-- заведомо невалидный bcrypt-хеш той же длины/формата), active=false
-- (эти аккаунты не создавались через новый локальный логин и не должны
-- быть доступны для входа по паролю, пока администратор явно не заведёт
-- им новый пароль через AdminUsersView.vue) — не ломает существующие
-- role assignments, позволяет добавить FK ниже безопасно.
INSERT INTO iam.staff_accounts (external_id, username, password_hash, display_name, active, created_by)
SELECT DISTINCT sra.external_id, sra.external_id,
    '$2a$12$invalidinvalidinvalidueinvalidinvalidinvalidinvalidinva',
    sra.external_id, false, 'migration'
FROM iam.staff_role_assignments sra
WHERE NOT EXISTS (
    SELECT 1 FROM iam.staff_accounts sa WHERE sa.external_id = sra.external_id
);

ALTER TABLE iam.staff_role_assignments
    ADD CONSTRAINT staff_role_assignments_external_id_fk
    FOREIGN KEY (external_id) REFERENCES iam.staff_accounts (external_id);
