-- V034__chat_messages.sql
-- BACKOFFICE_DESIGN_SPEC.md Экран 27 "Chat" — до этой миграции ничего
-- похожего не существовало нигде в платформе (ни топика, ни таблицы для
-- сообщений админ<->партнёр, см. запись Экрана 27 в этом файле до фикса).
-- Один тред на partner_id (не многотредовость/каналы — design-референс
-- MPP Backoffice.dc.html показывает ровно один список сообщений на
-- партнёра слева, один тред справа), polling-доставка (см.
-- services/chat-service/README.md за разбором, почему НЕ websocket — этот
-- кодбейз нигде не поднимает websocket-инфраструктуру).
--
-- Новая схема support — ни одна существующая схема (V001__schemas.sql)
-- концептуально не подходит: не iam (identity/RBAC), не messaging (A2P
-- pipeline read model), не billing/policy/routing/reconciliation/control.
-- support:trace (V025__iam.sql) уже называет этот домен ("саппорт"), но
-- ссылается на messaging.message_read_model, не на отдельную схему —
-- support как схема заводится здесь впервые.
CREATE SCHEMA IF NOT EXISTS support;

CREATE TABLE support.chat_messages (
    id          BIGSERIAL PRIMARY KEY,
    partner_id  TEXT NOT NULL,
    -- Небольшой фиксированный enum — тот же стиль, что incident.incidents.
    -- severity/status (V028): TEXT + CHECK, не отдельная lookup-таблица.
    sender_type TEXT NOT NULL CHECK (sender_type IN ('partner', 'admin')),
    -- external_id отправителя — iam.staff_accounts.external_id для
    -- sender_type='admin', iam.partner_portal_users.external_id для
    -- sender_type='partner' (V025/V031). Намеренно БЕЗ FK на оба варианта
    -- (CHECK-зависимый FK на разные таблицы не выразим в Postgres без
    -- триггера) — тот же класс решения, что incident.incidents.opened_by/
    -- iam.staff_role_assignments.external_id: свободная строка, источник
    -- истины для "кто" — claims.Subject на стороне вызывающего сервиса, не
    -- ограничение на уровне БД.
    sender_id   TEXT NOT NULL,
    body        TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Nullable — NULL значит "непрочитано". Заполняется read_at=now() как
    -- побочный эффект ListMessages со стороны ПРОТИВОПОЛОЖНОГО sender_type
    -- (chat-service/internal/store/store.go ListMessages) — нет отдельного
    -- mark-as-read эндпоинта/UI-полировки (сознательная граница объёма,
    -- задачи задания: "no read-receipts UI polish"), только то, что нужно
    -- для счётчика "N новых" в сайдбаре backoffice-ui (design-референс,
    -- экран 27: "payme_uz 2 новых").
    read_at     TIMESTAMPTZ NULL
);

-- Обслуживает обе стороны: ListMessages(partner_id, since=created_at)
-- (WHERE partner_id=$1 AND created_at > $2 ORDER BY created_at) и
-- ListThreads' "последнее сообщение по каждому partner_id" (агрегат по
-- partner_id, ORDER BY created_at внутри группы).
CREATE INDEX chat_messages_partner_id_created_at_idx
    ON support.chat_messages (partner_id, created_at);

-- Обслуживает подсчёт непрочитанных на сайдбаре (ListThreads: COUNT(*)
-- WHERE sender_type='partner' AND read_at IS NULL, сгруппировано по
-- partner_id) — партиционный по неполноте (read_at почти всегда быстро
-- становится не-NULL после того, как админ открывает тред; полный индекс
-- тратил бы место без пользы на давно прочитанные сообщения, тот же
-- принцип, что execution_control_audit_incident_id_idx, V028).
CREATE INDEX chat_messages_unread_idx
    ON support.chat_messages (partner_id, sender_type) WHERE read_at IS NULL;

-- Право chat:write — luminous-hugging-charm.md-класс гранулярного права
-- (V025__iam.sql), гейтит ВСЕ операции экрана "Chat" (список тредов,
-- чтение сообщений конкретного партнёра, отправка) одним правом — тот же
-- класс решения, что incident:manage (V028) покрывает
-- open/list/get/notes/resolve одним правом, не отдельными read/write
-- правами на каждую операцию. Названо chat:write (не chat:read или
-- chat:moderate), потому что write — доминирующая, более чувствительная
-- операция этого экрана (сообщение уходит партнёру от имени платформы), и
-- этот кодбейз уже называет права по САМОЙ ЧУВСТВИТЕЛЬНОЙ операции экрана
-- (audit:read — искючение, там весь экран read-only; execution-control:write/
-- scheduler:force/replay:request/dlq:manage — по мутации, не по чтению).
INSERT INTO iam.permissions (name, description) VALUES
    ('chat:write', 'Чтение и отправка сообщений в чате админ<->партнёр (backoffice-ui Экран 27, право покрывает и чтение тредов)');

-- backoffice-admin — суперроль, получает все новые права так же явно, как
-- V026__iam_manage_permission.sql (не переигрываем V025's CROSS JOIN
-- задним числом — миграции append-only, та же конвенция).
INSERT INTO iam.role_permissions (role_id, permission_id)
SELECT r.id, p.id FROM iam.roles r, iam.permissions p
    WHERE r.name = 'backoffice-admin' AND p.name = 'chat:write';

-- support-agent — саппорт-роль (V025: "audit:read + support:trace — саппорт
-- без права мутировать конфигурацию") получает chat:write тоже: общение с
-- партнёром через чат — ровно та же категория работы, что кросс-
-- партнёрский поиск сообщений (support:trace), не конфигурационная мутация.
INSERT INTO iam.role_permissions (role_id, permission_id)
SELECT r.id, p.id FROM iam.roles r, iam.permissions p
    WHERE r.name = 'support-agent' AND p.name = 'chat:write';
