-- V028__incident.sql
-- luminous-hugging-charm.md Фаза 7 — инцидент-менеджмент.
-- control.execution_control_audit (V016) уже пишет каждый override (scope,
-- scope_id, state, reason, requested_by, expires_at) — это сырьё, не
-- концепция группировки. Эта миграция добавляет саму концепцию: именованный,
-- отслеживаемый инцидент с таймлайном (набор связанных override-строк) и
-- постмортемом при закрытии.

CREATE SCHEMA IF NOT EXISTS incident;

CREATE TABLE incident.incidents (
    id                 BIGSERIAL PRIMARY KEY,
    title              TEXT NOT NULL,
    -- Небольшой фиксированный enum — тот же стиль, что control.execution_
    -- control_audit.scope/state (V016): TEXT + CHECK, не отдельная lookup-
    -- таблица, набор значений стабилен и мал.
    severity           TEXT NOT NULL CHECK (severity IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')),
    status             TEXT NOT NULL CHECK (status IN ('OPEN', 'RESOLVED')) DEFAULT 'OPEN',
    opened_by          TEXT NOT NULL,
    opened_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_by        TEXT NULL,
    resolved_at        TIMESTAMPTZ NULL,
    -- NOT NULL не форсируется на уровне CHECK здесь (пусто, пока инцидент
    -- OPEN) — требование "постмортем обязателен для закрытия" применяется
    -- на уровне incident-service.ResolveIncident (codes.InvalidArgument при
    -- пустом postmortem_notes), не на уровне БД, симметрично тому, как
    -- admission_rate валидируется явно в execution-control-service
    -- grpcserver ДО того, как дойти до CHECK-ограничения V016.
    postmortem_notes   TEXT NULL
);

-- Список открытых инцидентов ("что горит прямо сейчас") — самый частый
-- запрос backoffice-ui списка.
CREATE INDEX incidents_status_idx ON incident.incidents (status, opened_at DESC);

-- incident_notes IS сам таймлайн/audit-trail для заметок — в отличие от
-- iam.staff_role_assignments/credentials.issued_secrets, здесь нет отдельной
-- мутации + append-only audit таблицы: заметка сама по себе — событие,
-- добавление строки уже полный аудит-след (тот же принцип, что README этой
-- фазы явно проговаривает: opened_at/resolved_at на incidents — аудит-след
-- для переходов статуса, incident_notes — аудит-след для заметок).
CREATE TABLE incident.incident_notes (
    id            BIGSERIAL PRIMARY KEY,
    incident_id   BIGINT NOT NULL REFERENCES incident.incidents (id),
    author        TEXT NOT NULL,
    note          TEXT NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX incident_notes_incident_id_idx ON incident.incident_notes (incident_id, created_at);

-- Линковка override -> инцидент, без хрупкого join по времени. Nullable —
-- большинство override НЕ связаны ни с одним инцидентом (ручной override без
-- открытого разбора). Намеренно БЕЗ FOREIGN KEY через границу схем: тот же
-- конвенционный контраст, что уже виден в этой кодовой базе между
-- iam.staff_role_assignments.role_id (FK НА iam.roles.id — тот же владелец
-- схемы, ФК безопасен) и billing.reconciliation_audit/messaging.replay_audit
-- (ни один не несёт FK ни на что за пределами собственной таблицы) —
-- control.execution_control_audit принадлежит execution-control-service,
-- incident.incidents принадлежит incident-service (этой фазе); жёсткий FK
-- через эту границу владения сцепил бы схемы двух независимо
-- разворачиваемых сервисов на уровне DDL так, как нигде больше в этой
-- кодовой базе не сделано намеренно.
ALTER TABLE control.execution_control_audit ADD COLUMN incident_id BIGINT NULL;

-- Partial — подавляющее большинство строк execution_control_audit никогда
-- не получат incident_id (обычные override вне разбора инцидента), полный
-- индекс тратил бы место без пользы. Обслуживает
-- TimelineForIncident(incident_id) -> ORDER BY created_at.
CREATE INDEX execution_control_audit_incident_id_idx
    ON control.execution_control_audit (incident_id, created_at) WHERE incident_id IS NOT NULL;
