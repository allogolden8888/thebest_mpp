-- V017__backoffice_stub.sql
-- data_infrastructure_spec.md §1.11: "RBAC/пользователи Backoffice —
-- отдельная небольшая схема backoffice, детали — LLD Backoffice API."
-- Минимальная заглушка, чтобы схема существовала и не блокировала
-- остальные миграции — полная модель (роли, права, SSO-интеграция)
-- намеренно НЕ специфицируется здесь.

CREATE TABLE backoffice.users (
    id          BIGSERIAL PRIMARY KEY,
    external_id TEXT NOT NULL UNIQUE, -- из внешнего IdP (Keycloak, HLD/services_specifictaion.md §8.3)
    display_name TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE backoffice.users IS
    'Заглушка. Полная RBAC-модель (роли, права, привязка к операциям) — отдельный LLD Backoffice API.';
