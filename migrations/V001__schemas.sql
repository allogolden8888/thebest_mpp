-- V001__schemas.sql
-- Схемы PostgreSQL по доменам (data_infrastructure_spec.md §1, HLD §17.1).
-- `backoffice` — минимальная заглушка под RBAC/пользователей Backoffice,
-- детали намеренно не специфицированы здесь (data_infrastructure_spec.md §1.11).

CREATE SCHEMA IF NOT EXISTS config;
CREATE SCHEMA IF NOT EXISTS messaging;
CREATE SCHEMA IF NOT EXISTS billing;
CREATE SCHEMA IF NOT EXISTS dlr;
CREATE SCHEMA IF NOT EXISTS routing;
CREATE SCHEMA IF NOT EXISTS policy;
CREATE SCHEMA IF NOT EXISTS reconciliation;
CREATE SCHEMA IF NOT EXISTS control;
CREATE SCHEMA IF NOT EXISTS backoffice;
