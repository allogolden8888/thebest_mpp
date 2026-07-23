-- V005__message_lifecycle_history.sql
-- data_infrastructure_spec.md §1.4.
--
-- ИСПРАВЛЕНО при переходе на DDL: капасити-модель (capacity_model.md §6.3)
-- явно рекомендует ПОЧАСОВОЕ партиционирование ("не суточное — при
-- 27-40к строк/с суточная партиция слишком велика для эффективного
-- индекса/vacuum"), а предыдущая версия data_infrastructure_spec.md §1.4
-- ошибочно говорила "суточные партиции" — несогласованность между двумя
-- документами, не замеченная до реального DDL. Здесь и в
-- data_infrastructure_spec.md зафиксировано согласованно: почасовые.

CREATE TABLE messaging.message_lifecycle_history (
    message_id        UUID NOT NULL,
    lifecycle_version  BIGINT NOT NULL,
    status              TEXT NOT NULL,
    event_id            UUID NOT NULL,
    occurred_at         TIMESTAMPTZ NOT NULL,
    source               TEXT NOT NULL,
    PRIMARY KEY (message_id, lifecycle_version, occurred_at)
) PARTITION BY RANGE (occurred_at);

CREATE INDEX message_lifecycle_history_message_idx ON messaging.message_lifecycle_history (message_id);

-- Партиции создаются функцией messaging.create_lifecycle_history_partition()
-- (V015__partition_maintenance.sql), retention — 72 часа (message_ttl 24ч +
-- буфер на расследования, capacity_model.md §6.3), не задаётся здесь.
