-- V012__number_portability_override.sql
-- data_infrastructure_spec.md §1.9a. Проверяется Destination Resolution
-- ДО number_range — точное совпадение по msisdn перекрывает диапазон.
-- Наполнение — периодическая синхронизация из внешней MNP-базы (источник
-- и частота — открытый вопрос, ещё не решён).

CREATE TABLE routing.number_portability_override (
    msisdn       TEXT PRIMARY KEY,
    operator_id  TEXT NOT NULL,
    ported_at    TIMESTAMPTZ NOT NULL,
    source       TEXT NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
