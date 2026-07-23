-- V011__number_range.sql
-- data_infrastructure_spec.md §1.9a. range_start/range_end — числовое
-- представление MSISDN (BIGINT), только для этой таблицы: диапазонное
-- сравнение выигрывает от числового типа. В остальной системе msisdn —
-- TEXT (так же, как в platform-contracts SmsPayload.msisdn), это решение
-- локально для number_range, не смена канонического представления платформы.

CREATE TABLE routing.number_range (
    id           BIGSERIAL PRIMARY KEY,
    range_start  BIGINT NOT NULL,
    range_end    BIGINT NOT NULL,
    operator_id  TEXT NOT NULL,
    version      INT NOT NULL,
    status       TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'archived')),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT number_range_valid_range CHECK (range_end >= range_start)
);

CREATE INDEX number_range_lookup_idx ON routing.number_range (range_start, range_end)
    WHERE status = 'active';

-- Подтверждённые данные из чата (data_infrastructure_spec.md §1.9a) — неполный
-- список, только префиксы, явно проверенные на реальных номерах.
-- Формат MSISDN: 998 (код страны) + 2-значный префикс + 7 цифр абонента.
INSERT INTO routing.number_range (range_start, range_end, operator_id, version, status) VALUES
    (998900000000, 998909999999, 'beeline',  1, 'active'), -- префикс 90
    (998910000000, 998919999999, 'beeline',  1, 'active'), -- префикс 91
    (998920000000, 998929999999, 'beeline',  1, 'active'), -- префикс 92
    (998200000000, 998209999999, 'beeline',  1, 'active'), -- префикс 20
    (998500000000, 998509999999, 'ucell',    1, 'active'), -- префикс 50
    (998930000000, 998939999999, 'ucell',    1, 'active'), -- префикс 93
    (998940000000, 998949999999, 'ucell',    1, 'active'), -- префикс 94
    (998980000000, 998989999999, 'uzmobile', 1, 'active'), -- префикс 98
    (998990000000, 998999999999, 'uzmobile', 1, 'active'); -- префикс 99
