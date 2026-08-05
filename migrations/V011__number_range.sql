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

-- Расширение 5.1 (development_plan.md) — общеизвестные префиксы тех же
-- ТРЁХ операторов, для которых в платформе реально есть connection-профили
-- (config_schemas/examples/operator.valid.json и т.д.) — умышленно НЕ
-- добавлены другие узбекистанские операторы (Perfectum/UMS-бренд отдельно,
-- Mobiuz и т.п.), т.к. без их connection-профиля запись в number_range
-- резолвила бы сообщение оператору, которому физически некуда его
-- доставить. Уровень достоверности НИЖЕ, чем у блока выше: это общее
-- знание о нумерации Узбекистана, не проверено на реальных номерах в этом
-- чате — расставлено как v1/active тем же способом, но требует такой же
-- полевой проверки, что уже отмечена как открытый пункт в
-- data_infrastructure_spec.md §1.9a.
INSERT INTO routing.number_range (range_start, range_end, operator_id, version, status) VALUES
    (998330000000, 998339999999, 'beeline',  1, 'active'), -- префикс 33
    (998950000000, 998959999999, 'uzmobile', 1, 'active'), -- префикс 95
    (998970000000, 998979999999, 'uzmobile', 1, 'active'), -- префикс 97
    (998880000000, 998889999999, 'uzmobile', 1, 'active'); -- префикс 88
