-- V013__policy_template.sql
-- data_infrastructure_spec.md §1.9b. Синтаксис pattern (%w, %d{n,m}) —
-- см. комментарий там же и services_specifictaion.md §2.5.

CREATE TABLE policy.policy_template (
    template_id  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    partner_id   TEXT NOT NULL,
    operator_id  TEXT NULL,
    channel      TEXT NOT NULL DEFAULT 'SMS' CHECK (channel IN ('SMS', 'EMAIL', 'PUSH')),
    category     TEXT NOT NULL,
    pattern      TEXT NOT NULL,
    version      INT NOT NULL,
    status       TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'archived')),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- category не может быть служебным значением, присваиваемым Policy
    -- на этапе выполнения (UNTEMPLATED/BLOCKED) — эти два зарезервированы
    -- и не должны существовать как категория реального шаблона.
    CONSTRAINT policy_template_category_not_reserved
        CHECK (category NOT IN ('UNTEMPLATED', 'BLOCKED'))
);

CREATE INDEX policy_template_partner_idx ON policy.policy_template (partner_id, channel, status);
CREATE INDEX policy_template_category_idx ON policy.policy_template (category);
CREATE INDEX policy_template_operator_idx ON policy.policy_template (operator_id) WHERE operator_id IS NOT NULL;

-- Реальный пример из чата — проверка, что колонка pattern действительно
-- вмещает шаблон с плейсхолдерами предложенного синтаксиса.
INSERT INTO policy.policy_template (partner_id, operator_id, channel, category, pattern, version, status)
VALUES (
    'korona_mmt',
    NULL,
    'SMS',
    'TRANSACTION',
    E'%w shartnoma bo''yicha %d{1,6} so''m to''lovni bugun amalga oshiring.'
    'Kechikish va jarimaga yo''l qo''ymang.To''lov ilovada amalga oshiriladi.KORONA MMT',
    1,
    'active'
);
