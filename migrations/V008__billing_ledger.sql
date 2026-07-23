-- V008__billing_ledger.sql
-- data_infrastructure_spec.md §1.7. Append-only — вставка только через
-- INSERT ... ON CONFLICT (charge_id) DO NOTHING (HLD §15.1/§15.4),
-- апдейтов и удалений в приложении быть не должно.
--
-- НАЙДЕНО при переходе на DDL: конкретное правило "source_charge_id
-- заполнено только для compensating" (data_infrastructure_spec.md §1.7)
-- было текстовым примечанием, не инвариантом — теперь CHECK constraint,
-- невозможно вставить compensating-запись без source_charge_id на уровне БД.

CREATE TABLE billing.billing_ledger (
    id                BIGSERIAL PRIMARY KEY,
    charge_id         UUID NOT NULL UNIQUE,
    account_id        TEXT NOT NULL,
    partner_id        TEXT NOT NULL,
    amount            NUMERIC(18, 4) NOT NULL,
    currency          CHAR(3) NOT NULL,
    entry_type        TEXT NOT NULL CHECK (entry_type IN ('charge', 'compensating')),
    source_charge_id  UUID NULL,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT billing_ledger_compensating_source_required
        CHECK (entry_type <> 'compensating' OR source_charge_id IS NOT NULL),
    CONSTRAINT billing_ledger_charge_source_forbidden
        CHECK (entry_type <> 'charge' OR source_charge_id IS NULL)
);

CREATE INDEX billing_ledger_account_idx ON billing.billing_ledger (account_id, created_at);
CREATE INDEX billing_ledger_partner_idx ON billing.billing_ledger (partner_id, created_at);
