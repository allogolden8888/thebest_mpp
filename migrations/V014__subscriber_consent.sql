-- V014__subscriber_consent.sql
-- data_infrastructure_spec.md §1.9c. Source of truth для consent-блэклистов
-- (opt-out по категории/отправителю) — не только Redis, см. комментарий там же
-- про compliance-риск при потере без резервной копии.

CREATE TABLE policy.subscriber_consent (
    msisdn       TEXT NOT NULL,
    scope_type   TEXT NOT NULL CHECK (scope_type IN ('CATEGORY', 'SENDER')),
    scope_value  TEXT NOT NULL,
    channel      TEXT NOT NULL DEFAULT 'SMS',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (msisdn, scope_type, scope_value, channel)
);

CREATE INDEX subscriber_consent_msisdn_idx ON policy.subscriber_consent (msisdn);
