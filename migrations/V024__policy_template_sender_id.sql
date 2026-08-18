-- V024__policy_template_sender_id.sql
-- data_infrastructure_spec.md §1.9b. policy.policy_template (V013) скоупится
-- на partner_id (+опц. operator_id), но не на sender_id, хотя senders[]
-- уже существует как данные внутри partner.schema.json (у партнёра может
-- быть несколько alphaname/short-number отправителей). Без этой колонки
-- шаблон безусловно применяется ко ВСЕМ отправителям партнёра — нет способа
-- завести шаблон, специфичный для одного sender'а.
--
-- sender_id nullable по той же схеме, что и уже существующий operator_id:
-- NULL = шаблон применяется ко всем отправителям этого партнёра.
-- Ссылочная целостность (sender_id действительно принадлежит partner_id)
-- обеспечивается не FK на Postgres-таблицу — реестра отправителей как
-- отдельной таблицы сознательно нет (второй источник истины к
-- partner.schema.json), эта проверка выполняется в policy-service через
-- Redis-проекцию sender_id -> partner_id от config-cache-projector.

ALTER TABLE policy.policy_template ADD COLUMN sender_id TEXT NULL;

CREATE INDEX policy_template_sender_idx ON policy.policy_template (partner_id, sender_id);
