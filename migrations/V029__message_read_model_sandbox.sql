-- V029__message_read_model_sandbox.sql
-- (было V028 в ветке subagent-2 — перенумеровано при слиянии с subagent-1,
-- который независимо занял V028 под incident-service.)
--
-- Фаза 11 плана закрытия API-пробелов ("Sandbox mode"): без этой колонки
-- sandbox-сообщения в backoffice-ui/partner-api-отчётах и ClickHouse
-- неотличимы от настоящего трафика — "почему за это сообщение никто не
-- списал денег и не было реальной отправки" превращается в загадку при
-- разборе инцидента. lifecycle-writer заполняет её напрямую из
-- IncomingMessage.sandbox при первом появлении сообщения (incoming.messages),
-- не через message.lifecycle/message-state-resolver — тот флаг уже известен
-- на этом, более раннем шаге, и не меняется дальше по ходу пайплайна.

ALTER TABLE messaging.message_read_model
    ADD COLUMN sandbox BOOLEAN NOT NULL DEFAULT false;
