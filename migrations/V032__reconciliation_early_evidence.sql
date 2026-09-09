-- V032__reconciliation_early_evidence.sql
-- data_infrastructure_spec.md §1.9 (дополнение к V010).
--
-- ПОЧЕМУ таблица вообще появилась. Замер на живом стенде (ClickHouse,
-- analytics.stage_events): 16 187 сообщений получили финальный
-- RECONCILIATION_OUTCOME_CONFIRMED_NOT_SUBMITTED, и перекрёстный запрос
-- показал, что у ВСЕХ 16 187 стадия DELIVERY при этом была SUCCEEDED —
-- платформа объявляла успешно отправленные сообщения неотправленными.
-- Одна из двух причин этого — в delivery-reconciliation-service
-- свидетельство (DLR / operator.submit.accepted), пришедшее РАНЬШЕ, чем
-- pipeline-engine успел создать reconciliation case, молча выбрасывалось
-- (`store.loadByMessageId(...).ifPresent(...)` — нет case'а, нет и
-- получателя). Это не экзотика: SMSC стоит в одной сети со стендом, DLR
-- измеренно приходит через 20–70 мс после submit, тогда как создание
-- case'а идёт длинным путём delivery-service -> stage.completed ->
-- pipeline-engine -> stage.delivery-reconciliation, и регулярно
-- проигрывает эту гонку.
--
-- Эта таблица — durable «посадочная площадка» для такого раннего
-- свидетельства: consumer'ы пишут сюда, когда case'а ещё нет, а
-- handle_reconciliation_execute при создании case'а забирает накопленное
-- (drain) и мержит в reconciliation_cases.evidence.
--
-- ПОЧЕМУ отдельная таблица, а не lazy-создание самого case'а по
-- message_id: reconciliation_cases.stage_execution_id NOT NULL и попадает
-- в StageCompletedEvent — pipeline-engine по нему сопоставляет ответ со
-- своим ожиданием. Case, созданный «заранее» по DLR, не знает настоящего
-- stage_execution_id; выдуманный означал бы stage.completed, который
-- pipeline-engine не сможет сопоставить (а sweep по дедлайну вполне
-- успел бы такой case опубликовать). Отдельная таблица не может быть
-- ошибочно принята за case ни sweep'ом, ни аналитикой.
--
-- ПОЧЕМУ по колонке на вид свидетельства, а не один JSONB: каждый
-- источник (operator.submit.accepted / delivery.status / query_sm)
-- обновляет ТОЛЬКО свою колонку одним INSERT ... ON CONFLICT DO UPDATE.
-- Это делает накопление атомарным на уровне строки — без
-- read-modify-write, а значит без lost-update даже между двумя репликами
-- сервиса (replicas: 2, k8s/rendered/delivery-reconciliation-service.yaml),
-- где JVM-локи Main.caseLocks не помогают.
--
-- ОБЪЁМ. Строка появляется для сообщения, чьё свидетельство пришло, пока
-- case'а нет — а после исправления графа пайплайна в реконсиляцию идёт
-- только SUBMISSION_OUTCOME_UNKNOWN, т.е. для подавляющего большинства
-- сообщений case не появится никогда и строка останется висеть. Поэтому
-- она не вечная: строки старше окна реконсиляции удаляются sweep'ом
-- (Main.sweepDeadlines -> purgeEarlyEvidence); после окна свидетельство
-- всё равно бесполезно — case, который мог бы его забрать, к этому
-- моменту уже закрыт по дедлайну. При 300 сообщ/с и окне 2 минуты
-- установившийся размер таблицы — десятки тысяч строк, а не рост без
-- границы.
CREATE TABLE reconciliation.early_evidence (
    message_id      UUID PRIMARY KEY,
    submit_accepted BOOLEAN NOT NULL DEFAULT false,
    delivery_status TEXT NULL CHECK (delivery_status IN ('SUCCESS', 'FAILURE')),
    query_sm        TEXT NULL CHECK (query_sm IN ('CONFIRMED_DELIVERED', 'CONFIRMED_NOT_FOUND', 'INCONCLUSIVE')),
    first_seen_at   TIMESTAMPTZ NOT NULL DEFAULT now()
) WITH (fillfactor = 70); -- UPSERT-heavy, как messaging.message_read_model (V004)

-- Под purgeEarlyEvidence (DELETE ... WHERE first_seen_at < now() - окно).
CREATE INDEX early_evidence_first_seen_idx ON reconciliation.early_evidence (first_seen_at);

-- Уникальность case'а по message_id. Раньше индекс был обычным, а
-- handle_reconciliation_execute делал check-then-act (loadByMessageId ->
-- create): при at-least-once передоставке stage.delivery-reconciliation
-- или при ребалансе на двух репликах два процесса могли вставить ДВА
-- case'а для одного message_id, после чего loadByMessageId (fetchOptional)
-- начал бы бросать TooManyRows на КАЖДОЕ свидетельство этого сообщения —
-- то есть терялось бы уже всё свидетельство, а не только раннее. Теперь
-- вставка идёт через ON CONFLICT DO NOTHING и опирается на этот индекс.
--
-- Если на стенде уже успели накопиться дубли, построение индекса упадёт
-- здесь громко — это правильный сигнал: дубли надо развести руками
-- (оставить самый ранний case), а не прятать.
DROP INDEX IF EXISTS reconciliation.reconciliation_cases_message_idx;
CREATE UNIQUE INDEX reconciliation_cases_message_uniq ON reconciliation.reconciliation_cases (message_id);
