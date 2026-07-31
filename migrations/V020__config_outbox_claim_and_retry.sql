-- V020__config_outbox_claim_and_retry.sql
-- CODE_REVIEW.md (services/config-event-publisher) findings #2 ("poison
-- message head-of-line blocking, no bound, no DLQ") and #3 ("no
-- SELECT ... FOR UPDATE SKIP LOCKED for multi-replica safety").
--
-- config_outbox had no way to (a) claim a row so >1 config-event-publisher
-- replica doesn't publish the same row twice, or (b) bound retries on a
-- row that can never publish (malformed payload, unknown entity_type) so
-- it stops crowding out real pending rows in poll_outbox's
-- `ORDER BY created_at ASC LIMIT N`. See
-- services/config-event-publisher/internal/outbox/outbox.go PollOutboxWithLimits.

ALTER TABLE config.config_outbox
    ADD COLUMN claimed_at TIMESTAMPTZ NULL,
    ADD COLUMN attempts   INT NOT NULL DEFAULT 0,
    ADD COLUMN last_error TEXT NULL;

COMMENT ON COLUMN config.config_outbox.claimed_at IS
    'Выставляется PollOutbox внутри SELECT ... FOR UPDATE SKIP LOCKED транзакции. '
    'Считается протухшим через claimTTL (по умолчанию 30с в config-event-publisher) '
    '— защита от навсегда "занятой" строки, если реплика упала между claim и '
    'mark_published/mark_publish_failed.';
COMMENT ON COLUMN config.config_outbox.attempts IS
    'Инкрементируется на каждый mark_publish_failed. PollOutbox перестаёт выбирать '
    'строку после maxAttempts (по умолчанию 10) — мягкий DLQ без отдельной таблицы: '
    'строка остаётся published=false, видна через прямой SQL-запрос оператором '
    '(WHERE attempts >= 10 AND published = false).';
COMMENT ON COLUMN config.config_outbox.last_error IS
    'Текст последней ошибки публикации — для диагностики застрявших (attempts '
    'исчерпаны) строк без необходимости смотреть логи конкретного пода.';
