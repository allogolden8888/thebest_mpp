-- V019__message_read_model_lifecycle_version.sql
-- CODE_REVIEW.md MEDIUM finding (lifecycle-writer #3): UpdateReadModel
-- unconditionally overwrote current_status/terminal with no ordering
-- guard against out-of-order/duplicate redelivery (a Kafka partition
-- rebalance causing an older SUBMITTED event to redeliver and reprocess
-- after a newer DELIVERED was already applied could regress the
-- partner-facing read model backward). Nullable/default 0 so existing
-- rows (written before this column existed) never block a legitimate
-- first update.

ALTER TABLE messaging.message_read_model
    ADD COLUMN lifecycle_version BIGINT NOT NULL DEFAULT 0;
