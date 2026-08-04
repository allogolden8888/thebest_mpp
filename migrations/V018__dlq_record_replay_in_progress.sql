-- V018__dlq_record_replay_in_progress.sql
-- CODE_REVIEW.md CRITICAL finding (replay-service, #2): replay-service's
-- RequestReplay used to load_dlq_record (plain SELECT) and only later
-- MarkReplayed (UPDATE, after a successful republish), with no atomic
-- claim step between them. Two concurrent RequestReplay calls for the
-- same stage_execution_id (Backoffice UI double-click, gRPC client retry
-- after timeout, two Backoffice API pods proxying the same action) could
-- both observe replay_status='pending', both pass check_idempotency (the
-- only guard against reprocessing), and both republish — a DELIVERY-stage
-- record could be sent to the subscriber's handset twice, a BILLING-stage
-- record could be charged twice.
--
-- Fix: replay-service now claims a record atomically via
-- `UPDATE ... SET replay_status = 'in_progress' WHERE replay_status =
-- 'pending' RETURNING ...` — only one concurrent caller can win. This
-- migration widens the CHECK constraint to allow the new 'in_progress'
-- value; a record that fails a later safety check (billing/delivery
-- unsafe) is released back to 'pending' by the same service
-- (ReleaseClaim), so no other consumer of this table needs to know about
-- the new value to keep working correctly.

ALTER TABLE messaging.dlq_record DROP CONSTRAINT dlq_record_replay_status_check;
ALTER TABLE messaging.dlq_record ADD CONSTRAINT dlq_record_replay_status_check
    CHECK (replay_status IN ('pending', 'in_progress', 'replayed', 'expired'));
