-- Fold the `sayiir_task_claims` table into two columns on
-- `sayiir_workflow_snapshots`.
--
-- Rationale: the only purpose of task_claims was per-task exclusion during
-- execution. Each claim was an INSERT followed by a DELETE — two row writes
-- per task that produced no persistent value, only dispatch coordination.
-- Folding the claim into the snapshot row eliminates these writes:
--
--   - `claim_owner`       : worker_id currently executing this instance's
--                           current_task_id, NULL when not claimed.
--   - `claim_expires_at`  : TTL deadline, NULL when not claimed. Workers
--                           heartbeat by UPDATEing this column.
--
-- Claim acquisition is `pg_try_advisory_xact_lock(hashtext(instance_id))`
-- (fast-fail on contention) followed by a conditional UPDATE that sets the
-- two columns. Release UPDATEs them back to NULL. Heartbeat UPDATEs
-- `claim_expires_at`. find_available_tasks filters on
-- `(claim_owner IS NULL OR claim_expires_at < now())` — same eligibility,
-- one row to look at instead of a JOIN.
--
-- Pre-existing claims in `sayiir_task_claims` (if any) are NOT migrated —
-- on rollout, the assumption is that workers are stopped or any in-flight
-- claims naturally expire. The two new columns default to NULL which makes
-- every existing snapshot immediately claimable by the post-migration code.

ALTER TABLE sayiir_workflow_snapshots
    ADD COLUMN IF NOT EXISTS claim_owner       TEXT,
    ADD COLUMN IF NOT EXISTS claim_expires_at  TIMESTAMPTZ;

-- Used by `(claim_owner IS NULL OR claim_expires_at < now())` filter in
-- the polling dequeue. Partial index on the "currently claimed" set keeps
-- it small even with many terminal-state snapshots in the table.
CREATE INDEX IF NOT EXISTS idx_snapshots_claim_expires
    ON sayiir_workflow_snapshots (claim_expires_at)
    WHERE claim_expires_at IS NOT NULL;

-- View created in migration 009; drop before the underlying table.
DROP VIEW IF EXISTS sayiir_task_claims_hex;
DROP TABLE IF EXISTS sayiir_task_claims;
