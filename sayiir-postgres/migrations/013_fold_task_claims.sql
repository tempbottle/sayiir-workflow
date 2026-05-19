-- Fold the `sayiir_task_claims` table into two columns on
-- `sayiir_workflow_snapshots` to drop one INSERT+DELETE per task:
--
--   - `claim_owner`       : worker_id holding the claim, NULL when free.
--   - `claim_expires_at`  : TTL deadline, NULL for an eternal claim.
--
-- Both columns are deliberately left un-indexed: claim/release/
-- heartbeat UPDATEs touch only these two columns, so a missing index
-- keeps them HOT-eligible. Polling selectivity comes from
-- `idx_snapshots_inprogress`; the claim predicate is a row-level check
-- on that already-narrow set.
--
-- Operators upgrading with in-flight claims should stop workers first;
-- rows in the old `sayiir_task_claims` table are not migrated.

ALTER TABLE sayiir_workflow_snapshots
    ADD COLUMN IF NOT EXISTS claim_owner       TEXT,
    ADD COLUMN IF NOT EXISTS claim_expires_at  TIMESTAMPTZ;

-- View created in migration 009; drop before the underlying table.
DROP VIEW IF EXISTS sayiir_task_claims_hex;
DROP TABLE IF EXISTS sayiir_task_claims;
