-- Decouple claim ownership from the snapshot row.
--
-- Migration 008 folded `sayiir_task_claims` into two columns on
-- `sayiir_workflow_snapshots` (`claim_owner`, `claim_expires_at`) on
-- the theory that claim/release UPDATEs would be HOT-eligible and
-- save one round-trip per dispatch. In practice the fold added two
-- extra UPDATEs per task tick on the *same* wide snapshot row that
-- save_snapshot / save_task_result already write to. Under
-- synchronous_commit=on each commit pays the WAL fsync, so the
-- per-tick WAL volume on the snapshot row roughly tripled, and the
-- polling `FOR UPDATE OF s SKIP LOCKED` in find_available_tasks
-- started fighting with release_task's UPDATE on the same row.
--
-- Move ownership into a dedicated narrow table keyed on
-- `instance_id`. Only one task is in flight per workflow at a time,
-- so the PK is `instance_id` (not `(instance_id, task_id)` as in
-- the original task_claims). claim_task UPSERTs this row,
-- release_task DELETEs it, extend_task_claim UPDATEs the
-- expires_at. The eligibility predicate in dispatch SELECTs becomes
-- a NOT EXISTS join — small overhead because the table only holds
-- in-flight workflows and stays hot in shared_buffers.

CREATE TABLE IF NOT EXISTS sayiir_workflow_claims (
    instance_id TEXT        NOT NULL PRIMARY KEY,
    task_id     BYTEA       NOT NULL,
    worker_id   TEXT        NOT NULL,
    claimed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at  TIMESTAMPTZ
);

-- Sweeper helper: identify expired claim slots for ops queries /
-- future recovery sweeps. Partial — only TTL'd rows show up here, so
-- `NULL`-expires (no-TTL) claims don't bloat the index.
CREATE INDEX IF NOT EXISTS idx_workflow_claims_expires
    ON sayiir_workflow_claims (expires_at)
    WHERE expires_at IS NOT NULL;

-- The fold columns are obsolete. Dropping them shrinks every
-- snapshot row and removes the dead write target from save_snapshot.
ALTER TABLE sayiir_workflow_snapshots
    DROP COLUMN IF EXISTS claim_owner,
    DROP COLUMN IF EXISTS claim_expires_at;
