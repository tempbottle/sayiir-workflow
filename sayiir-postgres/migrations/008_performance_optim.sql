-- Performance/schema rework consolidated on top of the 0.5.0 release.
--
-- BREAKING: workflows running across this migration must be drained
-- and restarted; pre-migration history rows have a NULL `data_hash`
-- and cannot be resolved by hash once the KV-offload cutover lands.
-- Operators with in-flight task claims should stop workers first
-- (the dedicated claim table is dropped below).

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- sayiir_workflow_snapshots ---------------------------------------------------
--   - definition_hash / current_task_id move from hex TEXT to raw
--     BYTEA: half the storage, no per-bind to_hex() allocation.
--   - data drops NOT NULL: history becomes the canonical blob store.
--     New saves stop populating snapshots.data.
--   - history_version: row-local counter, replaces a racy
--     SELECT MAX(version)+1 subquery in save_snapshot.
--   - data_hash: mirrors history.data_hash at history_version so the
--     future KV cutover can resolve the current blob without the
--     history JOIN.
--   - claim_owner / claim_expires_at: fold the old sayiir_task_claims
--     table into the snapshot row. Deliberately left un-indexed so
--     claim/release/heartbeat UPDATEs stay HOT-eligible.
ALTER TABLE sayiir_workflow_snapshots
    ALTER COLUMN definition_hash TYPE BYTEA
        USING CASE WHEN definition_hash IS NULL THEN NULL
                   ELSE decode(definition_hash, 'hex') END,
    ALTER COLUMN current_task_id TYPE BYTEA
        USING CASE WHEN current_task_id IS NULL THEN NULL
                   ELSE digest(current_task_id, 'sha256') END,
    ALTER COLUMN data DROP NOT NULL,
    ADD COLUMN IF NOT EXISTS history_version  INT NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS data_hash        BYTEA,
    ADD COLUMN IF NOT EXISTS claim_owner      TEXT,
    ADD COLUMN IF NOT EXISTS claim_expires_at TIMESTAMPTZ;

UPDATE sayiir_workflow_snapshots s
SET history_version = COALESCE(
    (SELECT MAX(version) FROM sayiir_workflow_snapshot_history h
     WHERE h.instance_id = s.instance_id),
    0
);

-- sayiir_workflow_snapshot_history --------------------------------------------
ALTER TABLE sayiir_workflow_snapshot_history
    ALTER COLUMN current_task_id TYPE BYTEA
        USING CASE WHEN current_task_id IS NULL THEN NULL
                   ELSE digest(current_task_id, 'sha256') END,
    ADD COLUMN IF NOT EXISTS data_hash BYTEA;

-- sayiir_workflow_tasks -------------------------------------------------------
-- output: per-task result, split out of the snapshot blob so late-stage
-- saves stop being quadratic in WAL volume.
ALTER TABLE sayiir_workflow_tasks
    ALTER COLUMN task_id TYPE BYTEA
        USING digest(task_id, 'sha256'),
    ADD COLUMN IF NOT EXISTS output BYTEA;

-- Polling hot-path index. Selectivity-only — the SELECT's ORDER BY is
-- non-indexable. Pre-create with CREATE INDEX CONCURRENTLY on large
-- tables; sqlx migrations run inside a transaction so this build is
-- blocking otherwise.
CREATE INDEX IF NOT EXISTS idx_snapshots_inprogress
    ON sayiir_workflow_snapshots (task_priority, updated_at)
    WHERE status = 'InProgress';

-- Hex-rendering view for ad-hoc ops queries against the now-BYTEA ids.
CREATE OR REPLACE VIEW sayiir_workflow_snapshots_hex AS
SELECT
    instance_id,
    status,
    encode(definition_hash, 'hex')  AS definition_hash,
    encode(current_task_id, 'hex')  AS current_task_id,
    completed_task_count,
    error,
    started_at,
    completed_at,
    updated_at
FROM sayiir_workflow_snapshots;

DROP TABLE IF EXISTS sayiir_task_claims;

COMMENT ON COLUMN sayiir_workflow_snapshots.data_hash IS
    'WIP: SHA-256 of the current snapshot blob. Mirrors history.data_hash at s.history_version.';
COMMENT ON COLUMN sayiir_workflow_snapshot_history.data_hash IS
    'WIP: SHA-256 of the encoded blob. Pre-migration rows are NULL and unsupported by the future KV offload.';
