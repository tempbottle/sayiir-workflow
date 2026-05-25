-- Performance/schema rework consolidated on top of the 0.5.0 release.
--
-- BREAKING: workflows running across this migration must be drained
-- and restarted. Pre-migration history rows have a NULL `data_hash`
-- and cannot be resolved by hash once the KV-offload cutover lands;
-- operators with in-flight task claims should stop workers first
-- (the dedicated `sayiir_task_claims` table is dropped below and
-- replaced by `sayiir_workflow_claims`).

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
ALTER TABLE sayiir_workflow_snapshots
    ALTER COLUMN definition_hash TYPE BYTEA
        USING CASE WHEN definition_hash IS NULL THEN NULL
                   ELSE decode(definition_hash, 'hex') END,
    ALTER COLUMN current_task_id TYPE BYTEA
        USING CASE WHEN current_task_id IS NULL THEN NULL
                   ELSE digest(current_task_id, 'sha256') END,
    ALTER COLUMN data DROP NOT NULL,
    ADD COLUMN IF NOT EXISTS history_version  INT NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS data_hash        BYTEA;

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

-- sayiir_task_claims → sayiir_workflow_claims --------------------------------
-- Keep the same physical table (preserve its indexes, statistics, and
-- any rows already drained by an operator stopping workers before the
-- migration); just rename, narrow the PK, and convert task_id to BYTEA.
-- One row per in-flight workflow now (only one task in flight per
-- instance, so the PK is just instance_id). Eligibility is a NOT
-- EXISTS join on this table; it stays small and hot in shared_buffers.
ALTER TABLE sayiir_task_claims RENAME TO sayiir_workflow_claims;
ALTER TABLE sayiir_workflow_claims
    ALTER COLUMN task_id TYPE BYTEA
        USING digest(task_id, 'sha256');
ALTER TABLE sayiir_workflow_claims DROP CONSTRAINT sayiir_task_claims_pkey;
ALTER TABLE sayiir_workflow_claims ADD PRIMARY KEY (instance_id);

COMMENT ON COLUMN sayiir_workflow_snapshots.data_hash IS
    'WIP: SHA-256 of the current snapshot blob. Mirrors history.data_hash at s.history_version.';
COMMENT ON COLUMN sayiir_workflow_snapshot_history.data_hash IS
    'WIP: SHA-256 of the encoded blob. Pre-migration rows are NULL and unsupported by the future KV offload.';
