-- Migrate task identifier columns from hex-encoded TEXT to raw BYTEA.
--
-- The runtime now uses fixed-size 32-byte SHA-256 hashes (`TaskId`,
-- `DefinitionHash`) for all internal task / workflow-definition identifiers.
-- Storing them as hex-encoded TEXT doubled the on-disk size and forced a
-- per-bind `to_hex()` allocation on every write. Switching the columns to
-- BYTEA halves the storage, makes index leaves twice as dense, and lets the
-- application bind raw byte slices with zero allocation.
--
-- Column-by-column conversion:
--   - `task_id` / `current_task_id` were the user-facing task name as plain
--     text (e.g. `"step1"`, `"approval"`). The new runtime expects 32 bytes
--     of `sha256(name)`, so we hash the existing values in place.
--   - `definition_hash` was already a hex SHA-256 string (computed by
--     `compute_definition_hash`), so we just decode the hex back to bytes.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- sayiir_workflow_snapshots ----------------------------------------------------
ALTER TABLE sayiir_workflow_snapshots
    ALTER COLUMN definition_hash TYPE BYTEA
        USING CASE WHEN definition_hash IS NULL THEN NULL
                   ELSE decode(definition_hash, 'hex') END;

ALTER TABLE sayiir_workflow_snapshots
    ALTER COLUMN current_task_id TYPE BYTEA
        USING CASE WHEN current_task_id IS NULL THEN NULL
                   ELSE digest(current_task_id, 'sha256') END;

-- sayiir_workflow_snapshot_history --------------------------------------------
ALTER TABLE sayiir_workflow_snapshot_history
    ALTER COLUMN current_task_id TYPE BYTEA
        USING CASE WHEN current_task_id IS NULL THEN NULL
                   ELSE digest(current_task_id, 'sha256') END;

-- sayiir_workflow_tasks -------------------------------------------------------
ALTER TABLE sayiir_workflow_tasks
    ALTER COLUMN task_id TYPE BYTEA
        USING digest(task_id, 'sha256');

-- sayiir_task_claims ----------------------------------------------------------
ALTER TABLE sayiir_task_claims
    ALTER COLUMN task_id TYPE BYTEA
        USING digest(task_id, 'sha256');

-- Indexes on the converted columns rebuild automatically when the column type
-- changes. The B-tree on BYTEA uses raw byte ordering, which is equivalent to
-- the prior TEXT ordering for hex strings.

-- Convenience view for ops / debugging: render binary ids as lowercase hex
-- so engineers can keep doing `WHERE task_id = '9deb65b8...'` against a view.
CREATE OR REPLACE VIEW sayiir_workflow_snapshots_hex AS
SELECT
    instance_id,
    status,
    encode(definition_hash, 'hex') AS definition_hash,
    encode(current_task_id, 'hex') AS current_task_id,
    completed_task_count,
    error,
    started_at,
    completed_at,
    updated_at
FROM sayiir_workflow_snapshots;

CREATE OR REPLACE VIEW sayiir_task_claims_hex AS
SELECT
    instance_id,
    encode(task_id, 'hex') AS task_id,
    worker_id,
    claimed_at,
    expires_at
FROM sayiir_task_claims;
