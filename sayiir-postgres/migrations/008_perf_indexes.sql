-- 1) Partial index for the polling hot path. The existing
--    `idx_snapshots_status` indexes every status value, so it bloats with
--    terminal rows and loses selectivity for `status = 'InProgress'`. A partial
--    index ordered by the dominant sort key collapses the polling SELECT to a
--    bounded index scan.
--
--    NOTE for operators with large existing tables: this CREATE INDEX is *not*
--    CONCURRENTLY (sqlx migrations run inside a transaction). Pre-create the
--    index manually with CREATE INDEX CONCURRENTLY before applying this
--    migration to avoid blocking writes; the `IF NOT EXISTS` here makes the
--    migration a no-op afterwards.
CREATE INDEX IF NOT EXISTS idx_snapshots_inprogress
    ON sayiir_workflow_snapshots (task_priority, updated_at)
    WHERE status = 'InProgress';

-- 2) Row-local history version counter. Replaces the
--    `(SELECT MAX(version)+1 FROM history WHERE instance_id=$1)` subquery in
--    save_snapshot, which races against the UNIQUE(instance_id, version)
--    constraint under concurrent writers on the same instance.
ALTER TABLE sayiir_workflow_snapshots
    ADD COLUMN IF NOT EXISTS history_version INT NOT NULL DEFAULT 0;
