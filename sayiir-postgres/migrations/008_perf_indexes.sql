-- 1) Partial index for the polling hot path.
--
--    Purpose is **selectivity only**, not sort. The existing
--    `idx_snapshots_status` indexes every status value, so it bloats with
--    terminal rows and loses selectivity for `status = 'InProgress'`. This
--    partial index covers only the live working-set rows; the planner uses
--    it to satisfy the `WHERE status = 'InProgress'` predicate without
--    touching terminal rows.
--
--    The polling SELECT's `ORDER BY (task_priority - EXTRACT(EPOCH FROM
--    (now() - updated_at)) / $2) ASC` is a non-indexable expression, so the
--    planner cannot use this index to satisfy the ordering and will sort
--    the filtered rows separately. The (task_priority, updated_at) column
--    list is therefore not load-bearing for sort and chosen mainly so the
--    index entries traverse in a useful order for any future queries that
--    do scan by priority/recency without aging.
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
--
--    DEFAULT 1 is the value the save_snapshot INSERT path relies on for new
--    instances — the INSERT omits `history_version` entirely so this column
--    default becomes the single source of truth for "first version is 1".
--    The ON CONFLICT branch then does `history_version + 1`.
ALTER TABLE sayiir_workflow_snapshots
    ADD COLUMN IF NOT EXISTS history_version INT NOT NULL DEFAULT 1;

-- Backfill the counter for any pre-existing snapshot so the next
-- save_snapshot continues numbering from the current MAX rather than
-- restarting at 1 (which would collide with the existing history rows).
-- COALESCE to 0 for the (degenerate) case of a snapshot row with no history,
-- so the next save assigns version 1 cleanly.
UPDATE sayiir_workflow_snapshots s
SET history_version = COALESCE(
    (SELECT MAX(version) FROM sayiir_workflow_snapshot_history h
     WHERE h.instance_id = s.instance_id),
    0
);
