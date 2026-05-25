-- Index cleanup + heap-page tuning. Pure performance migration; no
-- behaviour change. Identified via a static audit of every read path in
-- src/: the dropped indexes are not referenced by any query, and were
-- either superseded by the partial idx_snapshots_inprogress (008) or
-- left over from the 001 schema before the PK on instance_id made
-- single-column secondary indexes redundant.
--
-- Operational note for large existing fleets: the GIN rebuild below
-- holds an AccessExclusiveLock on sayiir_workflow_snapshots while it
-- runs. To avoid blocking writes during the migration window,
-- pre-create the replacement index before deploying:
--
--   CREATE INDEX CONCURRENTLY idx_sayiir_task_tags_new
--       ON sayiir_workflow_snapshots USING GIN (task_tags)
--       WHERE status = 'InProgress';
--   ALTER INDEX idx_sayiir_task_tags RENAME TO idx_sayiir_task_tags_old;
--   ALTER INDEX idx_sayiir_task_tags_new RENAME TO idx_sayiir_task_tags;
--   DROP INDEX idx_sayiir_task_tags_old;
--
-- and the GIN-rebuild block below is then a no-op via IF EXISTS guards.

-- sayiir_workflow_snapshots --------------------------------------------------
-- Every save_snapshot UPDATE writes this row. Each surviving secondary
-- index on it is amortised across all writes to the table.
--   idx_snapshots_status: superseded by partial idx_snapshots_inprogress.
--   idx_snapshots_task:   redundant with the PK; every read-by-task
--                         path already binds instance_id = $1.
--   idx_snapshots_updated: updated_at moves on every save — maximum
--                         write churn for no read benefit (the polling
--                         ORDER BY is a non-SARGable expression).
--   idx_snapshots_position: low cardinality, redundant with the
--                         InProgress-partial pre-filter.
DROP INDEX IF EXISTS idx_snapshots_status;
DROP INDEX IF EXISTS idx_snapshots_task;
DROP INDEX IF EXISTS idx_snapshots_updated;
DROP INDEX IF EXISTS idx_snapshots_position;

-- Make task_tags GIN partial. Terminal rows are never matched by
-- find_available_tasks' tag filter, but the old non-partial GIN
-- indexed them anyway — pure write amplification for the lifetime of
-- the workflow's row.
--
-- This block is idempotent against the pre-deploy recipe in the
-- header: if the existing `idx_sayiir_task_tags` is already the
-- partial shape we want, both arms are skipped and no
-- AccessExclusiveLock is taken. Otherwise we drop the non-partial
-- and build the partial inline, which DOES briefly lock the table.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_indexes
        WHERE schemaname = current_schema()
          AND tablename  = 'sayiir_workflow_snapshots'
          AND indexname  = 'idx_sayiir_task_tags'
          AND indexdef ILIKE '%WHERE (status = ''InProgress''::text)%'
    ) THEN
        -- Already the partial shape; no-op.
        NULL;
    ELSE
        EXECUTE 'DROP INDEX IF EXISTS idx_sayiir_task_tags';
        EXECUTE $ddl$
            CREATE INDEX idx_sayiir_task_tags
                ON sayiir_workflow_snapshots USING GIN (task_tags)
                WHERE status = 'InProgress'
        $ddl$;
    END IF;
END $$;

-- Leave 20% free space on each heap page so future row-version writes
-- have somewhere to land in-page. Reduces page splits on updates that
-- grow the row (e.g. task_tags TEXT[] gaining an entry, error becoming
-- non-NULL on failure). HOT eligibility still requires that no indexed
-- column change; with idx_snapshots_inprogress on (task_priority,
-- updated_at) those updates remain non-HOT — fillfactor here is about
-- reducing split frequency, not enabling HOT outright.
ALTER TABLE sayiir_workflow_snapshots SET (fillfactor = 80);

-- sayiir_workflow_tasks ------------------------------------------------------
-- idx_tasks_status: not referenced by any read path. fetch_task_outputs
-- and friends filter by (instance_id, status='completed'); the PK on
-- (instance_id, task_id) covers the instance_id lookup, and a small
-- post-filter on status is cheaper than maintaining a low-cardinality
-- secondary index on every save_task_result write.
DROP INDEX IF EXISTS idx_tasks_status;

-- sayiir_workflow_claims (formerly sayiir_task_claims) -----------------------
-- The eligibility predicate in find_available_tasks / find_hinted_task is
-- `NOT EXISTS (SELECT 1 FROM claims c WHERE c.instance_id = s.instance_id
--              AND (c.expires_at IS NULL OR c.expires_at > now()))` —
-- always a PK lookup (the PK is on instance_id since migration 008).
-- Neither expires_at nor worker_id is the driver of any query in src/.
-- These rename-survivors from migration 001 are unused.
DROP INDEX IF EXISTS idx_claims_expires;
DROP INDEX IF EXISTS idx_claims_worker;
