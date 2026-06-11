-- Dispatch indexes.
--
-- find_available_tasks now fetches two LIMIT-bounded, index-ordered arms
-- (best-priority and oldest-first) instead of sorting every InProgress row
-- by the non-SARGable aging expression. Arm 1 is served by
-- idx_snapshots_inprogress (008); arm 2 needs an updated_at order.
CREATE INDEX IF NOT EXISTS idx_snapshots_inprogress_updated
    ON sayiir_workflow_snapshots (updated_at)
    WHERE status = 'InProgress';
