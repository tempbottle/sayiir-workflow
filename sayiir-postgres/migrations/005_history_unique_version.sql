-- Replace the non-unique index with a UNIQUE constraint on (instance_id, version)
-- to guarantee monotonic history even without the row-lock held.
DROP INDEX IF EXISTS idx_history_instance;
CREATE UNIQUE INDEX idx_history_instance ON sayiir_workflow_snapshot_history (instance_id, version);
