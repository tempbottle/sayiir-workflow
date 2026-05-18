-- Task output storage, decoupled from the snapshot blob.
--
-- Today every task completion grows `completed_tasks` inside the snapshot's
-- `data` BYTEA, which is rewritten in full on every save — accumulated
-- outputs make late-stage saves quadratic in WAL volume over a workflow's
-- lifetime. Outputs live here from now on, indexed by (instance_id, task_id)
-- exactly like the other denormalised metadata columns on this table.
--
-- Nullable on purpose: the existing `INSERT … ON CONFLICT` in save_snapshot
-- creates a row at task start (status='active') with no output yet. The
-- output is filled in by save_task_result. Rows from workflows that
-- completed tasks before this migration stay NULL — no backfill ships;
-- pre-migration outputs remain reachable only through the snapshot's
-- in-blob `completed_tasks` map.
ALTER TABLE sayiir_workflow_tasks
    ADD COLUMN IF NOT EXISTS output BYTEA;
