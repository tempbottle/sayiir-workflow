-- Add task tags column for informational purposes.
-- Tags are stored in the serialised snapshot blob; this column is
-- provided for observability queries only.
ALTER TABLE sayiir_workflow_snapshots
ADD COLUMN task_tags TEXT NOT NULL DEFAULT '[]';
