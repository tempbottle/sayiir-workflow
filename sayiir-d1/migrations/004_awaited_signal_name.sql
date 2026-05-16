-- Denormalize the signal name a workflow is currently waiting on, so the
-- `resumeAll` "signalled" pickup branch can join against
-- `sayiir_workflow_events` by (instance_id, signal_name).
--
-- Before this column existed, the pickup query matched any AtSignal
-- snapshot that had ANY buffered event for the instance — including
-- events from previous signal waits or unrelated signals delivered
-- early. The runtime would re-resume the workflow every cron tick,
-- find no matching event, and park again indefinitely.
ALTER TABLE sayiir_workflow_snapshots
    ADD COLUMN awaited_signal_name TEXT;

CREATE INDEX IF NOT EXISTS idx_snapshots_awaited_signal
    ON sayiir_workflow_snapshots (awaited_signal_name)
    WHERE awaited_signal_name IS NOT NULL;
