-- Mirror history.data_hash onto the snapshot row so the KV cutover can
-- fetch the current blob with one SQL probe + one KV.get, skipping the
-- history JOIN. Maintained in lockstep with history.data_hash by every
-- mutation path; pre-migration rows stay NULL.

ALTER TABLE sayiir_workflow_snapshots
    ADD COLUMN IF NOT EXISTS data_hash BYTEA;

COMMENT ON COLUMN sayiir_workflow_snapshots.data_hash IS
    'WIP: SHA-256 of the current snapshot blob. Mirrors history.data_hash at s.history_version.';
