-- Make `sayiir_workflow_snapshot_history` the canonical store of the
-- encoded snapshot blob, and pave the way for offloading blob storage to a
-- KV/object store later.
--
-- Today the same blob is written into both `sayiir_workflow_snapshots.data`
-- and `sayiir_workflow_snapshot_history.data` on every save — two TOAST
-- entries per save, and the snapshots-side write creates a dead tuple
-- because the column changes on every UPDATE. New writes stop populating
-- `snapshots.data`; reads source the blob from history via
-- `s.history_version → h.version` (a pointer that has existed since
-- migration 008). The column drops NOT NULL so new rows can leave it
-- unset; pre-migration rows are not migrated.
--
-- ┌─ BREAKING CHANGE — no backward compat for pre-migration data ─────┐
-- │                                                                    │
-- │ Snapshots and history rows written before this migration are not   │
-- │ supported by the new code paths:                                   │
-- │   • `snapshots.data` keeps its old value but is never read.        │
-- │   • `history.data_hash` is NULL for old rows; the future KV        │
-- │     cutover cannot resolve them by hash.                           │
-- │ Workflows must be drained / restarted across this migration.       │
-- │                                                                    │
-- │ `data_hash` is WIP: the algorithm (SHA-256) and column type may    │
-- │ change before the KV migration lands. Treat the contract as        │
-- │ unstable until that track ships.                                   │
-- │                                                                    │
-- └────────────────────────────────────────────────────────────────────┘

ALTER TABLE sayiir_workflow_snapshots
    ALTER COLUMN data DROP NOT NULL;

ALTER TABLE sayiir_workflow_snapshot_history
    ADD COLUMN IF NOT EXISTS data_hash BYTEA;

COMMENT ON COLUMN sayiir_workflow_snapshot_history.data_hash IS
    'WIP: SHA-256 of the encoded blob. Pre-migration-011 rows are NULL and unsupported by the future KV offload.';
