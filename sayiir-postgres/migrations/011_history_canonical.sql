-- Make `sayiir_workflow_snapshot_history` the canonical store of the
-- encoded snapshot blob, and pave the way for offloading blob storage to a
-- KV/object store later.
--
-- Today the same blob is written into both `sayiir_workflow_snapshots.data`
-- and `sayiir_workflow_snapshot_history.data` on every save — two TOAST
-- entries per save, and the snapshots-side write creates a dead tuple
-- because the column changes on every UPDATE. This migration enables the
-- shift: new writes stop populating `snapshots.data`, reads JOIN history
-- via `s.history_version → h.version` (a pointer that has existed since
-- migration 008), and the column stays around as a no-op for old rows so
-- the change is reversible without a backfill.
--
-- ┌─ BREAKING CHANGE for future KV/object-store migration ─────────────┐
-- │                                                                    │
-- │ The `data_hash` column is the content-addressing primitive for the │
-- │ planned blob-storage offload. Rows written **before** this         │
-- │ migration have `data_hash = NULL` and are NOT backfilled — the     │
-- │ engine ignores them on every read path today, so old data stays    │
-- │ visible through `data`, but the KV cutover will not be able to     │
-- │ resolve those rows by hash. At that point operators have two       │
-- │ options:                                                           │
-- │   1. Re-hash old rows offline (decode each `data` → SHA-256 →      │
-- │      UPDATE … WHERE data_hash IS NULL), then drop `data`.          │
-- │   2. Accept that history rows older than migration 010 become      │
-- │      unreadable post-cutover (in-flight workflows continue to      │
-- │      work because the last history row written by the new code     │
-- │      always carries a hash).                                       │
-- │                                                                    │
-- │ `data_hash` is also WIP: the algorithm (SHA-256) and column type   │
-- │ (BYTEA) may change before the KV migration lands. Treat the        │
-- │ contract as unstable until the KV track ships.                     │
-- │                                                                    │
-- └────────────────────────────────────────────────────────────────────┘

ALTER TABLE sayiir_workflow_snapshots
    ALTER COLUMN data DROP NOT NULL;

ALTER TABLE sayiir_workflow_snapshot_history
    ADD COLUMN IF NOT EXISTS data_hash BYTEA;

COMMENT ON COLUMN sayiir_workflow_snapshot_history.data_hash IS
    'WIP: SHA-256 of the encoded blob. Pre-migration-011 rows are NULL and unsupported by the future KV offload — see migration 011 header for details.';
