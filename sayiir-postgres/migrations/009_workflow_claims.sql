-- Claim ownership back into its own narrow table. One row per
-- in-flight workflow (only one task in flight per instance, so the
-- PK is just instance_id). Eligibility is a NOT EXISTS join on this
-- table; the table stays small and hot in shared_buffers.

CREATE TABLE IF NOT EXISTS sayiir_workflow_claims (
    instance_id TEXT        NOT NULL PRIMARY KEY,
    task_id     BYTEA       NOT NULL,
    worker_id   TEXT        NOT NULL,
    claimed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at  TIMESTAMPTZ
);

ALTER TABLE sayiir_workflow_snapshots
    DROP COLUMN IF EXISTS claim_owner,
    DROP COLUMN IF EXISTS claim_expires_at;
