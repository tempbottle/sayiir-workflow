//! Shared plumbing for paths that mutate the snapshot blob.
//!
//! `sayiir_workflow_snapshot_history` is the canonical store of the
//! encoded snapshot bytes; the `sayiir_workflow_snapshots` table only
//! holds metadata and a `history_version` pointer into history. Any
//! path that mutates the blob (save_snapshot, save_task_result,
//! check_and_cancel, check_and_pause, unpause) must:
//!
//! 1. Lock the snapshot row and read the current blob via
//!    [`lock_snapshot_for_mutation`].
//! 2. Mutate in memory and re-encode.
//! 3. Append a new history row with the bumped version through
//!    [`append_history`].
//! 4. Update snapshot metadata (status, history_version, position
//!    kind, etc.) without touching `data`.
//!
//! The helpers in this module own steps 1 and 3 so the SQL stays in
//! one place — every site that mutates the blob writes the same
//! history shape, including the `data_hash` (SHA-256 of the encoded
//! blob) that paves the way for a future KV/object-store offload.

use std::collections::HashMap;

use bytes::Bytes;
use sayiir_core::codec::{self, Decoder};
use sayiir_core::snapshot::WorkflowSnapshot;
use sayiir_persistence::BackendError;
use sha2::{Digest, Sha256};
use sqlx::{PgConnection, Row};

use crate::backend::PostgresBackend;
use crate::error::PgError;

/// Fetch the per-task `output` blobs from `sayiir_workflow_tasks` for one
/// instance and return them keyed by `TaskId`. Snapshot blobs no longer
/// carry the outputs (see [`WorkflowSnapshot::strip_task_outputs`]), so
/// every load path that hands a snapshot to the runtime calls this and
/// then [`WorkflowSnapshot::hydrate_task_outputs`].
pub(crate) async fn fetch_task_outputs<'e, E>(
    executor: E,
    instance_id: &str,
) -> Result<HashMap<sayiir_core::TaskId, Bytes>, BackendError>
where
    E: sqlx::PgExecutor<'e>,
{
    let rows = sqlx::query(
        "SELECT task_id, output FROM sayiir_workflow_tasks
         WHERE instance_id = $1 AND status = 'completed' AND output IS NOT NULL",
    )
    .bind(instance_id)
    .fetch_all(executor)
    .await
    .map_err(PgError)?;

    let mut outputs = HashMap::with_capacity(rows.len());
    for row in rows {
        let task_id_bytes: &[u8] = row.get("task_id");
        let Ok(task_id) = sayiir_core::TaskId::from_slice(task_id_bytes) else {
            continue;
        };
        let output: Vec<u8> = row.get("output");
        outputs.insert(task_id, Bytes::from(output));
    }
    Ok(outputs)
}

/// Batched variant of [`fetch_task_outputs`] for dispatch SELECTs that
/// load multiple snapshots at once. Returns rows grouped by
/// `instance_id` so each snapshot can be hydrated independently.
pub(crate) async fn fetch_task_outputs_batched<'e, E>(
    executor: E,
    instance_ids: &[&str],
) -> Result<HashMap<String, HashMap<sayiir_core::TaskId, Bytes>>, BackendError>
where
    E: sqlx::PgExecutor<'e>,
{
    if instance_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let rows = sqlx::query(
        "SELECT instance_id, task_id, output FROM sayiir_workflow_tasks
         WHERE instance_id = ANY($1) AND status = 'completed' AND output IS NOT NULL",
    )
    .bind(instance_ids)
    .fetch_all(executor)
    .await
    .map_err(PgError)?;

    let mut grouped: HashMap<String, HashMap<sayiir_core::TaskId, Bytes>> = HashMap::new();
    for row in rows {
        let instance_id: String = row.get("instance_id");
        let task_id_bytes: &[u8] = row.get("task_id");
        let Ok(task_id) = sayiir_core::TaskId::from_slice(task_id_bytes) else {
            continue;
        };
        let output: Vec<u8> = row.get("output");
        grouped
            .entry(instance_id)
            .or_default()
            .insert(task_id, Bytes::from(output));
    }
    Ok(grouped)
}

/// SHA-256 of the encoded snapshot bytes. Persisted on every history
/// row so a future migration can move blob storage to a KV/object
/// store keyed by hash without changing the on-disk row shape further.
pub(crate) fn snapshot_hash(data: &[u8]) -> [u8; 32] {
    Sha256::digest(data).into()
}

impl<C> PostgresBackend<C>
where
    C: Decoder + codec::sealed::DecodeValue<WorkflowSnapshot>,
{
    /// Single point of indirection for snapshot blob reads.
    ///
    /// Today this is a PK lookup against `sayiir_workflow_snapshot_history`.
    /// When blob storage moves to a KV / object store, this method becomes
    /// `SELECT data_hash → blob_store.get(hash)` (with a legacy fallback to
    /// `data` for rows from before that cutover) and every caller picks up
    /// the change without further edits. Use this instead of inlining the
    /// SQL anywhere a blob is fetched by `(instance_id, version)`.
    pub(crate) async fn fetch_blob<'e, E>(
        &self,
        executor: E,
        instance_id: &str,
        history_version: i32,
    ) -> Result<Vec<u8>, BackendError>
    where
        E: sqlx::PgExecutor<'e>,
    {
        let row = sqlx::query(
            "SELECT data FROM sayiir_workflow_snapshot_history
             WHERE instance_id = $1 AND version = $2",
        )
        .bind(instance_id)
        .bind(history_version)
        .fetch_one(executor)
        .await
        .map_err(PgError)?;
        Ok(row.get("data"))
    }

    /// Lock the snapshot row for mutation and load the latest blob.
    ///
    /// Used by every path that does read-modify-write on the snapshot:
    /// `FOR UPDATE OF s` serialises concurrent writers on the same
    /// instance (two racing completions would otherwise lose one
    /// mutation). The JOIN to `sayiir_workflow_snapshot_history` is
    /// where the blob actually lives now — the snapshot row only
    /// stores a pointer (`history_version`).
    ///
    /// Returns `Ok(None)` if the instance does not exist; callers
    /// decide whether that's an error.
    pub(crate) async fn lock_snapshot_for_mutation(
        &self,
        tx: &mut PgConnection,
        instance_id: &str,
    ) -> Result<Option<(WorkflowSnapshot, i32)>, BackendError> {
        // Statement 1 — take FOR UPDATE on s alone.
        //
        // IMPORTANT: this MUST be its own statement, not folded into a
        // CTE with the blob+outputs read below.
        //
        // Under READ COMMITTED, `SELECT s.history_version, h.data FROM s
        // JOIN h ... WHERE s.instance_id = $1 FOR UPDATE OF s` interacts
        // badly with concurrent writers. PG's EvalPlanQual re-evaluates
        // the locked s row's WHERE clause when the wait releases, but
        // does NOT re-run the JOIN against the updated s row — the JOIN
        // result is fixed at scan time against the *original* snapshot.
        // If a concurrent transaction bumps `s.history_version` and
        // inserts the matching `h.version` row while we're waiting on
        // the FOR UPDATE, our re-checked s row has the new
        // `history_version` but the JOIN was paired with the OLD
        // `h.version`, so PG considers the row "no longer matching"
        // and returns 0 rows. The caller misreads this as
        // `NotFound` and the workflow goes Failed even though the row
        // is right there in the DB. Repro:
        // `multi_stage_scatter_gather` with 4 parallel branches racing
        // `save_task_result`.
        let row = sqlx::query(
            "SELECT history_version FROM sayiir_workflow_snapshots
             WHERE instance_id = $1
             FOR UPDATE",
        )
        .bind(instance_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(PgError)?;

        let Some(row) = row else { return Ok(None) };
        let history_version: i32 = row.get("history_version");

        // Statement 2 — blob + outputs in one round-trip.
        //
        // The lock from statement 1 is still held in this same tx, so
        // both reads observe the version we just locked. We could
        // expand to fetch via correlated subqueries; using a single
        // CTE for outputs avoids scanning sayiir_workflow_tasks twice.
        let row = sqlx::query(
            "WITH outputs AS (
                 SELECT task_id, output FROM sayiir_workflow_tasks
                 WHERE instance_id = $1
                   AND status = 'completed'
                   AND output IS NOT NULL
             )
             SELECT
                 (SELECT data FROM sayiir_workflow_snapshot_history
                  WHERE instance_id = $1 AND version = $2) AS data,
                 COALESCE((SELECT array_agg(task_id) FROM outputs),
                          ARRAY[]::bytea[]) AS task_ids,
                 COALESCE((SELECT array_agg(output) FROM outputs),
                          ARRAY[]::bytea[]) AS outputs",
        )
        .bind(instance_id)
        .bind(history_version)
        .fetch_one(&mut *tx)
        .await
        .map_err(PgError)?;

        let data: Vec<u8> = row.get("data");
        let mut snapshot = self.decode(&data)?;
        let task_ids: Vec<Vec<u8>> = row.get("task_ids");
        let outputs_vec: Vec<Vec<u8>> = row.get("outputs");
        let mut outputs = std::collections::HashMap::with_capacity(task_ids.len());
        for (tid_bytes, output) in task_ids.into_iter().zip(outputs_vec.into_iter()) {
            let Ok(task_id) = sayiir_core::TaskId::from_slice(&tid_bytes) else {
                continue;
            };
            outputs.insert(task_id, Bytes::from(output));
        }
        snapshot.hydrate_task_outputs(outputs);
        Ok(Some((snapshot, history_version)))
    }
}

/// Append a row to `sayiir_workflow_snapshot_history`. Caller must
/// advance `history_version` AND `data_hash` on the snapshot row in
/// the same transaction so the two stay in lockstep. `data_hash` is
/// passed in (not recomputed) so the caller can bind the same value
/// into the snapshot-row UPDATE.
pub(crate) async fn append_history(
    tx: &mut PgConnection,
    instance_id: &str,
    version: i32,
    status: &str,
    current_task_id: Option<&[u8]>,
    data: &[u8],
    data_hash: &[u8; 32],
) -> Result<(), BackendError> {
    sqlx::query(
        "INSERT INTO sayiir_workflow_snapshot_history
            (instance_id, version, status, current_task_id, data, data_hash)
         VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind(instance_id)
    .bind(version)
    .bind(status)
    .bind(current_task_id)
    .bind(data)
    .bind(data_hash.as_slice())
    .execute(&mut *tx)
    .await
    .map_err(PgError)?;
    Ok(())
}
