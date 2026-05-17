//! Operator-facing one-shot migrations.
//!
//! Backfills exist to bring legacy rows into a shape that newly-deployed
//! code expects, without forcing a hard cutover at deploy time. They are
//! safe to re-run: each statement is scoped with `WHERE output IS NULL`
//! (or equivalent) so a partially-applied run picks up where it left off.

use sayiir_core::codec::{self, Decoder};
use sayiir_core::snapshot::WorkflowSnapshot;
use sayiir_persistence::BackendError;
use sqlx::Row;

use crate::backend::PostgresBackend;
use crate::error::PgError;
use crate::history::HISTORY_JOIN;

/// Result of a [`PostgresBackend::backfill_task_outputs`] run.
#[derive(Debug, Default, Clone, Copy)]
pub struct BackfillStats {
    /// Snapshots inspected by this run.
    pub snapshots_scanned: usize,
    /// Output rows written (only counts rows where `output IS NULL` was
    /// flipped to a value — re-running over already-backfilled rows
    /// reports zero).
    pub outputs_written: usize,
}

impl<C> PostgresBackend<C>
where
    C: Decoder + codec::sealed::DecodeValue<WorkflowSnapshot>,
{
    /// Copy task outputs from the snapshot blob's `completed_tasks` map
    /// into the `sayiir_workflow_tasks.output` column.
    ///
    /// Run this once after migration 009 lands and before relying on
    /// `workflow_tasks.output` as canonical (the cutover that drops the
    /// `completed_tasks` map from the snapshot blob). Without it,
    /// outputs produced before migration 009 deployed remain visible
    /// only through the blob, and read paths that have switched over
    /// will see them as missing.
    ///
    /// Scans snapshots in pages of `batch_size` to keep memory bounded
    /// even on large tables. Each output UPDATE is scoped with
    /// `WHERE output IS NULL` so re-running the call is safe and never
    /// clobbers a value the live dual-write put there.
    ///
    /// Only non-terminal snapshots are inspected — terminal states
    /// (`Completed`/`Failed`) discard `completed_tasks` from the blob,
    /// so there is nothing to copy. If the legacy task-result fallback
    /// in `load_task_result` is being relied on, recover those values
    /// separately from `sayiir_workflow_snapshot_history`.
    ///
    /// # Errors
    ///
    /// Returns the first DB or codec error encountered. State written
    /// before the error remains visible — the call is restartable.
    pub async fn backfill_task_outputs(
        &self,
        batch_size: usize,
    ) -> Result<BackfillStats, BackendError> {
        let mut stats = BackfillStats::default();
        let batch_size = batch_size.max(1);
        let mut cursor: Option<String> = None;

        let scan_query = format!(
            "SELECT s.instance_id, h.data
             FROM sayiir_workflow_snapshots s
             {HISTORY_JOIN}
             WHERE s.status NOT IN ('Completed', 'Failed')
               AND ($1::TEXT IS NULL OR s.instance_id > $1)
             ORDER BY s.instance_id
             LIMIT $2",
        );

        loop {
            let rows = sqlx::query(&scan_query)
                .bind(cursor.as_deref())
                .bind(i64::try_from(batch_size).unwrap_or(i64::MAX))
                .fetch_all(&self.pool)
                .await
                .map_err(PgError)?;

            if rows.is_empty() {
                break;
            }

            for row in &rows {
                let instance_id: String = row.get("instance_id");
                let raw: &[u8] = row.get("data");
                let snapshot = self.decode(raw)?;
                stats.snapshots_scanned += 1;

                let Some(results) = snapshot.get_all_task_results() else {
                    continue;
                };
                if results.is_empty() {
                    cursor = Some(instance_id);
                    continue;
                }

                // Batch all of this snapshot's outputs into one UPDATE
                // via parallel arrays — K task_ids and K output blobs
                // per instance. With a 100-task workflow this collapses
                // 100 round-trips into 1.
                let mut task_ids: Vec<&str> = Vec::with_capacity(results.len());
                let mut outputs: Vec<&[u8]> = Vec::with_capacity(results.len());
                for (task_id, result) in results {
                    task_ids.push(task_id.as_str());
                    outputs.push(result.output.as_ref());
                }

                let res = sqlx::query(
                    "UPDATE sayiir_workflow_tasks t
                     SET output = u.output
                     FROM UNNEST($2::TEXT[], $3::BYTEA[]) AS u(task_id, output)
                     WHERE t.instance_id = $1
                       AND t.task_id = u.task_id
                       AND t.output IS NULL",
                )
                .bind(&instance_id)
                .bind(&task_ids)
                .bind(&outputs)
                .execute(&self.pool)
                .await
                .map_err(PgError)?;

                stats.outputs_written += usize::try_from(res.rows_affected()).unwrap_or(0);
                cursor = Some(instance_id);
            }
        }

        Ok(stats)
    }
}
