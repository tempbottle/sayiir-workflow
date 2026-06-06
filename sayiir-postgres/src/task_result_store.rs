//! [`TaskResultStore`] implementation for Postgres.
//!
//! For non-terminal workflows, task results come from the current snapshot.
//! For completed or failed workflows (where `completed_tasks` has been
//! discarded), the implementation falls back to the most recent `InProgress`
//! entry in `sayiir_workflow_snapshot_history`.

use sayiir_core::codec::{self, Decoder};
use sayiir_core::snapshot::WorkflowSnapshot;
use sayiir_persistence::{BackendError, SnapshotStore, TaskResultStore};
use sqlx::Row;

use crate::backend::PostgresBackend;
use crate::error::PgError;

impl<C> TaskResultStore for PostgresBackend<C>
where
    C: codec::SnapshotCodec,
{
    #[tracing::instrument(
        name = "db.load_task_result",
        skip(self),
        fields(db.system = "postgresql"),
        err(level = tracing::Level::ERROR),
    )]
    async fn load_task_result(
        &self,
        instance_id: &str,
        task_id: &sayiir_core::TaskId,
    ) -> Result<Option<bytes::Bytes>, BackendError> {
        let snapshot = self.load_snapshot(instance_id).await?;

        // Non-terminal states carry completed_tasks directly.
        if let Some(bytes) = snapshot.get_task_result_bytes(task_id) {
            return Ok(Some(bytes));
        }

        // For terminal states, fall back to snapshot history.
        if (snapshot.state.is_completed() || snapshot.state.is_failed())
            && let Some(hist) = self.load_last_in_progress_snapshot(instance_id).await?
        {
            return Ok(hist.get_task_result_bytes(task_id));
        }

        Ok(None)
    }
}

impl<C> PostgresBackend<C>
where
    C: Decoder + codec::CodecIdentity + codec::sealed::DecodeValue<WorkflowSnapshot>,
{
    /// Load the most recent `InProgress` snapshot from the history table.
    ///
    /// This is the last snapshot before the workflow transitioned to a terminal
    /// state, so it still contains `completed_tasks` — but the on-disk blob
    /// is outputs-stripped, so hydrate per-task outputs from
    /// `sayiir_workflow_tasks` before returning.
    async fn load_last_in_progress_snapshot(
        &self,
        instance_id: &str,
    ) -> Result<Option<WorkflowSnapshot>, BackendError> {
        let row = sqlx::query(
            "SELECT data FROM sayiir_workflow_snapshot_history
             WHERE instance_id = $1 AND status = 'InProgress'
             ORDER BY version DESC LIMIT 1",
        )
        .bind(instance_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(PgError)?;

        match row {
            Some(r) => {
                let raw: &[u8] = r.get("data");
                let mut snapshot = self.decode(raw)?;
                let outputs = crate::history::fetch_task_outputs(&self.pool, instance_id).await?;
                snapshot.hydrate_task_outputs(outputs);
                Ok(Some(snapshot))
            }
            None => Ok(None),
        }
    }
}
