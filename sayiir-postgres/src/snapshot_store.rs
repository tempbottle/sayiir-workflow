//! [`SnapshotStore`] implementation for Postgres.

use sayiir_core::codec::{self, Decoder, Encoder};
use sayiir_core::snapshot::{SnapshotStatus, WorkflowSnapshot};
use sayiir_persistence::{BackendError, SnapshotStore};
use sqlx::Row;

use crate::backend::PostgresBackend;
use crate::error::PgError;
use crate::history::{append_history, snapshot_hash};
use crate::wakeup::emit_task_ready;

impl<C> SnapshotStore for PostgresBackend<C>
where
    C: Encoder
        + Decoder
        + codec::sealed::EncodeValue<WorkflowSnapshot>
        + codec::sealed::DecodeValue<WorkflowSnapshot>,
{
    #[tracing::instrument(
        name = "db.save_snapshot",
        skip(self, snapshot),
        fields(
            db.system = "postgresql",
            instance_id = %snapshot.instance_id,
            status = %snapshot.state.as_ref(),
        ),
        err(level = tracing::Level::ERROR),
    )]
    #[allow(clippy::too_many_lines)]
    async fn save_snapshot(&self, snapshot: &WorkflowSnapshot) -> Result<(), BackendError> {
        tracing::debug!("saving snapshot");
        let data = self.encode(snapshot)?;
        let data_hash = snapshot_hash(&data);
        let status = snapshot.state.as_ref();
        let task_id_bytes: Option<[u8; 32]> = snapshot.current_task_id().map(|t| *t.as_bytes());
        let task_id: Option<&[u8]> = task_id_bytes.as_ref().map(<[u8; 32]>::as_slice);
        let task_count = snapshot.completed_task_count();
        let error = snapshot.error_message().map(ToString::to_string);
        let terminal = snapshot.state.is_terminal();
        let pos_kind = snapshot.position_kind();
        let wake_at = snapshot.delay_wake_at();

        let task_priority = i16::from(snapshot.current_task_priority());
        let task_tags: Vec<&str> = snapshot
            .current_task_tags()
            .iter()
            .map(String::as_str)
            .collect();

        let mut tx = self.pool.begin().await.map_err(PgError)?;

        // `history_version` is omitted from the column list so the column
        // DEFAULT supplies version 1 on INSERT; the ON CONFLICT branch
        // increments from the locked existing value, then RETURNING hands
        // the chosen version to the history insert below.
        let upsert_row = sqlx::query(
            "INSERT INTO sayiir_workflow_snapshots
                (instance_id, status, definition_hash, current_task_id,
                 completed_task_count, error, position_kind, delay_wake_at,
                 trace_parent, task_priority, task_tags, data_hash,
                 completed_at, updated_at)
             VALUES ($1, $2, $3, $4, $5, $6, $8, $9, $10, $11, $12, $13,
                     CASE WHEN $7 THEN now() ELSE NULL END, now())
             ON CONFLICT (instance_id) DO UPDATE SET
                status = $2,
                definition_hash = $3,
                current_task_id = $4,
                completed_task_count = $5,
                error = $6,
                position_kind = $8,
                delay_wake_at = $9,
                trace_parent = $10,
                task_priority = $11,
                task_tags = $12,
                data_hash = $13,
                history_version = sayiir_workflow_snapshots.history_version + 1,
                completed_at = CASE WHEN $7 THEN now() ELSE sayiir_workflow_snapshots.completed_at END,
                updated_at = now()
             RETURNING history_version",
        )
        .bind(&*snapshot.instance_id) // $1
        .bind(status) // $2
        .bind(snapshot.definition_hash.as_bytes().as_slice()) // $3
        .bind(task_id) // $4
        .bind(task_count) // $5
        .bind(&error) // $6
        .bind(terminal) // $7
        .bind(pos_kind) // $8
        .bind(wake_at) // $9
        .bind(snapshot.trace_parent.as_deref()) // $10
        .bind(task_priority) // $11
        .bind(&task_tags) // $12
        .bind(data_hash.as_slice()) // $13
        .fetch_one(&mut *tx)
        .await
        .map_err(PgError)?;

        let history_version: i32 = upsert_row.get("history_version");

        append_history(
            &mut tx,
            &snapshot.instance_id,
            history_version,
            status,
            task_id,
            &data,
            &data_hash,
        )
        .await?;

        // --- Maintain sayiir_workflow_tasks lifecycle ---

        // If at a task, mark it as active
        if let Some(tid) = task_id {
            sqlx::query(
                "INSERT INTO sayiir_workflow_tasks (instance_id, task_id, status, started_at)
                 VALUES ($1, $2, 'active', now())
                 ON CONFLICT (instance_id, task_id) DO UPDATE SET
                    status = CASE
                        WHEN sayiir_workflow_tasks.status = 'completed' THEN sayiir_workflow_tasks.status
                        ELSE 'active'
                    END,
                    started_at = COALESCE(sayiir_workflow_tasks.started_at, now())",
            )
            .bind(&*snapshot.instance_id)
            .bind(tid)
            .execute(&mut *tx)
            .await
            .map_err(PgError)?;
        }

        // On terminal states, mark any still-active task as failed/cancelled
        if terminal {
            let terminal_status = match SnapshotStatus::from(&snapshot.state) {
                SnapshotStatus::Failed => "failed",
                SnapshotStatus::Cancelled => "cancelled",
                _ => "completed",
            };
            sqlx::query(
                "UPDATE sayiir_workflow_tasks SET status = $1, completed_at = now(), error = $2
                 WHERE instance_id = $3 AND status = 'active'",
            )
            .bind(terminal_status)
            .bind(&error)
            .bind(&*snapshot.instance_id)
            .execute(&mut *tx)
            .await
            .map_err(PgError)?;
        }

        emit_task_ready(&mut tx, snapshot).await.map_err(PgError)?;

        tx.commit().await.map_err(PgError)?;
        Ok(())
    }

    #[tracing::instrument(
        name = "db.save_task_result",
        skip(self, output),
        fields(db.system = "postgresql"),
        err(level = tracing::Level::ERROR),
    )]
    async fn save_task_result(
        &self,
        instance_id: &str,
        task_id: &sayiir_core::TaskId,
        output: bytes::Bytes,
    ) -> Result<(), BackendError> {
        tracing::debug!("saving task result");
        let mut tx = self.pool.begin().await.map_err(PgError)?;

        // Lock the snapshot row and read the current blob from history.
        // `FOR UPDATE OF s` serialises concurrent task completions on the
        // same instance — two racing read-modify-writes would otherwise
        // lose one completion.
        let (mut snapshot, prev_history_version) = self
            .lock_snapshot_for_mutation(&mut tx, instance_id)
            .await?
            .ok_or_else(|| BackendError::NotFound(instance_id.to_string()))?;

        // `output` is consumed by mark_task_completed; the Arc clone keeps
        // a refcount alive for the workflow_tasks UPSERT below.
        let output_bytes = output.clone();
        snapshot.mark_task_completed(*task_id, output);

        let data = self.encode(&snapshot)?;
        let data_hash = snapshot_hash(&data);
        let status = snapshot.state.as_ref();
        let current_bytes: Option<[u8; 32]> = snapshot.current_task_id().map(|t| *t.as_bytes());
        let current: Option<&[u8]> = current_bytes.as_ref().map(<[u8; 32]>::as_slice);
        let task_count = snapshot.completed_task_count();
        let next_history_version = prev_history_version + 1;

        sqlx::query(
            "UPDATE sayiir_workflow_snapshots
             SET status = $1, current_task_id = $2,
                 completed_task_count = $3, history_version = $4,
                 data_hash = $5, updated_at = now()
             WHERE instance_id = $6",
        )
        .bind(status)
        .bind(current)
        .bind(task_count)
        .bind(next_history_version)
        .bind(data_hash.as_slice())
        .bind(instance_id)
        .execute(&mut *tx)
        .await
        .map_err(PgError)?;

        append_history(
            &mut tx,
            instance_id,
            next_history_version,
            status,
            current,
            &data,
            &data_hash,
        )
        .await?;

        // Dual-write the output to workflow_tasks during the cutover
        // phase. The blob still carries `completed_tasks` for reads; once
        // that map is removed from the snapshot, this column becomes the
        // canonical source. Nullable because save_snapshot creates the
        // row earlier (status='active') before any output exists.
        sqlx::query(
            "INSERT INTO sayiir_workflow_tasks
                (instance_id, task_id, status, completed_at, output)
             VALUES ($1, $2, 'completed', now(), $3)
             ON CONFLICT (instance_id, task_id) DO UPDATE SET
                status = 'completed', completed_at = now(), error = NULL,
                output = EXCLUDED.output",
        )
        .bind(instance_id)
        .bind(task_id.as_bytes().as_slice())
        .bind(output_bytes.as_ref())
        .execute(&mut *tx)
        .await
        .map_err(PgError)?;

        emit_task_ready(&mut tx, &snapshot).await.map_err(PgError)?;

        tx.commit().await.map_err(PgError)?;
        Ok(())
    }

    #[tracing::instrument(
        name = "db.load_snapshot",
        skip(self),
        fields(db.system = "postgresql"),
        err(level = tracing::Level::ERROR),
    )]
    async fn load_snapshot(&self, instance_id: &str) -> Result<WorkflowSnapshot, BackendError> {
        tracing::debug!("loading snapshot");
        let row = sqlx::query(
            "SELECT history_version, trace_parent
             FROM sayiir_workflow_snapshots
             WHERE instance_id = $1",
        )
        .bind(instance_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(PgError)?
        .ok_or_else(|| BackendError::NotFound(instance_id.to_string()))?;

        let history_version: i32 = row.get("history_version");
        let data = self
            .fetch_blob(&self.pool, instance_id, history_version)
            .await?;
        let mut snapshot = self.decode(&data)?;
        snapshot.trace_parent = row.get("trace_parent");
        Ok(snapshot)
    }

    #[tracing::instrument(
        name = "db.delete_snapshot",
        skip(self),
        fields(db.system = "postgresql"),
        err(level = tracing::Level::ERROR),
    )]
    async fn delete_snapshot(&self, instance_id: &str) -> Result<(), BackendError> {
        tracing::debug!("deleting snapshot");

        let mut tx = self.pool.begin().await.map_err(PgError)?;

        for table in crate::WORKFLOW_CHILD_TABLES {
            sqlx::query(&format!("DELETE FROM {table} WHERE instance_id = $1"))
                .bind(instance_id)
                .execute(&mut *tx)
                .await
                .map_err(PgError)?;
        }

        let result = sqlx::query("DELETE FROM sayiir_workflow_snapshots WHERE instance_id = $1")
            .bind(instance_id)
            .execute(&mut *tx)
            .await
            .map_err(PgError)?;

        tx.commit().await.map_err(PgError)?;

        if result.rows_affected() == 0 {
            return Err(BackendError::NotFound(instance_id.to_string()));
        }
        Ok(())
    }

    #[tracing::instrument(
        name = "db.list_snapshots",
        skip(self),
        fields(db.system = "postgresql"),
        err(level = tracing::Level::ERROR),
    )]
    async fn list_snapshots(&self) -> Result<Vec<String>, BackendError> {
        tracing::debug!("listing snapshots");
        let rows = sqlx::query("SELECT instance_id FROM sayiir_workflow_snapshots")
            .fetch_all(&self.pool)
            .await
            .map_err(PgError)?;

        Ok(rows.iter().map(|r| r.get("instance_id")).collect())
    }
}
