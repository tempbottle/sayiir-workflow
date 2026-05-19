//! [`SnapshotStore`] implementation for Postgres.

use sayiir_core::codec::{self, Decoder, Encoder};
use sayiir_core::snapshot::{SnapshotStatus, WorkflowSnapshot};
use sayiir_persistence::{BackendError, SnapshotStore};
use sqlx::Row;

use crate::backend::PostgresBackend;
use crate::error::PgError;
use crate::history::snapshot_hash;
use crate::wakeup::{TASK_READY_CHANNEL, build_task_ready_payload};

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
        // Outputs live in `sayiir_workflow_tasks.output`; the blob carries
        // only control state. Hydrate happens on every load path.
        let mut stripped = snapshot.clone();
        stripped.strip_task_outputs();
        let data = self.encode(&stripped)?;
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
        let terminal_status = match SnapshotStatus::from(&snapshot.state) {
            SnapshotStatus::Failed => "failed",
            SnapshotStatus::Cancelled => "cancelled",
            _ => "completed",
        };
        let notify_payload = build_task_ready_payload(snapshot);

        // The runtime calls `mark_task_completed` then `save_snapshot`
        // (not `save_task_result`); persist the just-completed task's
        // output into the sidecar table here too, so dispatch can
        // hydrate `completed_tasks[*].output` from `workflow_tasks`.
        let last_completed_bytes: Option<[u8; 32]> =
            snapshot.last_completed_task_id().map(|t| *t.as_bytes());
        let last_completed_slice: Option<&[u8]> =
            last_completed_bytes.as_ref().map(<[u8; 32]>::as_slice);
        let last_completed_output: Option<bytes::Bytes> = snapshot
            .last_completed_task_id()
            .and_then(|id| snapshot.get_task_result(&id).map(|r| r.output.clone()));
        let last_completed_output_slice: Option<&[u8]> = last_completed_output.as_deref();

        // One round-trip via a CTE chain: bump the snapshot version,
        // append the history row at that version, refresh the
        // workflow_tasks lifecycle, and queue the wakeup NOTIFY — all
        // in one statement. Sibling DML CTEs in PG always run; the
        // gates (`WHERE $task_id IS NOT NULL`, `WHERE $terminal`,
        // `WHERE $notify IS NOT NULL`) make zero-row branches cheap
        // no-ops without conditional Rust string building.
        let query = "
            WITH snap AS (
                INSERT INTO sayiir_workflow_snapshots
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
                RETURNING history_version
            ),
            hist AS (
                INSERT INTO sayiir_workflow_snapshot_history
                    (instance_id, version, status, current_task_id, data, data_hash)
                SELECT $1, snap.history_version, $2, $4, $14, $13
                FROM snap
                RETURNING 1
            ),
            task_active AS (
                INSERT INTO sayiir_workflow_tasks (instance_id, task_id, status, started_at)
                SELECT $1, $4, 'active', now()
                WHERE $4 IS NOT NULL AND NOT $7
                ON CONFLICT (instance_id, task_id) DO UPDATE SET
                    status = CASE
                        WHEN sayiir_workflow_tasks.status = 'completed' THEN sayiir_workflow_tasks.status
                        ELSE 'active'
                    END,
                    started_at = COALESCE(sayiir_workflow_tasks.started_at, now())
                RETURNING 1
            ),
            task_terminal AS (
                UPDATE sayiir_workflow_tasks
                SET status = $15, completed_at = now(), error = $6
                WHERE instance_id = $1 AND status = 'active' AND $7
                RETURNING 1
            ),
            task_output AS (
                INSERT INTO sayiir_workflow_tasks
                    (instance_id, task_id, status, completed_at, output)
                SELECT $1, $18, 'completed', now(), $19
                WHERE $18 IS NOT NULL
                ON CONFLICT (instance_id, task_id) DO UPDATE SET
                    status = 'completed',
                    completed_at = now(),
                    error = NULL,
                    output = EXCLUDED.output
                WHERE sayiir_workflow_tasks.output IS NULL
                   OR sayiir_workflow_tasks.output IS DISTINCT FROM EXCLUDED.output
                RETURNING 1
            ),
            notify AS (
                SELECT pg_notify($16, $17) WHERE $17 IS NOT NULL
            )
            SELECT history_version FROM snap
        ";

        sqlx::query(query)
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
            .bind(&data) // $14
            .bind(terminal_status) // $15
            .bind(TASK_READY_CHANNEL) // $16
            .bind(notify_payload.as_deref()) // $17
            .bind(last_completed_slice) // $18
            .bind(last_completed_output_slice) // $19
            .fetch_one(&self.pool)
            .await
            .map_err(PgError)?;
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

        // FOR UPDATE OF s serialises read-modify-write across this path
        // and signal_store's mutators — TaskClaim only protects against
        // peer workers, not against concurrent check_and_cancel.
        let (mut snapshot, prev_history_version) = self
            .lock_snapshot_for_mutation(&mut tx, instance_id)
            .await?
            .ok_or_else(|| BackendError::NotFound(instance_id.to_string()))?;

        let output_bytes = output.clone();
        snapshot.mark_task_completed(*task_id, output);

        // The newly-completed output is written below to
        // `sayiir_workflow_tasks.output`; strip from the blob.
        let mut stripped = snapshot.clone();
        stripped.strip_task_outputs();
        let data = self.encode(&stripped)?;
        let data_hash = snapshot_hash(&data);
        let status = snapshot.state.as_ref();
        let current_bytes: Option<[u8; 32]> = snapshot.current_task_id().map(|t| *t.as_bytes());
        let current: Option<&[u8]> = current_bytes.as_ref().map(<[u8; 32]>::as_slice);
        let task_count = snapshot.completed_task_count();
        let next_history_version = prev_history_version + 1;
        let notify_payload = build_task_ready_payload(&snapshot);

        // UPDATE snapshots + INSERT history + UPSERT workflow_tasks +
        // pg_notify in one CTE — the lock acquired above by
        // lock_snapshot_for_mutation is still held in this same tx, so
        // all four writes commit together.
        sqlx::query(
            "WITH upd AS (
                 UPDATE sayiir_workflow_snapshots
                 SET status = $1, current_task_id = $2,
                     completed_task_count = $3, history_version = $4,
                     data_hash = $5, updated_at = now()
                 WHERE instance_id = $6
                 RETURNING 1
             ),
             hist AS (
                 INSERT INTO sayiir_workflow_snapshot_history
                     (instance_id, version, status, current_task_id, data, data_hash)
                 VALUES ($6, $4, $1, $2, $7, $5)
                 RETURNING 1
             ),
             task AS (
                 INSERT INTO sayiir_workflow_tasks
                     (instance_id, task_id, status, completed_at, output)
                 VALUES ($6, $8, 'completed', now(), $9)
                 ON CONFLICT (instance_id, task_id) DO UPDATE SET
                     status = 'completed', completed_at = now(), error = NULL,
                     output = EXCLUDED.output
                 RETURNING 1
             ),
             notify AS (
                 SELECT pg_notify($10, $11) WHERE $11 IS NOT NULL
             )
             SELECT 1 AS done",
        )
        .bind(status) // $1
        .bind(current) // $2
        .bind(task_count) // $3
        .bind(next_history_version) // $4
        .bind(data_hash.as_slice()) // $5
        .bind(instance_id) // $6
        .bind(&data) // $7
        .bind(task_id.as_bytes().as_slice()) // $8
        .bind(output_bytes.as_ref()) // $9
        .bind(TASK_READY_CHANNEL) // $10
        .bind(notify_payload.as_deref()) // $11
        .fetch_one(&mut *tx)
        .await
        .map_err(PgError)?;

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
        let outputs = crate::history::fetch_task_outputs(&self.pool, instance_id).await?;
        snapshot.hydrate_task_outputs(outputs);
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
