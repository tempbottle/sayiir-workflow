//! [`SignalStore`] implementation for Postgres.
//!
//! Overrides the 3 default composite methods with single-transaction
//! implementations for true ACID atomicity.

use sayiir_core::codec::{self, Decoder, Encoder};
use sayiir_core::snapshot::{PauseRequest, SignalKind, SignalRequest, WorkflowSnapshot};
use sayiir_persistence::validation::validate_signal_allowed;
use sayiir_persistence::{BackendError, SignalStore};
use sqlx::Row;

use crate::backend::PostgresBackend;
use crate::error::PgError;
use crate::history::append_history;
use crate::wakeup::{TASK_READY_CHANNEL, build_task_ready_payload};

impl<C> SignalStore for PostgresBackend<C>
where
    C: Encoder
        + Decoder
        + codec::sealed::EncodeValue<WorkflowSnapshot>
        + codec::sealed::DecodeValue<WorkflowSnapshot>,
{
    #[tracing::instrument(
        name = "db.store_signal",
        skip(self, request),
        fields(db.system = "postgresql", kind = %kind.as_ref()),
        err(level = tracing::Level::ERROR),
    )]
    async fn store_signal(
        &self,
        instance_id: &str,
        kind: SignalKind,
        request: SignalRequest,
    ) -> Result<(), BackendError> {
        tracing::debug!("storing signal");
        // Lock the snapshot row for the duration of validate-then-insert so a
        // concurrent `save_snapshot` can't transition the workflow to a state
        // that would have failed `validate_signal_allowed` between our read
        // and our write. `save_snapshot` upserts into the same row and will
        // block on the lock until this transaction commits.
        let mut tx = self.pool.begin().await.map_err(PgError)?;

        let row = sqlx::query(
            "SELECT status FROM sayiir_workflow_snapshots
             WHERE instance_id = $1
             FOR UPDATE",
        )
        .bind(instance_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(PgError)?
        .ok_or_else(|| BackendError::NotFound(instance_id.to_string()))?;

        let status: String = row.get("status");
        validate_signal_allowed(&status, kind)?;

        sqlx::query(
            "INSERT INTO sayiir_workflow_signals (instance_id, kind, reason, requested_by)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (instance_id, kind) DO UPDATE SET
                reason = $3, requested_by = $4, created_at = now()",
        )
        .bind(instance_id)
        .bind(kind.as_ref())
        .bind(&request.reason)
        .bind(&request.requested_by)
        .execute(&mut *tx)
        .await
        .map_err(PgError)?;

        tx.commit().await.map_err(PgError)?;
        Ok(())
    }

    #[tracing::instrument(
        name = "db.get_signal",
        skip(self),
        fields(db.system = "postgresql", kind = %kind.as_ref()),
        err(level = tracing::Level::ERROR),
    )]
    async fn get_signal(
        &self,
        instance_id: &str,
        kind: SignalKind,
    ) -> Result<Option<SignalRequest>, BackendError> {
        tracing::debug!("getting signal");
        let row = sqlx::query(
            "SELECT reason, requested_by, created_at
             FROM sayiir_workflow_signals
             WHERE instance_id = $1 AND kind = $2",
        )
        .bind(instance_id)
        .bind(kind.as_ref())
        .fetch_optional(&self.pool)
        .await
        .map_err(PgError)?;

        Ok(row.map(|r| SignalRequest {
            reason: r.get("reason"),
            requested_by: r.get("requested_by"),
            requested_at: r.get("created_at"),
        }))
    }

    #[tracing::instrument(
        name = "db.clear_signal",
        skip(self),
        fields(db.system = "postgresql", kind = %kind.as_ref()),
        err(level = tracing::Level::ERROR),
    )]
    async fn clear_signal(&self, instance_id: &str, kind: SignalKind) -> Result<(), BackendError> {
        tracing::debug!("clearing signal");
        sqlx::query("DELETE FROM sayiir_workflow_signals WHERE instance_id = $1 AND kind = $2")
            .bind(instance_id)
            .bind(kind.as_ref())
            .execute(&self.pool)
            .await
            .map_err(PgError)?;
        Ok(())
    }

    #[tracing::instrument(
        name = "db.send_event",
        skip(self, payload),
        fields(db.system = "postgresql"),
        err(level = tracing::Level::ERROR),
    )]
    #[allow(clippy::too_many_lines)]
    async fn send_event(
        &self,
        instance_id: &str,
        signal_name: &str,
        payload: bytes::Bytes,
    ) -> Result<(), BackendError> {
        // Atomic auto-resume path: if the workflow is parked at
        // `AtSignal` waiting for `signal_name`, mark the signal task
        // completed with `payload`, advance position to `AtTask` of the
        // next_task_id stored on the AtSignal variant, and save —
        // skipping the buffered-event detour entirely. PooledWorker
        // dispatch has no AwaitSignal-advance logic, so without this
        // shortcut a parked workflow would never resume (the analogous
        // gap to the pre-fix fork-join dispatch). Falls back to the
        // legacy buffered-event insert when the workflow isn't waiting
        // (e.g. signal arrives before the workflow reaches the
        // wait node — buffered events are consumed at the in-process
        // runner's AwaitSignal handling).
        //
        // Cheap probe first: position_kind lives on the snapshot row
        // (no blob decode, no FOR UPDATE), so the common "signal
        // arrives before the workflow reaches the wait node" case
        // skips opening a tx + FOR UPDATE + outputs hydration just to
        // INSERT a single event row.
        let probe = sqlx::query(
            "SELECT position_kind FROM sayiir_workflow_snapshots
             WHERE instance_id = $1",
        )
        .bind(instance_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(PgError)?;

        let is_at_signal = probe.as_ref().is_some_and(|r| {
            let kind: Option<String> = r.get("position_kind");
            kind.as_deref() == Some("AtSignal")
        });

        if !is_at_signal {
            tracing::debug!(%instance_id, %signal_name, "buffering external event (probe: not at signal)");
            sqlx::query(
                "INSERT INTO sayiir_workflow_events (instance_id, signal_name, payload)
                 VALUES ($1, $2, $3)",
            )
            .bind(instance_id)
            .bind(signal_name)
            .bind(payload.as_ref())
            .execute(&self.pool)
            .await
            .map_err(PgError)?;
            return Ok(());
        }

        let mut tx = self.pool.begin().await.map_err(PgError)?;
        let locked = self
            .lock_snapshot_for_mutation(&mut tx, instance_id)
            .await?;

        if let Some((mut snapshot, prev_history_version)) = locked
            && let Some((signal_id, next_task_id)) = signal_resume_target(&snapshot, signal_name)
        {
            tracing::debug!(%instance_id, %signal_name, "auto-resuming workflow at signal");
            snapshot.mark_task_completed(signal_id, payload);
            if let Some(next_id) = next_task_id {
                snapshot.update_position(sayiir_core::snapshot::ExecutionPosition::AtTask {
                    task_id: next_id,
                });
            } else {
                // Signal was the terminal node — complete the workflow
                // with the signal payload as the final output.
                let output = snapshot
                    .get_task_result_bytes(&signal_id)
                    .unwrap_or_default();
                snapshot.mark_completed(output);
            }
            // Encode the snapshot AFTER mark_task_completed so the
            // signal task's bytes are picked up by `encode_blob`'s
            // strip step and then re-persisted in sayiir_workflow_tasks
            // via the `task_output` CTE — without that UPSERT the
            // outputs-stripped blob loses the signal payload entirely
            // and the next dispatch hands the join an empty input.
            let signal_payload = snapshot
                .get_task_result_bytes(&signal_id)
                .unwrap_or_default();
            let (data, data_hash) = self.encode_blob(&snapshot)?;
            let status = snapshot.state.as_ref();
            let task_id_bytes: Option<[u8; 32]> = snapshot.current_task_id().map(|t| *t.as_bytes());
            let task_id: Option<&[u8]> = task_id_bytes.as_ref().map(<[u8; 32]>::as_slice);
            let task_count = snapshot.completed_task_count();
            let pos_kind = snapshot.position_kind();
            let wake_at = snapshot.delay_wake_at();
            let terminal = snapshot.state.is_terminal();
            let next_history_version = prev_history_version + 1;
            let notify_payload = build_task_ready_payload(&snapshot);

            // When the signal IS the terminal node, `mark_completed` has
            // flipped state to Completed and the snapshot row must carry
            // a `completed_at` timestamp — otherwise retention sweeps,
            // dashboards, and `WHERE completed_at IS NOT NULL` filters
            // silently miss signal-driven completions.
            sqlx::query(
                "WITH upd AS (
                     UPDATE sayiir_workflow_snapshots
                     SET status = $1, current_task_id = $2,
                         completed_task_count = $3, position_kind = $4,
                         delay_wake_at = $5, history_version = $6,
                         data_hash = $7,
                         completed_at = CASE
                             WHEN $13 THEN now()
                             ELSE completed_at
                         END,
                         updated_at = now()
                     WHERE instance_id = $8
                     RETURNING 1
                 ),
                 task_output AS (
                     INSERT INTO sayiir_workflow_tasks
                         (instance_id, task_id, status, completed_at, output)
                     VALUES ($8, $11, 'completed', now(), $12)
                     ON CONFLICT (instance_id, task_id) DO UPDATE SET
                         status = 'completed',
                         completed_at = now(),
                         error = NULL,
                         output = EXCLUDED.output
                     RETURNING 1
                 )
                 SELECT pg_notify($9, $10) FROM upd WHERE $10 IS NOT NULL",
            )
            .bind(status)
            .bind(task_id)
            .bind(task_count)
            .bind(pos_kind)
            .bind(wake_at)
            .bind(next_history_version)
            .bind(data_hash.as_slice())
            .bind(instance_id)
            .bind(TASK_READY_CHANNEL)
            .bind(notify_payload.as_deref())
            .bind(signal_id.as_bytes().as_slice())
            .bind(signal_payload.as_ref())
            .bind(terminal)
            .execute(&mut *tx)
            .await
            .map_err(PgError)?;

            append_history(
                &mut tx,
                instance_id,
                next_history_version,
                status,
                task_id,
                &data,
                &data_hash,
            )
            .await?;

            tx.commit().await.map_err(PgError)?;
            return Ok(());
        }

        // Not parked on this signal — buffer for later consumption by
        // the in-process AwaitSignal handling (or a future poll-based
        // recovery on the PooledWorker side).
        tracing::debug!(%instance_id, %signal_name, "buffering external event");
        sqlx::query(
            "INSERT INTO sayiir_workflow_events (instance_id, signal_name, payload)
             VALUES ($1, $2, $3)",
        )
        .bind(instance_id)
        .bind(signal_name)
        .bind(payload.as_ref())
        .execute(&mut *tx)
        .await
        .map_err(PgError)?;
        tx.commit().await.map_err(PgError)?;
        Ok(())
    }

    #[tracing::instrument(
        name = "db.consume_event",
        skip(self),
        fields(db.system = "postgresql"),
        err(level = tracing::Level::ERROR),
    )]
    async fn consume_event(
        &self,
        instance_id: &str,
        signal_name: &str,
    ) -> Result<Option<bytes::Bytes>, BackendError> {
        tracing::debug!("consuming oldest buffered event");
        // Atomically delete-and-return the oldest event for this (instance, signal).
        let row = sqlx::query(
            "DELETE FROM sayiir_workflow_events
             WHERE id = (
                 SELECT id FROM sayiir_workflow_events
                 WHERE instance_id = $1 AND signal_name = $2
                 ORDER BY id ASC
                 LIMIT 1
                 FOR UPDATE SKIP LOCKED
             )
             RETURNING payload",
        )
        .bind(instance_id)
        .bind(signal_name)
        .fetch_optional(&self.pool)
        .await
        .map_err(PgError)?;

        Ok(row.map(|r| {
            let raw: Vec<u8> = r.get("payload");
            bytes::Bytes::from(raw)
        }))
    }

    // --- Overridden composites: single ACID transactions ---

    #[tracing::instrument(
        name = "db.check_and_cancel",
        skip(self),
        fields(db.system = "postgresql"),
        err(level = tracing::Level::ERROR),
    )]
    async fn check_and_cancel(
        &self,
        instance_id: &str,
        interrupted_at_task: Option<sayiir_core::TaskId>,
    ) -> Result<bool, BackendError> {
        tracing::debug!("checking for cancel signal");
        let mut tx = self.pool.begin().await.map_err(PgError)?;

        // Check for cancel signal (lock the row)
        let signal_row = sqlx::query(
            "SELECT reason, requested_by
             FROM sayiir_workflow_signals
             WHERE instance_id = $1 AND kind = $2
             FOR UPDATE",
        )
        .bind(instance_id)
        .bind(SignalKind::Cancel.as_ref())
        .fetch_optional(&mut *tx)
        .await
        .map_err(PgError)?;

        let Some(signal_row) = signal_row else {
            tx.rollback().await.map_err(PgError)?;
            return Ok(false);
        };

        // Lock the snapshot row and load the latest blob from history.
        let Some((mut snapshot, prev_history_version)) = self
            .lock_snapshot_for_mutation(&mut tx, instance_id)
            .await?
        else {
            tx.rollback().await.map_err(PgError)?;
            return Ok(false);
        };

        if !snapshot.state.is_in_progress() {
            tx.rollback().await.map_err(PgError)?;
            return Ok(false);
        }

        let reason: Option<String> = signal_row.get("reason");
        let requested_by: Option<String> = signal_row.get("requested_by");
        snapshot.mark_cancelled(reason, requested_by, interrupted_at_task);

        let (data, data_hash) = self.encode_blob(&snapshot)?;
        let status = snapshot.state.as_ref();
        let error = snapshot.error_message().map(ToString::to_string);
        let pos_kind = snapshot.position_kind();
        let wake_at = snapshot.delay_wake_at();
        let next_history_version = prev_history_version + 1;
        // History row's `current_task_id` records the task the workflow
        // was interrupted at. `snapshot.current_task_id()` returns None
        // here because `mark_cancelled` already transitioned to the
        // Cancelled variant (current_task_id only matches InProgress
        // AtTask), so binding it would lose the interrupted-task
        // pointer in the indexed history column. Use the caller's
        // `interrupted_at_task` directly.
        let task_id_bytes: Option<[u8; 32]> = interrupted_at_task.map(|t| *t.as_bytes());
        let task_id: Option<&[u8]> = task_id_bytes.as_ref().map(<[u8; 32]>::as_slice);
        let notify_payload = build_task_ready_payload(&snapshot);

        // Pipeline the snapshot UPDATE and pg_notify into one statement.
        // pg_notify lands in the outer SELECT (not a sibling CTE) so it
        // can't be pruned by the planner — `FROM upd` forces upd to
        // execute, and the WHERE gates the notify when there's no
        // payload (cancel typically lands on a terminal state with no
        // current_task_id, so payload is NULL).
        sqlx::query(
            "WITH upd AS (
                 UPDATE sayiir_workflow_snapshots
                 SET status = $1, error = $2,
                     position_kind = $3, delay_wake_at = $4,
                     history_version = $5, data_hash = $6,
                     completed_at = now(), updated_at = now()
                 WHERE instance_id = $7
                 RETURNING 1
             )
             SELECT pg_notify($8, $9) FROM upd WHERE $9 IS NOT NULL",
        )
        .bind(status)
        .bind(&error)
        .bind(pos_kind)
        .bind(wake_at)
        .bind(next_history_version)
        .bind(data_hash.as_slice())
        .bind(instance_id)
        .bind(TASK_READY_CHANNEL)
        .bind(notify_payload.as_deref())
        .execute(&mut *tx)
        .await
        .map_err(PgError)?;

        append_history(
            &mut tx,
            instance_id,
            next_history_version,
            status,
            task_id,
            &data,
            &data_hash,
        )
        .await?;

        // Mark any still-active tasks as cancelled
        sqlx::query(
            "UPDATE sayiir_workflow_tasks SET status = 'cancelled', completed_at = now()
             WHERE instance_id = $1 AND status = 'active'",
        )
        .bind(instance_id)
        .execute(&mut *tx)
        .await
        .map_err(PgError)?;

        // Clear the signal
        sqlx::query("DELETE FROM sayiir_workflow_signals WHERE instance_id = $1 AND kind = $2")
            .bind(instance_id)
            .bind(SignalKind::Cancel.as_ref())
            .execute(&mut *tx)
            .await
            .map_err(PgError)?;

        tx.commit().await.map_err(PgError)?;
        tracing::info!(instance_id, "workflow cancelled");
        Ok(true)
    }

    #[tracing::instrument(
        name = "db.check_and_pause",
        skip(self),
        fields(db.system = "postgresql"),
        err(level = tracing::Level::ERROR),
    )]
    async fn check_and_pause(&self, instance_id: &str) -> Result<bool, BackendError> {
        tracing::debug!("checking for pause signal");
        let mut tx = self.pool.begin().await.map_err(PgError)?;

        // Check for pause signal (lock the row)
        let signal_row = sqlx::query(
            "SELECT reason, requested_by
             FROM sayiir_workflow_signals
             WHERE instance_id = $1 AND kind = $2
             FOR UPDATE",
        )
        .bind(instance_id)
        .bind(SignalKind::Pause.as_ref())
        .fetch_optional(&mut *tx)
        .await
        .map_err(PgError)?;

        let Some(signal_row) = signal_row else {
            tx.rollback().await.map_err(PgError)?;
            return Ok(false);
        };

        // Lock the snapshot row and load the latest blob from history.
        let Some((mut snapshot, prev_history_version)) = self
            .lock_snapshot_for_mutation(&mut tx, instance_id)
            .await?
        else {
            tx.rollback().await.map_err(PgError)?;
            return Ok(false);
        };

        if !snapshot.state.is_in_progress() {
            tx.rollback().await.map_err(PgError)?;
            return Ok(false);
        }

        let reason: Option<String> = signal_row.get("reason");
        let requested_by: Option<String> = signal_row.get("requested_by");
        let pause_request = PauseRequest::new(reason, requested_by);
        snapshot.mark_paused(&pause_request);

        let (data, data_hash) = self.encode_blob(&snapshot)?;
        let status = snapshot.state.as_ref();
        let task_id_bytes: Option<[u8; 32]> = snapshot.current_task_id().map(|t| *t.as_bytes());
        let task_id: Option<&[u8]> = task_id_bytes.as_ref().map(<[u8; 32]>::as_slice);
        let task_count = snapshot.completed_task_count();
        let pos_kind = snapshot.position_kind();
        let wake_at = snapshot.delay_wake_at();
        let next_history_version = prev_history_version + 1;
        let notify_payload = build_task_ready_payload(&snapshot);

        // Pipeline UPDATE + pg_notify in one statement. Paused snapshots
        // typically don't carry a wakeup payload, but mirroring the
        // pattern keeps the signal-store write paths uniform with
        // save_snapshot.
        sqlx::query(
            "WITH upd AS (
                 UPDATE sayiir_workflow_snapshots
                 SET status = $1, current_task_id = $2,
                     completed_task_count = $3, position_kind = $4,
                     delay_wake_at = $5, history_version = $6,
                     data_hash = $7, updated_at = now()
                 WHERE instance_id = $8
                 RETURNING 1
             )
             SELECT pg_notify($9, $10) FROM upd WHERE $10 IS NOT NULL",
        )
        .bind(status)
        .bind(task_id)
        .bind(task_count)
        .bind(pos_kind)
        .bind(wake_at)
        .bind(next_history_version)
        .bind(data_hash.as_slice())
        .bind(instance_id)
        .bind(TASK_READY_CHANNEL)
        .bind(notify_payload.as_deref())
        .execute(&mut *tx)
        .await
        .map_err(PgError)?;

        append_history(
            &mut tx,
            instance_id,
            next_history_version,
            status,
            task_id,
            &data,
            &data_hash,
        )
        .await?;

        // Clear the signal
        sqlx::query("DELETE FROM sayiir_workflow_signals WHERE instance_id = $1 AND kind = $2")
            .bind(instance_id)
            .bind(SignalKind::Pause.as_ref())
            .execute(&mut *tx)
            .await
            .map_err(PgError)?;

        tx.commit().await.map_err(PgError)?;
        tracing::info!(instance_id, "workflow paused");
        Ok(true)
    }

    #[tracing::instrument(
        name = "db.unpause",
        skip(self),
        fields(db.system = "postgresql"),
        err(level = tracing::Level::ERROR),
    )]
    async fn unpause(&self, instance_id: &str) -> Result<WorkflowSnapshot, BackendError> {
        tracing::debug!("unpausing workflow");
        let mut tx = self.pool.begin().await.map_err(PgError)?;

        let (mut snapshot, prev_history_version) = self
            .lock_snapshot_for_mutation(&mut tx, instance_id)
            .await?
            .ok_or_else(|| BackendError::NotFound(instance_id.to_string()))?;

        if !snapshot.state.is_paused() {
            let state_name = snapshot.state.as_ref();
            // Explicit rollback releases the FOR UPDATE row lock
            // immediately rather than waiting for sqlx's async
            // Transaction Drop to fire — matches the pattern in
            // check_and_cancel/check_and_pause and matters under
            // bursty admin scripts that hammer unpause across many
            // already-running workflows.
            tx.rollback().await.map_err(PgError)?;
            return Err(BackendError::CannotPause(format!(
                "Workflow is not paused (current state: {state_name:?})"
            )));
        }

        snapshot.mark_unpaused();

        let (data, data_hash) = self.encode_blob(&snapshot)?;
        let status = snapshot.state.as_ref();
        let task_id_bytes: Option<[u8; 32]> = snapshot.current_task_id().map(|t| *t.as_bytes());
        let task_id: Option<&[u8]> = task_id_bytes.as_ref().map(<[u8; 32]>::as_slice);
        let task_count = snapshot.completed_task_count();
        let pos_kind = snapshot.position_kind();
        let wake_at = snapshot.delay_wake_at();
        let next_history_version = prev_history_version + 1;
        let notify_payload = build_task_ready_payload(&snapshot);

        // Unpause restores the snapshot to InProgress AtTask, so this is
        // the one signal-store path that actually emits a NOTIFY.
        // Folding it into the UPDATE eliminates the separate pg_notify
        // round-trip.
        sqlx::query(
            "WITH upd AS (
                 UPDATE sayiir_workflow_snapshots
                 SET status = $1, current_task_id = $2,
                     completed_task_count = $3, position_kind = $4,
                     delay_wake_at = $5, history_version = $6,
                     data_hash = $7, updated_at = now()
                 WHERE instance_id = $8
                 RETURNING 1
             )
             SELECT pg_notify($9, $10) FROM upd WHERE $10 IS NOT NULL",
        )
        .bind(status)
        .bind(task_id)
        .bind(task_count)
        .bind(pos_kind)
        .bind(wake_at)
        .bind(next_history_version)
        .bind(data_hash.as_slice())
        .bind(instance_id)
        .bind(TASK_READY_CHANNEL)
        .bind(notify_payload.as_deref())
        .execute(&mut *tx)
        .await
        .map_err(PgError)?;

        append_history(
            &mut tx,
            instance_id,
            next_history_version,
            status,
            task_id,
            &data,
            &data_hash,
        )
        .await?;

        tx.commit().await.map_err(PgError)?;
        tracing::info!(instance_id, "workflow unpaused");
        Ok(snapshot)
    }
}

/// If `snapshot` is parked at `AtSignal` waiting for `signal_name`,
/// return the (signal_id, next_task_id) pair so `send_event` can
/// advance the workflow inline. Returns `None` for any other position
/// or signal-name mismatch.
fn signal_resume_target(
    snapshot: &WorkflowSnapshot,
    signal_name: &str,
) -> Option<(sayiir_core::TaskId, Option<sayiir_core::TaskId>)> {
    use sayiir_core::snapshot::{ExecutionPosition, WorkflowSnapshotState};
    match &snapshot.state {
        WorkflowSnapshotState::InProgress {
            position:
                ExecutionPosition::AtSignal {
                    signal_id,
                    signal_name: parked_name,
                    next_task_id,
                    ..
                },
            ..
        } if parked_name == signal_name => Some((*signal_id, *next_task_id)),
        _ => None,
    }
}
