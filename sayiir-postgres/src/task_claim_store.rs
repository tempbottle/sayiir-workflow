//! [`TaskClaimStore`] implementation for Postgres.
//!
//! Claims live in two columns on `sayiir_workflow_snapshots`
//! (`claim_owner`, `claim_expires_at`) rather than a dedicated
//! `sayiir_task_claims` table. Acquisition is gated by a fast-fail
//! `pg_try_advisory_xact_lock(hashtext(instance_id))` so racing workers
//! short-circuit without blocking on a row lock. Each acquisition and
//! release is one UPDATE on the snapshot row (HOT-eligible when no
//! other indexed columns change). See migration 013 for the schema
//! change and rationale.

use chrono::{Duration, Utc};
use sayiir_core::codec::{self, Decoder, Encoder};
use sayiir_core::snapshot::{ExecutionPosition, WorkflowSnapshot, WorkflowSnapshotState};
use sayiir_core::task_claim::{AvailableTask, TaskClaim};
use sayiir_persistence::{BackendError, SnapshotStore, TaskClaimStore};
use sqlx::Row;

use crate::backend::PostgresBackend;
use crate::error::PgError;
use crate::history::HISTORY_JOIN;

/// SQL fragment that gates a snapshot row to "claimable right now":
/// the per-row claim slot is free or expired, and no signal is pending
/// on the instance. Shared between `find_available_tasks` (polling scan)
/// and `find_hinted_task` (single-row lookup) so a future change to the
/// eligibility rules can't silently desynchronise the two paths.
const ELIGIBILITY_PREDICATE: &str = "(s.claim_owner IS NULL OR s.claim_expires_at < now())
        AND NOT EXISTS (
            SELECT 1 FROM sayiir_workflow_signals sig
            WHERE sig.instance_id = s.instance_id
        )";

impl<C> TaskClaimStore for PostgresBackend<C>
where
    C: Encoder
        + Decoder
        + codec::sealed::EncodeValue<WorkflowSnapshot>
        + codec::sealed::DecodeValue<WorkflowSnapshot>,
{
    #[tracing::instrument(
        name = "db.claim_task",
        skip(self),
        fields(db.system = "postgresql"),
        err(level = tracing::Level::ERROR),
    )]
    async fn claim_task(
        &self,
        instance_id: &str,
        task_id: &sayiir_core::TaskId,
        worker_id: &str,
        ttl: Option<Duration>,
    ) -> Result<Option<TaskClaim>, BackendError> {
        tracing::debug!("claiming task");
        let expires_at = ttl.and_then(|d| Utc::now().checked_add_signed(d));

        // Advisory lock as a fast-fail gate, then conditional UPDATE.
        //
        // The CTE evaluates `pg_try_advisory_xact_lock(hashtext($1))`
        // first; the `FROM lock WHERE lock.got` join means if the lock
        // wasn't acquired (another worker is racing this same instance),
        // no row is updated and `RETURNING` is empty. The lock auto-
        // releases at end of the implicit transaction wrapping this
        // statement — it's not held across our caller's subsequent
        // execution. Race protection from there on is provided by the
        // `claim_owner` / `claim_expires_at` columns themselves.
        //
        // The `current_task_id = $2` guard is defensive: if the snapshot
        // has advanced past the task we're trying to claim (because
        // another worker beat us through a chain of saves), the UPDATE
        // affects 0 rows and we return None — caller falls back to the
        // polling scan.
        let query = "
            WITH lock AS (
                SELECT pg_try_advisory_xact_lock(hashtext($1::text)) AS got
            )
            UPDATE sayiir_workflow_snapshots s
            SET claim_owner = $3,
                claim_expires_at = $4
            FROM lock
            WHERE s.instance_id = $1
              AND lock.got
              AND s.status = 'InProgress'
              AND s.current_task_id = $2
              AND (s.claim_owner IS NULL OR s.claim_expires_at < now())
            RETURNING
                s.instance_id,
                EXTRACT(EPOCH FROM now())::BIGINT AS claimed_epoch,
                EXTRACT(EPOCH FROM s.claim_expires_at)::BIGINT AS expires_epoch
        ";
        let row = sqlx::query(query)
            .bind(instance_id)
            .bind(task_id.as_bytes().as_slice())
            .bind(worker_id)
            .bind(expires_at)
            .fetch_optional(&self.pool)
            .await
            .map_err(PgError)?;

        Ok(row.map(|r| TaskClaim {
            instance_id: std::sync::Arc::from(r.get::<&str, _>("instance_id")),
            task_id: *task_id,
            worker_id: worker_id.to_string(),
            claimed_at: r.get::<i64, _>("claimed_epoch").cast_unsigned(),
            expires_at: r
                .get::<Option<i64>, _>("expires_epoch")
                .map(i64::cast_unsigned),
        }))
    }

    #[tracing::instrument(
        name = "db.release_task_claim",
        skip(self),
        fields(db.system = "postgresql"),
        err(level = tracing::Level::ERROR),
    )]
    async fn release_task_claim(
        &self,
        instance_id: &str,
        _task_id: &sayiir_core::TaskId,
        worker_id: &str,
    ) -> Result<(), BackendError> {
        tracing::debug!("releasing task claim");
        // Clear the claim slot iff this worker still owns it. If the slot
        // is empty (already released) or owned by someone else (claim
        // expired and another worker re-claimed), the UPDATE affects 0
        // rows and we silently no-op — Sayiir's claim model has always
        // allowed expired claims to be re-acquired and the original
        // owner's release of a stale claim is a benign no-op rather than
        // an error.
        sqlx::query(
            "UPDATE sayiir_workflow_snapshots
             SET claim_owner = NULL, claim_expires_at = NULL
             WHERE instance_id = $1 AND claim_owner = $2",
        )
        .bind(instance_id)
        .bind(worker_id)
        .execute(&self.pool)
        .await
        .map_err(PgError)?;
        Ok(())
    }

    #[tracing::instrument(
        name = "db.extend_task_claim",
        skip(self),
        fields(db.system = "postgresql"),
        err(level = tracing::Level::ERROR),
    )]
    async fn extend_task_claim(
        &self,
        instance_id: &str,
        _task_id: &sayiir_core::TaskId,
        worker_id: &str,
        additional_duration: Duration,
    ) -> Result<(), BackendError> {
        tracing::debug!("extending task claim");
        // Heartbeat: bump `claim_expires_at` by `additional_duration` iff
        // this worker still owns the slot. If the claim has already been
        // stolen (TTL elapsed → another worker re-claimed), the UPDATE
        // affects 0 rows; the caller's next `find_available_tasks` will
        // surface the new state.
        sqlx::query(
            "UPDATE sayiir_workflow_snapshots
             SET claim_expires_at = COALESCE(claim_expires_at, now()) + $3
             WHERE instance_id = $1 AND claim_owner = $2",
        )
        .bind(instance_id)
        .bind(worker_id)
        .bind(additional_duration)
        .execute(&self.pool)
        .await
        .map_err(PgError)?;
        Ok(())
    }

    #[tracing::instrument(
        name = "db.find_available_tasks",
        skip(self),
        fields(db.system = "postgresql"),
        err(level = tracing::Level::ERROR),
    )]
    #[allow(clippy::cast_precision_loss, clippy::too_many_lines)]
    async fn find_available_tasks(
        &self,
        worker_id: &str,
        limit: usize,
        aging_interval: Duration,
        worker_tags: &[String],
    ) -> Result<Vec<AvailableTask>, BackendError> {
        // Expired claims are no longer a separate table — there's nothing
        // to garbage-collect here. The eligibility predicate uses
        // `claim_expires_at < now()` so stale claim slots are picked up
        // implicitly on the next dequeue.

        let aging_secs = (aging_interval.num_milliseconds() as f64 / 1000.0).max(1.0);
        let worker_tags_vec: Vec<&str> = worker_tags.iter().map(String::as_str).collect();
        let tag_filter = if worker_tags.is_empty() {
            ""
        } else {
            "AND s.task_tags <@ $3"
        };

        // `FOR UPDATE OF s SKIP LOCKED` keeps statement-scope dedup —
        // two pollers running simultaneously skip each other's rows
        // instead of returning the full set in duplicate. The actual
        // claim race is resolved by the conditional UPDATE in
        // `claim_task` plus its advisory lock.
        let query = format!(
            "SELECT s.instance_id, h.data, s.trace_parent
             FROM sayiir_workflow_snapshots s
             {HISTORY_JOIN}
             WHERE s.status = 'InProgress'
               AND (
                   s.position_kind = 'AtTask'
                   OR (s.position_kind = 'AtDelay'
                       AND s.delay_wake_at IS NOT NULL
                       AND s.delay_wake_at <= now())
               )
               AND {ELIGIBILITY_PREDICATE}
               {tag_filter}
             ORDER BY
               (s.task_priority - EXTRACT(EPOCH FROM (now() - s.updated_at)) / $2) ASC,
               s.updated_at ASC
             LIMIT $1
             FOR UPDATE OF s SKIP LOCKED"
        );

        let mut q = sqlx::query(&query)
            .bind(i64::try_from(limit).unwrap_or(i64::MAX))
            .bind(aging_secs);
        if !worker_tags.is_empty() {
            q = q.bind(&worker_tags_vec);
        }
        let rows = q.fetch_all(&self.pool).await.map_err(PgError)?;

        let mut available: Vec<(bool, AvailableTask)> = Vec::with_capacity(rows.len());
        for row in &rows {
            let raw: &[u8] = row.get("data");
            let mut snapshot = self.decode(raw)?;
            snapshot.trace_parent = row.get("trace_parent");

            match &snapshot.state {
                // Delay: if expired, advance past it
                WorkflowSnapshotState::InProgress {
                    position:
                        ExecutionPosition::AtDelay {
                            wake_at,
                            next_task_id,
                            delay_id,
                            ..
                        },
                    ..
                } if Utc::now() >= *wake_at => {
                    if let Some(next_id) = *next_task_id {
                        snapshot.update_position(ExecutionPosition::AtTask { task_id: next_id });
                        self.save_snapshot(&snapshot).await?;

                        if let WorkflowSnapshotState::InProgress {
                            position: ExecutionPosition::AtTask { task_id },
                            completed_tasks,
                            ..
                        } = &snapshot.state
                            && let Some(task) =
                                build_available_task(&snapshot, task_id, completed_tasks)
                        {
                            let bias = snapshot.has_failed_on_worker(task_id, worker_id);
                            available.push((bias, task));
                        }
                    } else {
                        // Delay is the last node — complete the workflow
                        let output = snapshot.get_task_result_bytes(delay_id).unwrap_or_default();
                        snapshot.mark_completed(output);
                        self.save_snapshot(&snapshot).await?;
                    }
                }

                WorkflowSnapshotState::InProgress {
                    position: ExecutionPosition::AtTask { task_id },
                    completed_tasks,
                    ..
                } => {
                    if completed_tasks.contains_key(task_id) {
                        continue;
                    }
                    if let Some(rs) = snapshot.task_retries.get(task_id)
                        && Utc::now() < rs.next_retry_at
                    {
                        continue;
                    }
                    if let Some(task) = build_available_task(&snapshot, task_id, completed_tasks) {
                        let bias = snapshot.has_failed_on_worker(task_id, worker_id);
                        available.push((bias, task));
                    }
                }

                _ => continue,
            }

            if available.len() >= limit {
                break;
            }
        }

        available.sort_by_key(|(bias, _)| *bias);
        tracing::debug!(count = available.len(), "available tasks found");
        Ok(available.into_iter().map(|(_, task)| task).collect())
    }

    async fn wait_for_wakeup(
        &self,
        timeout: std::time::Duration,
    ) -> Result<Option<sayiir_persistence::TaskWakeupHint>, BackendError> {
        Ok(self.wakeup.wait(timeout).await)
    }

    #[tracing::instrument(
        name = "db.find_hinted_task",
        skip(self, hint),
        fields(
            db.system = "postgresql",
            instance_id = %hint.instance_id,
            task_id = %sayiir_core::TaskId::from_bytes(hint.task_id),
        ),
        err(level = tracing::Level::ERROR),
    )]
    async fn find_hinted_task(
        &self,
        hint: &sayiir_persistence::TaskWakeupHint,
    ) -> Result<Option<AvailableTask>, BackendError> {
        // Single-row eligibility check. Same predicate shape as
        // find_available_tasks, but the matching task_id (here from the
        // hint payload) lets us read just the named row.
        let query = format!(
            "SELECT s.history_version, s.trace_parent
             FROM sayiir_workflow_snapshots s
             WHERE s.instance_id = $1
               AND s.current_task_id = $2
               AND s.status = 'InProgress'
               AND {ELIGIBILITY_PREDICATE}"
        );
        let row = sqlx::query(&query)
            .bind(&hint.instance_id)
            .bind(hint.task_id.as_slice())
            .fetch_optional(&self.pool)
            .await
            .map_err(PgError)?;

        let Some(row) = row else { return Ok(None) };

        let history_version: i32 = row.get("history_version");
        let data = self
            .fetch_blob(&self.pool, &hint.instance_id, history_version)
            .await?;
        let mut snapshot = self.decode(&data)?;
        snapshot.trace_parent = row.get("trace_parent");

        let WorkflowSnapshotState::InProgress {
            position: ExecutionPosition::AtTask { task_id },
            completed_tasks,
            ..
        } = &snapshot.state
        else {
            return Ok(None);
        };
        let hint_task_id = sayiir_core::TaskId::from_bytes(hint.task_id);
        if *task_id != hint_task_id || completed_tasks.contains_key(task_id) {
            return Ok(None);
        }

        Ok(build_available_task(
            &snapshot,
            &hint_task_id,
            completed_tasks,
        ))
    }
}

/// Build an [`AvailableTask`] from a snapshot at a task position.
fn build_available_task(
    snapshot: &WorkflowSnapshot,
    task_id: &sayiir_core::TaskId,
    completed_tasks: &std::collections::HashMap<
        sayiir_core::TaskId,
        sayiir_core::snapshot::TaskResult,
    >,
) -> Option<AvailableTask> {
    let input = if completed_tasks.is_empty() {
        snapshot.initial_input_bytes()
    } else {
        snapshot.get_last_task_output()
    };

    input.map(|input_bytes| AvailableTask {
        instance_id: snapshot.instance_id.clone(),
        task_id: *task_id,
        input: input_bytes,
        workflow_definition_hash: snapshot.definition_hash,
        trace_parent: snapshot.trace_parent.clone(),
    })
}
