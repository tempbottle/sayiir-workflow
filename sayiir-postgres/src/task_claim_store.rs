//! [`TaskClaimStore`] implementation for Postgres.
//!
//! Claim ownership lives in a dedicated narrow table
//! `sayiir_workflow_claims` (one row per in-flight workflow,
//! keyed on `instance_id`). Acquisition is still gated by a
//! fast-fail `pg_try_advisory_xact_lock(hashtextextended(instance_id, 0))`
//! so racing workers short-circuit without blocking on a row lock.
//!
//! Decoupling from the snapshot row matters because the polling
//! `find_available_tasks` scan takes `FOR UPDATE OF s SKIP LOCKED`
//! on `sayiir_workflow_snapshots`. When claim/release wrote to that
//! same row, every dispatch tick produced a lock fight between the
//! poller and the worker that just released. With ownership on a
//! separate table, polling and claim/release never touch the same
//! row and the snapshot UPDATE budget per tick drops to just the
//! state writes (save_snapshot / save_task_result).
//!
//! Eligibility for dispatch is a NOT EXISTS join into this table.
//! The table only holds rows for currently-running workflows so it
//! stays hot in shared_buffers; the planner picks an index-only
//! plan via the PK.

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
/// no active claim on the instance and no pending signal. Shared
/// between `find_available_tasks` (polling scan) and `find_hinted_task`
/// (single-row lookup) so a future change to the eligibility rules
/// can't silently desynchronise the two paths.
const ELIGIBILITY_PREDICATE: &str = "NOT EXISTS (
            SELECT 1 FROM sayiir_workflow_claims c
            WHERE c.instance_id = s.instance_id
              AND (c.expires_at IS NULL OR c.expires_at > now())
        )
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
        let expires_at = ttl.and_then(|d| Utc::now().checked_add_signed(d));

        // One round-trip: the conditional INSERT and the
        // `snapshot_exists` probe share a single statement, so a
        // concurrent insert/delete can't interleave between them. A
        // missing snapshot is reported as `NotFound` (almost always a
        // stale caller); the other no-update reasons — non-InProgress,
        // task advanced, lost advisory lock, slot held and unexpired —
        // collapse to `Ok(None)` for the caller to retry.
        //
        // The INSERT pulls (instance_id, current_task_id) from the
        // matching snapshot row and is gated by the advisory lock plus
        // a NOT EXISTS check on any live claim. ON CONFLICT (instance_id)
        // DO UPDATE overwrites an expired-but-still-present row.
        //
        // Epoch fields use `FLOOR(EXTRACT(EPOCH ...))::BIGINT`: a bare
        // cast to BIGINT rounds, so a `.5+` fractional second would
        // return an epoch one ahead of the stored timestamp and break
        // round-trips against `chrono::DateTime::timestamp()` (floor).
        let query = "
            WITH lock AS (
                SELECT pg_try_advisory_xact_lock(hashtextextended($1::text, 0)) AS got
            ),
            upsert AS (
                INSERT INTO sayiir_workflow_claims
                    (instance_id, task_id, worker_id, expires_at)
                SELECT s.instance_id, s.current_task_id, $3, $4
                FROM sayiir_workflow_snapshots s, lock
                WHERE s.instance_id = $1
                  AND lock.got
                  AND s.status = 'InProgress'
                  AND s.current_task_id = $2
                  AND NOT EXISTS (
                      SELECT 1 FROM sayiir_workflow_claims c
                      WHERE c.instance_id = s.instance_id
                        AND (c.expires_at IS NULL OR c.expires_at > now())
                  )
                ON CONFLICT (instance_id) DO UPDATE
                    SET task_id    = EXCLUDED.task_id,
                        worker_id  = EXCLUDED.worker_id,
                        claimed_at = now(),
                        expires_at = EXCLUDED.expires_at
                RETURNING
                    instance_id,
                    FLOOR(EXTRACT(EPOCH FROM claimed_at))::BIGINT AS claimed_epoch,
                    FLOOR(EXTRACT(EPOCH FROM expires_at))::BIGINT AS expires_epoch
            )
            SELECT instance_id, claimed_epoch, expires_epoch, TRUE AS snapshot_exists
            FROM upsert
            UNION ALL
            SELECT
                NULL::text, NULL::bigint, NULL::bigint,
                EXISTS (SELECT 1 FROM sayiir_workflow_snapshots WHERE instance_id = $1)
            WHERE NOT EXISTS (SELECT 1 FROM upsert)
        ";
        let row = sqlx::query(query)
            .bind(instance_id)
            .bind(task_id.as_bytes().as_slice())
            .bind(worker_id)
            .bind(expires_at)
            .fetch_one(&self.pool)
            .await
            .map_err(PgError)?;

        let claimed_instance_id: Option<&str> = row.get("instance_id");
        if let Some(claimed_instance_id) = claimed_instance_id {
            return Ok(Some(TaskClaim {
                instance_id: std::sync::Arc::from(claimed_instance_id),
                task_id: *task_id,
                worker_id: worker_id.to_string(),
                claimed_at: row.get::<i64, _>("claimed_epoch").cast_unsigned(),
                expires_at: row
                    .get::<Option<i64>, _>("expires_epoch")
                    .map(i64::cast_unsigned),
            }));
        }

        let snapshot_exists: bool = row.get("snapshot_exists");
        if !snapshot_exists {
            return Err(BackendError::NotFound(format!("{instance_id}:{task_id}")));
        }
        Ok(None)
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
        task_id: &sayiir_core::TaskId,
        worker_id: &str,
    ) -> Result<(), BackendError> {
        // DELETE + disambiguating SELECT in one statement: `released`
        // (driven by the writable CTE's RETURNING) carries the
        // success/failure signal, and bundling both into a single
        // round-trip closes the inter-statement window where a
        // concurrent release/re-claim could otherwise make the
        // disambiguation report a misleading error variant.
        let row = sqlx::query(
            "WITH del AS (
                 DELETE FROM sayiir_workflow_claims
                 WHERE instance_id = $1 AND worker_id = $2
                 RETURNING 1
             )
             SELECT
                 EXISTS (SELECT 1 FROM del) AS released,
                 (SELECT worker_id FROM sayiir_workflow_claims
                  WHERE instance_id = $1) AS owner",
        )
        .bind(instance_id)
        .bind(worker_id)
        .fetch_one(&self.pool)
        .await
        .map_err(PgError)?;

        if row.get::<bool, _>("released") {
            return Ok(());
        }
        match row.get::<Option<String>, _>("owner") {
            None => Err(BackendError::NotFound(format!("{instance_id}:{task_id}"))),
            Some(other) => Err(BackendError::Backend(format!(
                "Claim owned by different worker: {other}"
            ))),
        }
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
        task_id: &sayiir_core::TaskId,
        worker_id: &str,
        additional_duration: Duration,
    ) -> Result<(), BackendError> {
        // Heartbeat. A no-TTL claim is eternal-until-released; mirror
        // the in-memory backend by silently no-op'ing the extension in
        // that case (no `COALESCE` — that would *introduce* a TTL).
        // Pairing the UPDATE and the disambiguation SELECT in one
        // statement (one round-trip) avoids the inter-statement window
        // where a concurrent release/re-claim could let the
        // disambiguation lie about who held the slot.
        let row = sqlx::query(
            "WITH upd AS (
                 UPDATE sayiir_workflow_claims
                 SET expires_at = expires_at + $3
                 WHERE instance_id = $1
                   AND worker_id = $2
                   AND expires_at IS NOT NULL
                 RETURNING 1
             )
             SELECT
                 EXISTS (SELECT 1 FROM upd) AS extended,
                 (SELECT worker_id FROM sayiir_workflow_claims
                  WHERE instance_id = $1) AS owner",
        )
        .bind(instance_id)
        .bind(worker_id)
        .bind(additional_duration)
        .fetch_one(&self.pool)
        .await
        .map_err(PgError)?;

        if row.get::<bool, _>("extended") {
            return Ok(());
        }
        match row.get::<Option<String>, _>("owner") {
            None => Err(BackendError::NotFound(format!("{instance_id}:{task_id}"))),
            Some(other) if other != worker_id => Err(BackendError::Backend(format!(
                "Claim owned by different worker: {other}"
            ))),
            // Own the claim but it had no TTL — deliberate no-op.
            Some(_) => Ok(()),
        }
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
        // Expired claims are picked up implicitly via the eligibility
        // predicate's `c.expires_at > now()` check — no explicit
        // garbage-collect needed here.

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
