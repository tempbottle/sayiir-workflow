//! [`TaskClaimStore`] implementation for Postgres.
//!
//! Claim ownership lives in `sayiir_workflow_claims` — one row per
//! in-flight workflow, PK on `instance_id`. Off the snapshot row so
//! `find_available_tasks`' `FOR UPDATE OF s SKIP LOCKED` doesn't
//! lock-fight with claim/release. Acquisition is gated by a
//! fast-fail `pg_try_advisory_xact_lock(hashtextextended(instance_id, 0))`
//! so racing workers short-circuit without blocking on a row lock.

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
        // `snapshot_exists` probe share a single statement so a
        // concurrent insert/delete can't interleave between them. A
        // missing snapshot is reported as `NotFound` (almost always a
        // stale caller); the other no-update reasons — non-InProgress,
        // task advanced, lost advisory lock, slot held and unexpired —
        // collapse to `Ok(None)` for the caller to retry.
        //
        // Epoch fields use `FLOOR(EXTRACT(EPOCH ...))::BIGINT`: a bare
        // cast to BIGINT rounds, so a `.5+` fractional second would
        // return an epoch one ahead of the stored timestamp and break
        // round-trips against `chrono::DateTime::timestamp()` (floor).
        let query = "
            WITH probe AS (
                SELECT instance_id, current_task_id, status
                FROM sayiir_workflow_snapshots
                WHERE instance_id = $1
            ),
            lock AS (
                SELECT pg_try_advisory_xact_lock(hashtextextended($1::text, 0)) AS got
            ),
            upsert AS (
                INSERT INTO sayiir_workflow_claims
                    (instance_id, task_id, worker_id, expires_at)
                SELECT probe.instance_id, probe.current_task_id, $3, $4
                FROM probe, lock
                WHERE lock.got
                  AND probe.status = 'InProgress'
                  AND probe.current_task_id = $2
                  AND NOT EXISTS (
                      SELECT 1 FROM sayiir_workflow_claims c
                      WHERE c.instance_id = probe.instance_id
                        AND (c.expires_at IS NULL OR c.expires_at > now())
                  )
                ON CONFLICT (instance_id) DO UPDATE
                    SET task_id    = EXCLUDED.task_id,
                        worker_id  = EXCLUDED.worker_id,
                        claimed_at = now(),
                        expires_at = EXCLUDED.expires_at
                RETURNING
                    FLOOR(EXTRACT(EPOCH FROM claimed_at))::BIGINT AS claimed_epoch,
                    FLOOR(EXTRACT(EPOCH FROM expires_at))::BIGINT AS expires_epoch
            )
            SELECT claimed_epoch, expires_epoch, TRUE AS snapshot_exists
            FROM upsert
            UNION ALL
            SELECT
                NULL::bigint, NULL::bigint,
                EXISTS (SELECT 1 FROM probe)
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

        if let Some(claimed_epoch) = row.get::<Option<i64>, _>("claimed_epoch") {
            return Ok(Some(TaskClaim {
                instance_id: std::sync::Arc::from(instance_id),
                task_id: *task_id,
                worker_id: worker_id.to_string(),
                claimed_at: claimed_epoch.cast_unsigned(),
                expires_at: row
                    .get::<Option<i64>, _>("expires_epoch")
                    .map(i64::cast_unsigned),
            }));
        }

        if !row.get::<bool, _>("snapshot_exists") {
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
        // DELETE + owner disambiguation in one statement closes the
        // inter-statement window where a concurrent release/re-claim
        // could otherwise make the error path misreport the owner.
        // The owner subselect is gated on `NOT EXISTS (del)` so the
        // hot success path skips it.
        let row = sqlx::query(
            "WITH del AS (
                 DELETE FROM sayiir_workflow_claims
                 WHERE instance_id = $1 AND worker_id = $2
                 RETURNING 1
             )
             SELECT
                 EXISTS (SELECT 1 FROM del) AS done,
                 (SELECT worker_id FROM sayiir_workflow_claims
                  WHERE instance_id = $1 AND NOT EXISTS (SELECT 1 FROM del)) AS owner",
        )
        .bind(instance_id)
        .bind(worker_id)
        .fetch_one(&self.pool)
        .await
        .map_err(PgError)?;

        disambiguate_writeback(&row, instance_id, task_id, worker_id)
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
        // When the row exists with our worker but `expires_at IS NULL`
        // the UPDATE returns 0 rows, the owner subselect returns our
        // worker_id, and `disambiguate_writeback` treats self-owned-
        // not-done as Ok(()).
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
                 EXISTS (SELECT 1 FROM upd) AS done,
                 (SELECT worker_id FROM sayiir_workflow_claims
                  WHERE instance_id = $1 AND NOT EXISTS (SELECT 1 FROM upd)) AS owner",
        )
        .bind(instance_id)
        .bind(worker_id)
        .bind(additional_duration)
        .fetch_one(&self.pool)
        .await
        .map_err(PgError)?;

        disambiguate_writeback(&row, instance_id, task_id, worker_id)
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

/// Decode the `(done, owner)` shape returned by the release/extend
/// writable-CTE queries. `done` carries the success signal; `owner` is
/// the current worker_id when the write didn't match, used to map the
/// failure to the correct error variant. Self-owned-not-done is the
/// extend heartbeat's no-TTL no-op (release never reaches that arm
/// because `WHERE worker_id = $2` already excludes the case).
fn disambiguate_writeback(
    row: &sqlx::postgres::PgRow,
    instance_id: &str,
    task_id: &sayiir_core::TaskId,
    worker_id: &str,
) -> Result<(), BackendError> {
    if row.get::<bool, _>("done") {
        return Ok(());
    }
    match row.get::<Option<String>, _>("owner") {
        None => Err(BackendError::NotFound(format!("{instance_id}:{task_id}"))),
        Some(other) if other == worker_id => Ok(()),
        Some(other) => Err(BackendError::Backend(format!(
            "Claim owned by different worker: {other}"
        ))),
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
