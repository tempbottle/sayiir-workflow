//! Workflow lifecycle: prepare, resume, and finalize.
//!
//! Minimal reimplementation of `sayiir-runtime`'s lifecycle functions for the
//! WASM environment (no tokio dependency).

use bytes::Bytes;
use sayiir_core::error::WorkflowError;
use sayiir_core::snapshot::{
    ExecutionPosition, SignalKind, WorkflowSnapshot, WorkflowSnapshotState,
};
use sayiir_core::workflow::{ConflictPolicy, WorkflowStatus};
use sayiir_persistence::{BackendError, SignalStore, SnapshotStore};

use crate::error::to_js_error;

/// Outcome of [`prepare_run`].
pub(crate) enum PrepareRunOutcome {
    /// Snapshot is fresh — execute the workflow from this state.
    /// Boxed so the enum's size is dominated by `ExistingStatus` (the
    /// cheap variant). `WorkflowSnapshot` is large enough that clippy's
    /// `large_enum_variant` lint trips otherwise; the caller unboxes once
    /// at the match site so there's no per-task allocation overhead.
    Fresh(Box<WorkflowSnapshot>),
    /// Existing instance reused under `UseExisting`. Caller must return
    /// the carried status without executing.
    ExistingStatus(WorkflowStatus, Option<Bytes>),
}

/// Outcome of [`prepare_resume`].
pub(crate) enum ResumeOutcome {
    /// Workflow can be resumed with this snapshot and input.
    Ready {
        snapshot: Box<WorkflowSnapshot>,
        input_bytes: Bytes,
    },
    /// Workflow is already in a terminal state.
    AlreadyTerminal(WorkflowStatus),
    /// Workflow is paused.
    Paused(WorkflowStatus),
    /// Parked position not yet ready.
    NotReady(WorkflowStatus),
}

/// Prepare a workflow run while honouring the configured [`ConflictPolicy`].
///
/// - **`Fail`** — returns `RunConflict::AlreadyExists` if a snapshot for
///   `instance_id` already exists. This is the default and prevents the
///   silent-overwrite footgun where `run()` is called twice with the same
///   id on a parked workflow.
/// - **`UseExisting`** — returns the existing instance's current status
///   without re-executing; idempotent re-entry for clients that retry.
/// - **`TerminateExisting`** — deletes the existing snapshot + clears
///   cancel/pause signals, then starts fresh.
///
/// Definition-hash mismatches always abort regardless of policy.
pub(crate) async fn prepare_run<B>(
    instance_id: String,
    definition_hash: String,
    input_bytes: Bytes,
    first_task_id: String,
    backend: &B,
    conflict_policy: ConflictPolicy,
) -> Result<PrepareRunOutcome, RunConflict>
where
    B: SnapshotStore + SignalStore,
{
    match backend.load_snapshot(&instance_id).await {
        Ok(existing) => {
            if existing.definition_hash != definition_hash {
                return Err(RunConflict::DefinitionMismatch {
                    expected: definition_hash,
                    found: existing.definition_hash,
                });
            }
            match conflict_policy {
                ConflictPolicy::Fail => return Err(RunConflict::AlreadyExists(instance_id)),
                ConflictPolicy::UseExisting => {
                    let output = existing.state.completed_output().cloned();
                    return Ok(PrepareRunOutcome::ExistingStatus(
                        existing.state.as_status(),
                        output,
                    ));
                }
                ConflictPolicy::TerminateExisting => {
                    backend.delete_snapshot(&instance_id).await?;
                    backend
                        .clear_signal(&instance_id, SignalKind::Cancel)
                        .await?;
                    backend
                        .clear_signal(&instance_id, SignalKind::Pause)
                        .await?;
                }
            }
        }
        Err(BackendError::NotFound(_)) => {}
        Err(e) => return Err(e.into()),
    }

    let mut snapshot =
        WorkflowSnapshot::with_initial_input(instance_id, definition_hash, input_bytes);
    snapshot.update_position(ExecutionPosition::AtTask {
        task_id: first_task_id,
    });
    backend.save_snapshot(&snapshot).await?;
    Ok(PrepareRunOutcome::Fresh(Box::new(snapshot)))
}

/// Reasons [`prepare_run`] may reject a call.
#[derive(Debug)]
pub(crate) enum RunConflict {
    /// `Fail` policy and the instance id is already in use.
    AlreadyExists(String),
    /// The existing snapshot was produced from a different workflow definition.
    DefinitionMismatch { expected: String, found: String },
    /// Backend I/O error.
    Backend(BackendError),
}

impl From<BackendError> for RunConflict {
    fn from(e: BackendError) -> Self {
        RunConflict::Backend(e)
    }
}

impl RunConflict {
    pub(crate) fn into_js_error(self) -> wasm_bindgen::JsValue {
        match self {
            RunConflict::AlreadyExists(id) => to_js_error(format!(
                "Workflow instance '{id}' already exists. Pass conflictPolicy: 'use_existing' or 'terminate_existing' to override, or call resume() instead.",
            )),
            RunConflict::DefinitionMismatch { expected, found } => to_js_error(format!(
                "Workflow definition mismatch for existing snapshot: expected '{expected}', found '{found}'",
            )),
            RunConflict::Backend(e) => to_js_error(e.to_string()),
        }
    }
}

/// Prepare to resume a workflow from a saved snapshot.
pub(crate) async fn prepare_resume<B: SignalStore>(
    instance_id: &str,
    definition_hash: &str,
    backend: &B,
) -> Result<ResumeOutcome, wasm_bindgen::JsValue> {
    let mut snapshot = backend
        .load_snapshot(instance_id)
        .await
        .map_err(to_js_error)?;

    if snapshot.definition_hash != definition_hash {
        return Err(to_js_error(format!(
            "Definition mismatch: expected '{}', found '{}'",
            definition_hash, snapshot.definition_hash
        )));
    }

    if let Some(status) = snapshot.state.as_terminal_status() {
        if snapshot.state.is_paused() {
            return Ok(ResumeOutcome::Paused(status));
        }
        return Ok(ResumeOutcome::AlreadyTerminal(status));
    }

    if let Some(status) = resolve_parked(&mut snapshot, instance_id, backend).await? {
        return Ok(ResumeOutcome::NotReady(status));
    }

    let input_bytes = get_resume_input(&snapshot)?;
    Ok(ResumeOutcome::Ready {
        snapshot: Box::new(snapshot),
        input_bytes,
    })
}

/// Finalize a workflow execution, converting the result to a [`WorkflowStatus`].
pub(crate) async fn finalize_execution<B: SnapshotStore>(
    result: Result<Bytes, WorkflowError>,
    snapshot: &mut WorkflowSnapshot,
    backend: &B,
) -> Result<(WorkflowStatus, Option<Bytes>), wasm_bindgen::JsValue> {
    match result {
        Ok(output) => {
            snapshot.mark_completed(output.clone());
            backend.save_snapshot(snapshot).await.map_err(to_js_error)?;
            Ok((WorkflowStatus::Completed, Some(output)))
        }
        Err(WorkflowError::Waiting { wake_at }) => {
            let delay_id = match &snapshot.state {
                WorkflowSnapshotState::InProgress {
                    position: ExecutionPosition::AtDelay { delay_id, .. },
                    ..
                } => delay_id.clone(),
                WorkflowSnapshotState::InProgress {
                    position: ExecutionPosition::AtFork { fork_id, .. },
                    ..
                } => fork_id.clone(),
                _ => String::new(),
            };
            Ok((WorkflowStatus::Waiting { wake_at, delay_id }, None))
        }
        Err(WorkflowError::AwaitingSignal {
            signal_id,
            signal_name,
            wake_at,
        }) => Ok((
            WorkflowStatus::AwaitingSignal {
                signal_id,
                signal_name,
                wake_at,
            },
            None,
        )),
        Err(WorkflowError::Cancelled { .. }) => {
            // Reload snapshot to get cancellation details
            if let Ok(cancelled_snapshot) = backend.load_snapshot(&snapshot.instance_id).await
                && let Some((reason, cancelled_by)) =
                    cancelled_snapshot.state.cancellation_details()
            {
                return Ok((
                    WorkflowStatus::Cancelled {
                        reason,
                        cancelled_by,
                    },
                    None,
                ));
            }
            Ok((
                WorkflowStatus::Cancelled {
                    reason: None,
                    cancelled_by: None,
                },
                None,
            ))
        }
        Err(WorkflowError::Paused { .. }) => {
            if let Ok(paused_snapshot) = backend.load_snapshot(&snapshot.instance_id).await
                && let Some((reason, paused_by)) = paused_snapshot.state.pause_details()
            {
                return Ok((WorkflowStatus::Paused { reason, paused_by }, None));
            }
            Ok((
                WorkflowStatus::Paused {
                    reason: None,
                    paused_by: None,
                },
                None,
            ))
        }
        Err(e) => {
            snapshot.mark_failed(e.to_string());
            let _ = backend.save_snapshot(snapshot).await;
            Ok((WorkflowStatus::Failed(e.to_string()), None))
        }
    }
}

/// Get the input for resuming execution from a snapshot.
fn get_resume_input(snapshot: &WorkflowSnapshot) -> Result<Bytes, wasm_bindgen::JsValue> {
    match &snapshot.state {
        WorkflowSnapshotState::InProgress {
            completed_tasks, ..
        } => {
            if completed_tasks.is_empty() {
                snapshot
                    .initial_input_bytes()
                    .ok_or_else(|| to_js_error("No completed tasks and initial input not stored"))
            } else {
                snapshot
                    .get_last_task_output()
                    .ok_or_else(|| to_js_error("No task results available for resume"))
            }
        }
        _ => Err(to_js_error("Workflow not in progress, cannot resume")),
    }
}

/// Resolve parked positions (delay, signal, fork) on resume.
///
/// Returns `Some(status)` if the workflow should not continue yet.
/// Returns `None` if the parked position has expired and the snapshot was advanced.
#[allow(clippy::too_many_lines)]
async fn resolve_parked<B: SignalStore>(
    snapshot: &mut WorkflowSnapshot,
    instance_id: &str,
    backend: &B,
) -> Result<Option<WorkflowStatus>, wasm_bindgen::JsValue> {
    match &snapshot.state {
        WorkflowSnapshotState::InProgress {
            position:
                ExecutionPosition::AtDelay {
                    delay_id,
                    wake_at,
                    next_task_id,
                    ..
                },
            ..
        } => {
            let delay_id = delay_id.clone();
            let wake_at = *wake_at;
            let next_task_id = next_task_id.clone();

            // Check cancel/pause
            if let Some(status) = check_cancel_pause(backend, instance_id, Some(&delay_id)).await? {
                return Ok(Some(status));
            }

            if chrono::Utc::now() < wake_at {
                return Ok(Some(WorkflowStatus::Waiting { wake_at, delay_id }));
            }

            // Delay expired — advance
            if let Some(next_id) = next_task_id {
                snapshot.update_position(ExecutionPosition::AtTask { task_id: next_id });
            } else {
                let output = snapshot
                    .get_task_result_bytes(&delay_id)
                    .unwrap_or_default();
                snapshot.mark_completed(output);
                backend.save_snapshot(snapshot).await.map_err(to_js_error)?;
                return Ok(Some(WorkflowStatus::Completed));
            }
            backend.save_snapshot(snapshot).await.map_err(to_js_error)?;
            Ok(None)
        }
        WorkflowSnapshotState::InProgress {
            position: ExecutionPosition::AtFork {
                fork_id, wake_at, ..
            },
            ..
        } => {
            let fork_id = fork_id.clone();
            let wake_at = *wake_at;

            if let Some(status) = check_cancel_pause(backend, instance_id, Some(&fork_id)).await? {
                return Ok(Some(status));
            }

            if chrono::Utc::now() < wake_at {
                return Ok(Some(WorkflowStatus::Waiting {
                    wake_at,
                    delay_id: fork_id,
                }));
            }

            // Wake has passed — position stays `AtFork` because the executor
            // re-enters the fork loop from here and walks the branches itself.
            // Persist anyway so `updated_at` is bumped: otherwise resumeAll
            // would still see this row as stale (updated_at frozen at the
            // original fork-entry timestamp) and a parallel cron tick could
            // launch a concurrent resume() while we're executing the fork.
            backend.save_snapshot(snapshot).await.map_err(to_js_error)?;
            Ok(None)
        }
        WorkflowSnapshotState::InProgress {
            position:
                ExecutionPosition::AtSignal {
                    signal_id,
                    signal_name,
                    wake_at,
                    next_task_id,
                },
            ..
        } => {
            let signal_id = signal_id.clone();
            let signal_name = signal_name.clone();
            let wake_at = *wake_at;
            let next_task_id = next_task_id.clone();

            if let Some(status) = check_cancel_pause(backend, instance_id, Some(&signal_id)).await?
            {
                return Ok(Some(status));
            }

            // Try to consume a buffered signal
            if let Ok(Some(payload)) = backend.consume_event(instance_id, &signal_name).await {
                snapshot.mark_task_completed(signal_id.clone(), payload);
                if let Some(next_id) = next_task_id {
                    snapshot.update_position(ExecutionPosition::AtTask { task_id: next_id });
                } else {
                    let output = snapshot
                        .get_task_result_bytes(&signal_id)
                        .unwrap_or_default();
                    snapshot.mark_completed(output);
                    backend.save_snapshot(snapshot).await.map_err(to_js_error)?;
                    return Ok(Some(WorkflowStatus::Completed));
                }
                backend.save_snapshot(snapshot).await.map_err(to_js_error)?;
                return Ok(None);
            }

            // Check timeout
            if let Some(wt) = wake_at
                && chrono::Utc::now() >= wt
            {
                snapshot.mark_task_completed(signal_id, Bytes::new());
                if let Some(next_id) = next_task_id {
                    snapshot.update_position(ExecutionPosition::AtTask { task_id: next_id });
                } else {
                    snapshot.mark_completed(Bytes::new());
                    backend.save_snapshot(snapshot).await.map_err(to_js_error)?;
                    return Ok(Some(WorkflowStatus::Completed));
                }
                backend.save_snapshot(snapshot).await.map_err(to_js_error)?;
                return Ok(None);
            }

            // Still waiting
            Ok(Some(WorkflowStatus::AwaitingSignal {
                signal_id,
                signal_name,
                wake_at,
            }))
        }
        _ => Ok(None),
    }
}

/// Check cancel and pause signals, returning a status if the workflow should stop.
async fn check_cancel_pause<B: SignalStore>(
    backend: &B,
    instance_id: &str,
    scope: Option<&str>,
) -> Result<Option<WorkflowStatus>, wasm_bindgen::JsValue> {
    if backend
        .check_and_cancel(instance_id, scope)
        .await
        .map_err(to_js_error)?
    {
        let snapshot = backend
            .load_snapshot(instance_id)
            .await
            .map_err(to_js_error)?;
        let (reason, cancelled_by) = snapshot
            .state
            .cancellation_details()
            .unwrap_or((None, None));
        return Ok(Some(WorkflowStatus::Cancelled {
            reason,
            cancelled_by,
        }));
    }
    if backend
        .check_and_pause(instance_id)
        .await
        .map_err(to_js_error)?
    {
        let snapshot = backend
            .load_snapshot(instance_id)
            .await
            .map_err(to_js_error)?;
        let (reason, paused_by) = snapshot.state.pause_details().unwrap_or((None, None));
        return Ok(Some(WorkflowStatus::Paused { reason, paused_by }));
    }
    Ok(None)
}
