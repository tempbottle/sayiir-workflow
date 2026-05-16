//! Workflow-lifecycle primitives shared across runtimes.
//!
//! [`prepare_run`] handles the snapshot-existence check + conflict-policy
//! resolution that every binding needs to do at the start of a `run()`.
//! The runtime (`sayiir-runtime`) and the Cloudflare Workers binding
//! (`sayiir-cloudflare`) both used to carry their own copies of this
//! logic; this module is the de-duplicated home.
//!
//! The error type [`RunConflict`] carries the three reasons a run can be
//! rejected (`AlreadyExists`, `DefinitionMismatch`, backend I/O). Callers
//! convert it into their own error envelopes via `From<RunConflict>`.

use bytes::Bytes;
use sayiir_core::snapshot::{ExecutionPosition, SignalKind, TaskHint, WorkflowSnapshot};
use sayiir_core::workflow::{ConflictPolicy, WorkflowStatus};

use crate::{BackendError, SignalStore, SnapshotStore};

/// Outcome of [`prepare_run`].
#[derive(Debug)]
pub enum PrepareRunOutcome {
    /// Snapshot is fresh — execute the workflow from this state.
    ///
    /// Boxed so the enum's size is dominated by `ExistingStatus` (the
    /// cheap variant). `WorkflowSnapshot` is large enough that clippy's
    /// `large_enum_variant` lint trips otherwise; the caller unboxes once
    /// at the match site so there's no per-task allocation overhead.
    Fresh(Box<WorkflowSnapshot>),
    /// Existing instance reused under `UseExisting`. Caller must return
    /// the carried status without executing.
    ExistingStatus(WorkflowStatus, Option<Bytes>),
}

/// Reasons [`prepare_run`] may reject a call.
#[derive(Debug)]
pub enum RunConflict {
    /// `Fail` policy and the instance id is already in use.
    AlreadyExists(String),
    /// The existing snapshot was produced from a different workflow definition.
    DefinitionMismatch {
        /// Definition hash the caller expected.
        expected: String,
        /// Definition hash actually stored.
        found: String,
    },
    /// Backend I/O error.
    Backend(BackendError),
}

impl From<BackendError> for RunConflict {
    fn from(e: BackendError) -> Self {
        Self::Backend(e)
    }
}

impl std::fmt::Display for RunConflict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Names are the canonical strum-serialized forms, which both the
        // Rust `ConflictPolicy::*` variants and the JS `conflictPolicy:`
        // engine option accept.
        match self {
            Self::AlreadyExists(id) => write!(
                f,
                "Workflow instance '{id}' already exists. Use conflict policy 'use_existing' or 'terminate_existing' to override, or resume() instead.",
            ),
            Self::DefinitionMismatch { expected, found } => write!(
                f,
                "Workflow definition mismatch: expected '{expected}', found '{found}'",
            ),
            Self::Backend(e) => std::fmt::Display::fmt(e, f),
        }
    }
}

impl std::error::Error for RunConflict {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Backend(e) => Some(e),
            _ => None,
        }
    }
}

/// Prepare a workflow run while honouring the configured [`ConflictPolicy`].
///
/// - **`Fail`** — returns [`RunConflict::AlreadyExists`] if a snapshot for
///   `instance_id` already exists. Default; prevents the silent-overwrite
///   footgun where `run()` is called twice with the same id on a parked
///   workflow.
/// - **`UseExisting`** — returns the existing instance's current status
///   without re-executing; idempotent re-entry for clients that retry.
/// - **`TerminateExisting`** — deletes the existing snapshot + clears
///   cancel/pause signals, then starts fresh.
///
/// Definition-hash mismatches always abort regardless of policy.
///
/// On the `Fresh` path the function:
/// 1. Builds a `WorkflowSnapshot::with_initial_input`,
/// 2. Positions execution at the first task,
/// 3. Stores `first_task`'s hint metadata,
/// 4. Saves the snapshot,
/// 5. Returns the boxed snapshot for the caller to execute.
///
/// # Errors
///
/// Returns [`RunConflict`] for policy rejections, definition mismatches,
/// or backend I/O failures.
pub async fn prepare_run<B>(
    instance_id: String,
    definition_hash: String,
    input_bytes: Bytes,
    first_task: TaskHint,
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
        task_id: first_task.id.clone(),
    });
    snapshot.set_task_hint(&first_task);
    backend.save_snapshot(&snapshot).await?;
    Ok(PrepareRunOutcome::Fresh(Box::new(snapshot)))
}
