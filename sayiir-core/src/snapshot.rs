//! Workflow snapshot structures for checkpoint/restore functionality.
//!
//! Snapshots capture the complete execution state of a workflow, including
//! which tasks have completed and their outputs. This enables resuming
//! workflows from any checkpoint.

use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

use crate::task::RetryPolicy;

/// Pre-computed metadata about the next task to execute.
///
/// Carried through `ParkReason` and `prepare_run` so that persistence-layer
/// advancement inherits priority and tags without re-reading the continuation.
#[derive(Debug, Clone, Default)]
pub struct TaskHint {
    /// SHA-256 hash of the task's user-facing name.
    pub id: crate::TaskId,
    /// Optional execution priority (lower = higher priority).
    pub priority: Option<u8>,
    /// Affinity tags for worker routing.
    pub tags: Vec<String>,
}

impl TaskHint {
    /// Build a hint by hashing the task name.
    #[must_use]
    pub fn new(name: &str, priority: Option<u8>, tags: &[String]) -> Self {
        Self {
            id: crate::TaskId::from(name),
            priority,
            tags: tags.to_vec(),
        }
    }
}

/// A persisted deadline for a running task.
///
/// When a task with a timeout starts, the absolute wall-clock deadline is
/// computed and stored in the snapshot. This is a **durable** timeout: the
/// deadline survives process crashes and is checked on resume before
/// re-executing the task.
///
/// When the deadline expires mid-execution, the task future is dropped
/// (cooperative cancellation) and the workflow is marked `Failed`. In the
/// distributed worker, the deadline is checked on every heartbeat tick. In
/// single-process runners, a periodic interval checks the persisted deadline.
///
/// See the `sayiir-runtime` README for full timeout documentation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskDeadline {
    /// The task this deadline applies to.
    pub task_id: crate::TaskId,
    /// Absolute wall-clock deadline.
    pub deadline: DateTime<Utc>,
    /// Original configured timeout in milliseconds (for error reporting).
    pub timeout_ms: u64,
}

/// Durable retry state for a task that has failed and is pending retry.
///
/// Stored in the snapshot so that retry state survives process crashes.
/// When a task with a `RetryPolicy` fails, the runner records the attempt
/// here and computes the next retry time via exponential backoff.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskRetryState {
    /// Number of retry attempts so far (starts at 1 after the first failure).
    pub attempts: u32,
    /// The retry policy governing this task (stored so state is self-contained on resume).
    pub policy: RetryPolicy,
    /// Error message from the last failure.
    pub last_error: String,
    /// Worker ID that last failed the task (for soft worker-bias on retry).
    pub last_failed_worker: Option<String>,
    /// Absolute wall-clock time after which the next retry may begin.
    pub next_retry_at: DateTime<Utc>,
}

/// Represents the position in workflow execution.
///
/// This tracks which tasks have been completed and where execution should resume.
#[derive(Debug, Clone, Serialize, Deserialize, strum::AsRefStr)]
pub enum ExecutionPosition {
    /// Workflow has not started yet.
    NotStarted,
    /// Execution is at the start of a task.
    /// The task ID indicates which task should be executed next.
    AtTask {
        /// ID of the task to execute next.
        task_id: crate::TaskId,
    },
    /// Execution is parked at a fork because one or more branches hit a delay.
    /// Completed branches have their results cached; delayed branches will
    /// re-execute (skipping cached sub-tasks) once `wake_at` passes.
    AtFork {
        /// Fork node ID.
        fork_id: crate::TaskId,
        /// Branch results collected so far. Keyed by branch label, not task id.
        completed_branches: HashMap<String, TaskResult>,
        /// Earliest time the fork can resume.
        wake_at: DateTime<Utc>,
    },
    /// Execution is at a join task, waiting for all branches.
    AtJoin {
        /// Join task ID.
        join_id: crate::TaskId,
        /// Branch results collected so far. Keyed by branch label, not task id.
        completed_branches: HashMap<String, TaskResult>,
    },
    /// Execution is parked at a delay node, waiting for `wake_at`.
    AtDelay {
        /// Delay node ID.
        delay_id: crate::TaskId,
        /// When the delay was entered.
        entered_at: DateTime<Utc>,
        /// When the delay expires.
        wake_at: DateTime<Utc>,
        /// First task ID after the delay (so backends can advance without traversing the workflow tree).
        next_task_id: Option<crate::TaskId>,
    },
    /// Execution is parked waiting for an external signal.
    AtSignal {
        /// Signal node ID.
        signal_id: crate::TaskId,
        /// Name of the signal being waited on (user-defined string).
        signal_name: String,
        /// Optional timeout deadline. `None` means wait indefinitely.
        wake_at: Option<DateTime<Utc>>,
        /// First task ID after the signal (so backends can advance without traversing the workflow tree).
        next_task_id: Option<crate::TaskId>,
    },
    /// Execution is inside a loop at a given iteration.
    InLoop {
        /// Loop node ID.
        loop_id: crate::TaskId,
        /// Current iteration (0-based).
        iteration: u32,
        /// Next task to execute within the loop body (for resume positioning).
        next_task_id: Option<crate::TaskId>,
    },
}

/// Result of a completed task execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskResult {
    /// The task ID that produced this result.
    pub task_id: crate::TaskId,
    /// The serialized output of the task.
    pub output: Bytes,
}

/// A request to cancel a workflow.
///
/// This is stored separately from the workflow state and checked by workers
/// at task boundaries. The actual `Cancelled` state is set after processing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CancellationRequest {
    /// Optional reason for the cancellation.
    pub reason: Option<String>,
    /// Optional identifier of who requested the cancellation.
    pub requested_by: Option<String>,
    /// Timestamp when the cancellation was requested.
    pub requested_at: DateTime<Utc>,
}

impl CancellationRequest {
    /// Create a new cancellation request with the current timestamp.
    #[must_use]
    pub fn new(reason: Option<String>, requested_by: Option<String>) -> Self {
        Self {
            reason,
            requested_by,
            requested_at: Utc::now(),
        }
    }
}

/// A request to pause a workflow.
///
/// This is stored separately from the workflow state and checked by workers
/// at task boundaries. The actual `Paused` state is set after processing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PauseRequest {
    /// Optional reason for the pause.
    pub reason: Option<String>,
    /// Optional identifier of who requested the pause.
    pub requested_by: Option<String>,
    /// Timestamp when the pause was requested.
    pub requested_at: DateTime<Utc>,
}

impl PauseRequest {
    /// Create a new pause request with the current timestamp.
    #[must_use]
    pub fn new(reason: Option<String>, requested_by: Option<String>) -> Self {
        Self {
            reason,
            requested_by,
            requested_at: Utc::now(),
        }
    }
}

/// Kind of signal that can be sent to a running workflow.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, strum::AsRefStr)]
pub enum SignalKind {
    /// Request cancellation.
    Cancel,
    /// Request pause.
    Pause,
}

/// A unified signal request that covers both cancel and pause.
///
/// Old `CancellationRequest`/`PauseRequest` types remain for snapshot state
/// serialization. `SignalRequest` is the type used by the `SignalStore` trait.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalRequest {
    /// Optional reason for the signal.
    pub reason: Option<String>,
    /// Optional identifier of who sent the signal.
    pub requested_by: Option<String>,
    /// Timestamp when the signal was sent.
    pub requested_at: DateTime<Utc>,
}

impl SignalRequest {
    /// Create a new signal request with the current timestamp.
    #[must_use]
    pub fn new(reason: Option<String>, requested_by: Option<String>) -> Self {
        Self {
            reason,
            requested_by,
            requested_at: Utc::now(),
        }
    }
}

impl From<CancellationRequest> for SignalRequest {
    fn from(r: CancellationRequest) -> Self {
        Self {
            reason: r.reason,
            requested_by: r.requested_by,
            requested_at: r.requested_at,
        }
    }
}

impl From<PauseRequest> for SignalRequest {
    fn from(r: PauseRequest) -> Self {
        Self {
            reason: r.reason,
            requested_by: r.requested_by,
            requested_at: r.requested_at,
        }
    }
}

impl From<SignalRequest> for CancellationRequest {
    fn from(r: SignalRequest) -> Self {
        Self {
            reason: r.reason,
            requested_by: r.requested_by,
            requested_at: r.requested_at,
        }
    }
}

impl From<SignalRequest> for PauseRequest {
    fn from(r: SignalRequest) -> Self {
        Self {
            reason: r.reason,
            requested_by: r.requested_by,
            requested_at: r.requested_at,
        }
    }
}

/// State of a workflow snapshot.
#[derive(Debug, Clone, Serialize, Deserialize, strum::AsRefStr, strum::EnumDiscriminants)]
#[strum_discriminants(name(SnapshotStatus))]
#[strum_discriminants(derive(strum::EnumString, strum::AsRefStr))]
#[strum_discriminants(
    doc = "Discriminant-only version of [`WorkflowSnapshotState`] for lightweight status checks."
)]
pub enum WorkflowSnapshotState {
    /// Workflow is in progress.
    InProgress {
        /// Current execution position.
        position: ExecutionPosition,
        /// Results from completed tasks (by task ID).
        completed_tasks: HashMap<crate::TaskId, TaskResult>,
        /// ID of the last completed task (for deterministic resume).
        /// This is needed because `HashMap` iteration order is not guaranteed.
        last_completed_task_id: Option<crate::TaskId>,
    },
    /// Workflow completed successfully.
    Completed {
        /// Final output of the workflow.
        final_output: Bytes,
    },
    /// Workflow failed with an error.
    Failed {
        /// Error message.
        error: String,
    },
    /// Workflow was cancelled.
    Cancelled {
        /// Optional reason for the cancellation.
        reason: Option<String>,
        /// Optional identifier of who cancelled the workflow.
        cancelled_by: Option<String>,
        /// Timestamp when the workflow was cancelled.
        cancelled_at: DateTime<Utc>,
        /// Results from tasks that completed before cancellation.
        completed_tasks: HashMap<crate::TaskId, TaskResult>,
        /// The task ID that was interrupted (if any).
        interrupted_at_task: Option<crate::TaskId>,
    },
    /// Workflow was paused. Unlike Cancelled, this preserves position and
    /// `last_completed_task_id` so that the workflow can resume from exactly
    /// where it stopped.
    Paused {
        /// Optional reason for the pause.
        reason: Option<String>,
        /// Optional identifier of who paused the workflow.
        paused_by: Option<String>,
        /// Timestamp when the workflow was paused.
        paused_at: DateTime<Utc>,
        /// Results from tasks that completed before pausing.
        completed_tasks: HashMap<crate::TaskId, TaskResult>,
        /// Execution position at the time of pause (for exact resume).
        position: ExecutionPosition,
        /// ID of the last completed task (for deterministic resume).
        last_completed_task_id: Option<crate::TaskId>,
    },
}

impl WorkflowSnapshotState {
    /// Check if the workflow is completed.
    pub fn is_completed(&self) -> bool {
        matches!(self, WorkflowSnapshotState::Completed { .. })
    }

    /// Check if the workflow has failed.
    pub fn is_failed(&self) -> bool {
        matches!(self, WorkflowSnapshotState::Failed { .. })
    }

    /// Check if the workflow is still in progress.
    pub fn is_in_progress(&self) -> bool {
        matches!(self, WorkflowSnapshotState::InProgress { .. })
    }

    /// Check if the workflow was cancelled.
    pub fn is_cancelled(&self) -> bool {
        matches!(self, WorkflowSnapshotState::Cancelled { .. })
    }

    /// Check if the workflow is paused.
    pub fn is_paused(&self) -> bool {
        matches!(self, WorkflowSnapshotState::Paused { .. })
    }

    /// Check if the workflow is in a terminal state (completed, failed, or cancelled).
    /// Note: Paused is NOT terminal — the workflow can be resumed.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            WorkflowSnapshotState::Completed { .. }
                | WorkflowSnapshotState::Failed { .. }
                | WorkflowSnapshotState::Cancelled { .. }
        )
    }

    /// Convert a terminal state to a [`WorkflowStatus`](crate::workflow::WorkflowStatus).
    ///
    /// Returns `Some(status)` for `Completed`, `Failed`, and `Cancelled` states,
    /// or `None` if still `InProgress`.
    #[must_use]
    pub fn as_terminal_status(&self) -> Option<crate::workflow::WorkflowStatus> {
        use crate::workflow::WorkflowStatus;
        match self {
            Self::Completed { .. } => Some(WorkflowStatus::Completed),
            Self::Failed { error } => Some(WorkflowStatus::Failed(error.clone())),
            Self::Cancelled {
                reason,
                cancelled_by,
                ..
            } => Some(WorkflowStatus::Cancelled {
                reason: reason.clone(),
                cancelled_by: cancelled_by.clone(),
            }),
            Self::Paused {
                reason, paused_by, ..
            } => Some(WorkflowStatus::Paused {
                reason: reason.clone(),
                paused_by: paused_by.clone(),
            }),
            Self::InProgress { .. } => None,
        }
    }

    /// Convert the snapshot state to a [`crate::workflow::WorkflowStatus`], including parked
    /// in-progress positions (delay, signal, fork) instead of collapsing them
    /// to [`crate::workflow::WorkflowStatus::InProgress`].
    #[must_use]
    pub fn as_status(&self) -> crate::workflow::WorkflowStatus {
        use crate::workflow::WorkflowStatus;
        if let Some(terminal) = self.as_terminal_status() {
            return terminal;
        }
        match self {
            Self::InProgress {
                position:
                    ExecutionPosition::AtDelay {
                        wake_at, delay_id, ..
                    },
                ..
            } => WorkflowStatus::Waiting {
                wake_at: *wake_at,
                delay_id: *delay_id,
            },
            Self::InProgress {
                position:
                    ExecutionPosition::AtFork {
                        fork_id, wake_at, ..
                    },
                ..
            } => WorkflowStatus::Waiting {
                wake_at: *wake_at,
                delay_id: *fork_id,
            },
            Self::InProgress {
                position:
                    ExecutionPosition::AtSignal {
                        signal_id,
                        signal_name,
                        wake_at,
                        ..
                    },
                ..
            } => WorkflowStatus::AwaitingSignal {
                signal_id: *signal_id,
                signal_name: signal_name.clone(),
                wake_at: *wake_at,
            },
            _ => WorkflowStatus::InProgress,
        }
    }

    /// Extract the final output if in `Completed` state.
    #[must_use]
    pub fn completed_output(&self) -> Option<&Bytes> {
        if let WorkflowSnapshotState::Completed { final_output } = self {
            Some(final_output)
        } else {
            None
        }
    }

    /// Extract cancellation details if in `Cancelled` state.
    ///
    /// Returns `Some((reason, cancelled_by))` if cancelled, `None` otherwise.
    #[must_use]
    pub fn cancellation_details(&self) -> Option<(Option<String>, Option<String>)> {
        if let WorkflowSnapshotState::Cancelled {
            reason,
            cancelled_by,
            ..
        } = self
        {
            Some((reason.clone(), cancelled_by.clone()))
        } else {
            None
        }
    }

    /// Extract pause details if in `Paused` state.
    ///
    /// Returns `Some((reason, paused_by))` if paused, `None` otherwise.
    #[must_use]
    pub fn pause_details(&self) -> Option<(Option<String>, Option<String>)> {
        if let WorkflowSnapshotState::Paused {
            reason, paused_by, ..
        } = self
        {
            Some((reason.clone(), paused_by.clone()))
        } else {
            None
        }
    }
}

/// A complete snapshot of workflow execution state.
///
/// This captures everything needed to resume a workflow from a checkpoint:
/// - The workflow instance ID
/// - The workflow definition hash (for validation)
/// - The current execution state
/// - All completed task results
/// - The initial input (for resuming from the beginning)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowSnapshot {
    /// Unique identifier for this workflow instance.
    pub instance_id: Arc<str>,
    /// Hash of the workflow definition (for validation).
    pub definition_hash: crate::DefinitionHash,
    /// Current state of execution.
    pub state: WorkflowSnapshotState,
    /// Timestamp when this snapshot was created (Unix timestamp).
    pub created_at: u64,
    /// Timestamp when this snapshot was last updated (Unix timestamp).
    pub updated_at: u64,
    /// Initial input to the workflow (for resuming from start).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub initial_input: Option<Bytes>,
    /// Active task deadline (set when a task with a timeout starts executing).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_deadline: Option<TaskDeadline>,
    /// Retry state for tasks that have failed and are pending retry.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub task_retries: HashMap<crate::TaskId, TaskRetryState>,
    /// Current iteration counts for active loops (keyed by loop ID).
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub loop_iterations: HashMap<crate::TaskId, u32>,
    /// Execution priority of the current task (1–5).
    ///
    /// Set from the continuation tree when advancing to a new task. Used by
    /// persistence backends to order `find_available_tasks` results.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_priority: Option<u8>,
    /// Affinity tags of the current task.
    ///
    /// Set from the continuation tree when advancing to a new task. Used by
    /// persistence backends to filter `find_available_tasks` results by worker tags.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub task_tags: Vec<String>,
    /// W3C `traceparent` header for distributed trace context propagation.
    ///
    /// This is an in-memory carrier — never serialized in the snapshot blob.
    /// Postgres reads/writes it from a dedicated column.
    #[serde(skip)]
    pub trace_parent: Option<String>,
    /// Whether `last_completed_task_id`'s output is still unflushed to the
    /// backend's task sidecar table.
    ///
    /// In-memory only (never serialized): set by [`mark_task_completed`](
    /// Self::mark_task_completed) when a fresh output is produced, and read by
    /// persistence backends to decide whether `save_snapshot` must ship the
    /// output bytes. A freshly-loaded snapshot defaults to `false` — its
    /// outputs were already hydrated from the sidecar, so position-only saves
    /// skip re-shipping the last completed payload on every dispatch tick.
    #[serde(skip)]
    output_unflushed: bool,
}

impl WorkflowSnapshot {
    /// Apply a [`TaskHint`] to this snapshot, setting priority and tags.
    pub fn set_task_hint(&mut self, hint: &TaskHint) {
        self.task_priority = hint.priority;
        self.task_tags.clone_from(&hint.tags);
    }

    /// Get the current Unix timestamp.
    #[allow(clippy::cast_sign_loss)] // Timestamps are always positive
    fn current_timestamp() -> u64 {
        Utc::now().timestamp() as u64
    }

    /// Create a new snapshot for a workflow instance.
    #[must_use]
    pub fn new(instance_id: &str, definition_hash: crate::DefinitionHash) -> Self {
        let instance_id: Arc<str> = Arc::from(instance_id);
        let now = Self::current_timestamp();
        Self {
            instance_id,
            definition_hash,
            state: WorkflowSnapshotState::InProgress {
                position: ExecutionPosition::NotStarted,
                completed_tasks: HashMap::new(),
                last_completed_task_id: None,
            },
            created_at: now,
            updated_at: now,
            initial_input: None,
            task_deadline: None,
            task_retries: HashMap::new(),
            loop_iterations: HashMap::new(),
            task_priority: None,
            task_tags: vec![],
            trace_parent: None,
            output_unflushed: false,
        }
    }

    /// Create a new snapshot with initial input.
    pub fn with_initial_input(
        instance_id: &str,
        definition_hash: crate::DefinitionHash,
        initial_input: Bytes,
    ) -> Self {
        let instance_id: Arc<str> = Arc::from(instance_id);
        let now = Self::current_timestamp();
        Self {
            instance_id,
            definition_hash,
            state: WorkflowSnapshotState::InProgress {
                position: ExecutionPosition::NotStarted,
                completed_tasks: HashMap::new(),
                last_completed_task_id: None,
            },
            created_at: now,
            updated_at: now,
            initial_input: Some(initial_input),
            task_deadline: None,
            task_retries: HashMap::new(),
            loop_iterations: HashMap::new(),
            task_priority: None,
            task_tags: vec![],
            trace_parent: None,
            output_unflushed: false,
        }
    }

    /// Get the initial input as Bytes.
    pub fn initial_input_bytes(&self) -> Option<Bytes> {
        self.initial_input.clone()
    }

    /// Get the result of a completed task, if available.
    pub fn get_task_result(&self, task_id: &crate::TaskId) -> Option<&TaskResult> {
        match &self.state {
            WorkflowSnapshotState::InProgress {
                completed_tasks, ..
            }
            | WorkflowSnapshotState::Cancelled {
                completed_tasks, ..
            }
            | WorkflowSnapshotState::Paused {
                completed_tasks, ..
            } => completed_tasks.get(task_id),
            _ => None,
        }
    }

    /// Get the result of a completed task as Bytes, if available.
    pub fn get_task_result_bytes(&self, task_id: &crate::TaskId) -> Option<Bytes> {
        self.get_task_result(task_id).map(|r| r.output.clone())
    }

    /// Get all completed task results, if the state carries them.
    ///
    /// Returns `Some` for `InProgress`, `Cancelled`, and `Paused` states (which
    /// retain `completed_tasks`), and `None` for `Completed` and `Failed` states
    /// (where task results have been discarded).
    #[must_use]
    pub fn get_all_task_results(&self) -> Option<&HashMap<crate::TaskId, TaskResult>> {
        match &self.state {
            WorkflowSnapshotState::InProgress {
                completed_tasks, ..
            }
            | WorkflowSnapshotState::Cancelled {
                completed_tasks, ..
            }
            | WorkflowSnapshotState::Paused {
                completed_tasks, ..
            } => Some(completed_tasks),
            WorkflowSnapshotState::Completed { .. } | WorkflowSnapshotState::Failed { .. } => None,
        }
    }

    /// Replace every completed task's `output` with empty `Bytes`. Used by
    /// backends that stage outputs in a sidecar table and don't want to
    /// duplicate them in the encoded snapshot blob.
    pub fn strip_task_outputs(&mut self) {
        if let Some(completed) = self.completed_tasks_mut() {
            for result in completed.values_mut() {
                result.output = Bytes::new();
            }
        }
    }

    /// Move every completed task's `output` out of the snapshot, leaving
    /// each one empty, and return them keyed by task id.
    ///
    /// Pairs with [`hydrate_task_outputs`](Self::hydrate_task_outputs) to
    /// restore: `take → encode → hydrate`. This lets a sidecar-staging
    /// backend encode the outputs-stripped blob without cloning the whole
    /// snapshot — the moves are cheap (no `Bytes` refcount churn) and the
    /// snapshot is left logically unchanged once the outputs are hydrated
    /// back. Empty outputs are skipped, so a round-trip with nothing to
    /// strip allocates nothing.
    #[must_use]
    pub fn take_task_outputs(&mut self) -> Vec<(crate::TaskId, Bytes)> {
        let Some(completed) = self.completed_tasks_mut() else {
            return Vec::new();
        };
        let mut taken = Vec::with_capacity(completed.len());
        for result in completed.values_mut() {
            let output = std::mem::take(&mut result.output);
            if !output.is_empty() {
                taken.push((result.task_id, output));
            }
        }
        taken
    }

    /// Patch outputs back onto completed-task results from an external
    /// source (e.g. the `workflow_tasks` sidecar table). Outputs for
    /// task IDs not currently in `completed_tasks` are silently
    /// dropped — the snapshot owns the canonical set of completed tasks.
    pub fn hydrate_task_outputs<I>(&mut self, outputs: I)
    where
        I: IntoIterator<Item = (crate::TaskId, Bytes)>,
    {
        let Some(completed) = self.completed_tasks_mut() else {
            return;
        };
        for (task_id, output) in outputs {
            if let Some(result) = completed.get_mut(&task_id) {
                result.output = output;
            }
        }
    }

    fn completed_tasks_mut(&mut self) -> Option<&mut HashMap<crate::TaskId, TaskResult>> {
        match &mut self.state {
            WorkflowSnapshotState::InProgress {
                completed_tasks, ..
            }
            | WorkflowSnapshotState::Cancelled {
                completed_tasks, ..
            }
            | WorkflowSnapshotState::Paused {
                completed_tasks, ..
            } => Some(completed_tasks),
            WorkflowSnapshotState::Completed { .. } | WorkflowSnapshotState::Failed { .. } => None,
        }
    }

    /// Mark a task as completed and store its result.
    pub fn mark_task_completed(&mut self, task_id: crate::TaskId, output: Bytes) {
        if let WorkflowSnapshotState::InProgress {
            completed_tasks,
            last_completed_task_id,
            ..
        } = &mut self.state
        {
            completed_tasks.insert(task_id, TaskResult { task_id, output });
            *last_completed_task_id = Some(task_id);
            self.updated_at = Self::current_timestamp();
            // The new output has not yet reached the backend's task sidecar;
            // the next `save_snapshot` must ship it. See `output_unflushed`.
            self.output_unflushed = true;
        }
    }

    /// Whether the last completed task's output still needs flushing to the
    /// backend's task sidecar table. See [`output_unflushed`](
    /// Self::output_unflushed) (the field) for the full contract.
    #[must_use]
    pub fn output_unflushed(&self) -> bool {
        self.output_unflushed
    }

    /// Clear the [`output_unflushed`](Self::output_unflushed) marker.
    ///
    /// Called by a persistence backend once it has durably written the
    /// last completed task's output, so a subsequent save of the same
    /// in-memory snapshot doesn't re-ship bytes that are already persisted.
    pub fn mark_output_flushed(&mut self) {
        self.output_unflushed = false;
    }

    /// ID of the most recently completed task, if any. Set by
    /// [`mark_task_completed`] and surfaced here so persistence
    /// backends can write the just-completed output without scanning
    /// the entire `completed_tasks` map.
    #[must_use]
    pub fn last_completed_task_id(&self) -> Option<crate::TaskId> {
        match &self.state {
            WorkflowSnapshotState::InProgress {
                last_completed_task_id,
                ..
            }
            | WorkflowSnapshotState::Paused {
                last_completed_task_id,
                ..
            } => *last_completed_task_id,
            _ => None,
        }
    }

    /// Get the last completed task's output, if any.
    pub fn get_last_task_output(&self) -> Option<Bytes> {
        match &self.state {
            WorkflowSnapshotState::InProgress {
                completed_tasks,
                last_completed_task_id,
                ..
            } => last_completed_task_id
                .and_then(|id| completed_tasks.get(&id))
                .map(|r| r.output.clone()),
            _ => None,
        }
    }

    /// Get the final output as Bytes if completed.
    pub fn final_output_bytes(&self) -> Option<Bytes> {
        match &self.state {
            WorkflowSnapshotState::Completed { final_output } => Some(final_output.clone()),
            _ => None,
        }
    }

    /// Update the execution position.
    pub fn update_position(&mut self, position: ExecutionPosition) {
        if let WorkflowSnapshotState::InProgress { position: pos, .. } = &mut self.state {
            *pos = position;
            self.updated_at = Self::current_timestamp();
        }
    }

    /// Set a task deadline from a timeout duration.
    ///
    /// Computes `deadline = Utc::now() + timeout` and stores it in the snapshot.
    #[allow(clippy::cast_possible_truncation)]
    pub fn set_task_deadline(&mut self, task_id: crate::TaskId, timeout: std::time::Duration) {
        let deadline =
            Utc::now() + chrono::Duration::from_std(timeout).unwrap_or(chrono::Duration::MAX);
        self.task_deadline = Some(TaskDeadline {
            task_id,
            deadline,
            timeout_ms: timeout.as_millis() as u64,
        });
    }

    /// Recompute the deadline to `Utc::now() + timeout_ms`.
    ///
    /// Call this right before actual task execution so the deadline reflects
    /// the true execution start, not the earlier moment when the deadline was
    /// first persisted (which includes snapshot-save I/O overhead).
    #[allow(clippy::cast_possible_wrap)]
    pub fn refresh_task_deadline(&mut self) {
        if let Some(d) = &mut self.task_deadline {
            let timeout = chrono::Duration::milliseconds(d.timeout_ms as i64);
            d.deadline = Utc::now() + timeout;
        }
    }

    /// Clear the active task deadline.
    pub fn clear_task_deadline(&mut self) {
        self.task_deadline = None;
    }

    /// If the persisted deadline has expired, return `(task_id, timeout)`.
    ///
    /// Returns `None` if no deadline is set or it hasn't expired yet.
    pub fn expired_task_deadline(&self) -> Option<(crate::TaskId, std::time::Duration)> {
        self.task_deadline.as_ref().and_then(|d| {
            if Utc::now() >= d.deadline {
                Some((d.task_id, std::time::Duration::from_millis(d.timeout_ms)))
            } else {
                None
            }
        })
    }

    /// Record a retry attempt for a failed task.
    ///
    /// Upserts the retry state entry, increments the attempt counter, and
    /// computes `next_retry_at` via exponential backoff:
    /// `delay = initial_delay * backoff_multiplier^(attempt - 1)`
    ///
    /// Returns the scheduled `next_retry_at` time.
    #[allow(clippy::cast_possible_truncation)]
    pub fn record_retry(
        &mut self,
        task_id: crate::TaskId,
        policy: &RetryPolicy,
        error: &str,
        worker_id: Option<&str>,
    ) -> DateTime<Utc> {
        let entry = self
            .task_retries
            .entry(task_id)
            .or_insert_with(|| TaskRetryState {
                attempts: 0,
                policy: policy.clone(),
                last_error: String::new(),
                last_failed_worker: None,
                next_retry_at: Utc::now(),
            });
        entry.attempts += 1;
        entry.last_error = error.to_string();
        entry.last_failed_worker = worker_id.map(ToString::to_string);
        entry.policy = policy.clone();

        let exponent = entry.attempts.saturating_sub(1);
        #[allow(clippy::cast_possible_wrap)]
        let multiplier = policy.backoff_multiplier.powi(exponent as i32);
        #[allow(clippy::cast_precision_loss)]
        let delay_ms = (policy.initial_delay.as_millis() as f64 * f64::from(multiplier)) as i64;
        let delay = chrono::Duration::milliseconds(delay_ms);
        entry.next_retry_at = Utc::now() + delay;

        self.updated_at = Self::current_timestamp();
        entry.next_retry_at
    }

    /// Get the retry state for a task, if any.
    #[must_use]
    pub fn get_retry_state(&self, task_id: &crate::TaskId) -> Option<&TaskRetryState> {
        self.task_retries.get(task_id)
    }

    /// Clear retry state for a task (e.g. on success).
    pub fn clear_retry_state(&mut self, task_id: &crate::TaskId) {
        self.task_retries.remove(task_id);
    }

    /// Check whether retries are exhausted for a task.
    ///
    /// Returns `true` if retry state exists and `attempts >= policy.max_retries`.
    #[must_use]
    pub fn retries_exhausted(&self, task_id: &crate::TaskId) -> bool {
        self.task_retries
            .get(task_id)
            .is_some_and(|rs| rs.attempts >= rs.policy.max_retries)
    }

    /// Get the current iteration for a loop, defaulting to 0.
    #[must_use]
    pub fn loop_iteration(&self, loop_id: &crate::TaskId) -> u32 {
        self.loop_iterations.get(loop_id).copied().unwrap_or(0)
    }

    /// Set the current iteration for a loop.
    pub fn set_loop_iteration(&mut self, loop_id: crate::TaskId, iteration: u32) {
        self.loop_iterations.insert(loop_id, iteration);
        self.updated_at = Self::current_timestamp();
    }

    /// Clear loop iteration tracking (called when a loop completes).
    pub fn clear_loop_iteration(&mut self, loop_id: &crate::TaskId) {
        self.loop_iterations.remove(loop_id);
    }

    /// Remove a task result from the completed tasks map.
    ///
    /// Used by loop execution to clear body task results between iterations
    /// so the body re-executes on the next iteration.
    pub fn remove_task_result(&mut self, task_id: &crate::TaskId) {
        if let WorkflowSnapshotState::InProgress {
            completed_tasks, ..
        } = &mut self.state
        {
            completed_tasks.remove(task_id);
            self.updated_at = Self::current_timestamp();
        }
    }

    /// Mark the workflow as completed with a final output.
    pub fn mark_completed(&mut self, final_output: Bytes) {
        self.task_deadline = None;
        self.task_retries.clear();
        self.loop_iterations.clear();
        self.state = WorkflowSnapshotState::Completed { final_output };
        self.updated_at = Self::current_timestamp();
    }

    /// Mark the workflow as failed with an error.
    pub fn mark_failed(&mut self, error: String) {
        self.task_deadline = None;
        self.task_retries.clear();
        self.loop_iterations.clear();
        self.state = WorkflowSnapshotState::Failed { error };
        self.updated_at = Self::current_timestamp();
    }

    /// Mark the workflow as paused.
    ///
    /// This preserves the completed tasks, position, and `last_completed_task_id`
    /// from the current `InProgress` state so that the workflow can resume from
    /// exactly where it stopped.
    pub fn mark_paused(&mut self, request: &PauseRequest) {
        if let WorkflowSnapshotState::InProgress {
            position,
            completed_tasks,
            last_completed_task_id,
        } = &self.state
        {
            self.task_deadline = None;
            self.state = WorkflowSnapshotState::Paused {
                reason: request.reason.clone(),
                paused_by: request.requested_by.clone(),
                paused_at: Utc::now(),
                completed_tasks: completed_tasks.clone(),
                position: position.clone(),
                last_completed_task_id: *last_completed_task_id,
            };
            self.updated_at = Self::current_timestamp();
        }
    }

    /// Transition from Paused back to `InProgress`, restoring position and
    /// `completed_tasks` so execution can continue.
    pub fn mark_unpaused(&mut self) {
        if let WorkflowSnapshotState::Paused {
            completed_tasks,
            position,
            last_completed_task_id,
            ..
        } = &self.state
        {
            self.state = WorkflowSnapshotState::InProgress {
                position: position.clone(),
                completed_tasks: completed_tasks.clone(),
                last_completed_task_id: *last_completed_task_id,
            };
            self.updated_at = Self::current_timestamp();
        }
    }

    /// Mark the workflow as cancelled.
    ///
    /// This preserves the completed tasks from the current state.
    pub fn mark_cancelled(
        &mut self,
        reason: Option<String>,
        cancelled_by: Option<String>,
        interrupted_at_task: Option<crate::TaskId>,
    ) {
        let completed_tasks = match &self.state {
            WorkflowSnapshotState::InProgress {
                completed_tasks, ..
            } => completed_tasks.clone(),
            _ => HashMap::new(),
        };

        self.task_deadline = None;
        self.state = WorkflowSnapshotState::Cancelled {
            reason,
            cancelled_by,
            cancelled_at: Utc::now(),
            completed_tasks,
            interrupted_at_task,
        };
        self.updated_at = Self::current_timestamp();
    }

    /// The task priority of the current task, defaulting to 3 (Normal).
    #[must_use]
    pub fn current_task_priority(&self) -> u8 {
        self.task_priority.unwrap_or(3)
    }

    /// The affinity tags of the current task.
    #[must_use]
    pub fn current_task_tags(&self) -> &[String] {
        &self.task_tags
    }

    /// Returns `true` if the given task last failed on the specified worker.
    ///
    /// Used as a soft bias to prefer a different worker on retry.
    #[must_use]
    pub fn has_failed_on_worker(&self, task_id: &crate::TaskId, worker_id: &str) -> bool {
        self.task_retries
            .get(task_id)
            .and_then(|rs| rs.last_failed_worker.as_deref())
            .is_some_and(|w| w == worker_id)
    }

    // --- Persistence helpers (database column extraction) ---

    /// The current task ID, if the workflow is at a task position.
    #[must_use]
    pub fn current_task_id(&self) -> Option<crate::TaskId> {
        match &self.state {
            WorkflowSnapshotState::InProgress {
                position: ExecutionPosition::AtTask { task_id },
                ..
            } => Some(*task_id),
            _ => None,
        }
    }

    /// The count of completed tasks in the snapshot.
    #[must_use]
    pub fn completed_task_count(&self) -> i32 {
        match &self.state {
            WorkflowSnapshotState::InProgress {
                completed_tasks, ..
            }
            | WorkflowSnapshotState::Cancelled {
                completed_tasks, ..
            }
            | WorkflowSnapshotState::Paused {
                completed_tasks, ..
            } => completed_tasks.len().try_into().unwrap_or(i32::MAX),
            _ => 0,
        }
    }

    /// The error message, if the workflow is in a failed state.
    #[must_use]
    pub fn error_message(&self) -> Option<&str> {
        match &self.state {
            WorkflowSnapshotState::Failed { error } => Some(error.as_str()),
            _ => None,
        }
    }

    /// The position kind string (e.g. `"AtTask"`, `"AtDelay"`), if applicable.
    ///
    /// The string is the [`ExecutionPosition`] variant name as written
    /// (derived from `strum::AsRefStr` with no `serialize_all`). Backends
    /// persist this into the `position_kind` column and may filter on it —
    /// see the pin test in the `tests` module before changing the strum
    /// derive.
    ///
    /// Returns `None` for terminal states (Completed, Failed, Cancelled).
    #[must_use]
    pub fn position_kind(&self) -> Option<&str> {
        match &self.state {
            WorkflowSnapshotState::InProgress { position, .. }
            | WorkflowSnapshotState::Paused { position, .. } => Some(position.as_ref()),
            _ => None,
        }
    }

    /// The name of the signal a workflow is currently waiting on, if it is
    /// parked at an `AtSignal` position. `None` otherwise.
    ///
    /// Backends denormalize this into an `awaited_signal_name` column so
    /// the `resumeAll` "signalled" pickup branch can filter buffered events
    /// by (`instance_id`, `signal_name`) — without it, an event delivered
    /// for a different signal would keep re-resuming the workflow
    /// indefinitely.
    #[must_use]
    pub fn awaited_signal_name(&self) -> Option<&str> {
        match &self.state {
            WorkflowSnapshotState::InProgress {
                position: ExecutionPosition::AtSignal { signal_name, .. },
                ..
            } => Some(signal_name.as_str()),
            _ => None,
        }
    }

    /// The wake-at time when the workflow can next make progress, if it is
    /// parked at a delay, timed signal, or fork-with-delayed-branch.
    ///
    /// Backends promote this to a `delay_wake_at` column so dashboards and
    /// cron sweepers (e.g. `Engine.resumeAll`) can find ready instances by a
    /// simple `delay_wake_at <= now()` filter rather than walking the data
    /// blob.
    #[must_use]
    pub fn delay_wake_at(&self) -> Option<DateTime<Utc>> {
        match &self.state {
            WorkflowSnapshotState::InProgress {
                position: ExecutionPosition::AtDelay { wake_at, .. },
                ..
            }
            | WorkflowSnapshotState::InProgress {
                position: ExecutionPosition::AtFork { wake_at, .. },
                ..
            }
            | WorkflowSnapshotState::InProgress {
                position:
                    ExecutionPosition::AtSignal {
                        wake_at: Some(wake_at),
                        ..
                    },
                ..
            } => Some(*wake_at),
            _ => None,
        }
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn test_new_snapshot_in_progress() {
        let snapshot = WorkflowSnapshot::new("inst-1", "hash-1".into());
        assert_eq!(&*snapshot.instance_id, "inst-1");
        assert_eq!(
            snapshot.definition_hash,
            crate::DefinitionHash::from("hash-1")
        );
        assert!(snapshot.state.is_in_progress());
        assert!(!snapshot.state.is_terminal());
        assert!(snapshot.initial_input.is_none());
    }

    /// Pin the `ExecutionPosition` variant strings consumed by the
    /// `position_kind` column in persistence backends.
    ///
    /// `sayiir-d1::SQLiteBackend::find_resumable_instances` filters on these
    /// literals (e.g. `position_kind IN ('AtTask', 'AtJoin', 'InLoop',
    /// 'NotStarted')`). Renaming a variant — or adding `#[strum(serialize_all
    /// = ...)]` to `ExecutionPosition` — would silently change the persisted
    /// value and make the stale-pickup branch match nothing on existing rows.
    /// Update both the SQL and the pinned strings together.
    #[test]
    fn execution_position_kind_strings_are_stable() {
        use std::collections::HashMap;
        let cases: [(ExecutionPosition, &str); 7] = [
            (ExecutionPosition::NotStarted, "NotStarted"),
            (
                ExecutionPosition::AtTask {
                    task_id: crate::TaskId::from("t"),
                },
                "AtTask",
            ),
            (
                ExecutionPosition::AtFork {
                    fork_id: crate::TaskId::from("f"),
                    completed_branches: HashMap::new(),
                    wake_at: chrono::Utc::now(),
                },
                "AtFork",
            ),
            (
                ExecutionPosition::AtJoin {
                    join_id: crate::TaskId::from("j"),
                    completed_branches: HashMap::new(),
                },
                "AtJoin",
            ),
            (
                ExecutionPosition::AtDelay {
                    delay_id: crate::TaskId::from("d"),
                    entered_at: chrono::Utc::now(),
                    wake_at: chrono::Utc::now(),
                    next_task_id: None,
                },
                "AtDelay",
            ),
            (
                ExecutionPosition::AtSignal {
                    signal_id: crate::TaskId::from("s"),
                    signal_name: "n".into(),
                    wake_at: None,
                    next_task_id: None,
                },
                "AtSignal",
            ),
            (
                ExecutionPosition::InLoop {
                    loop_id: crate::TaskId::from("l"),
                    iteration: 0,
                    next_task_id: None,
                },
                "InLoop",
            ),
        ];
        for (variant, expected) in &cases {
            assert_eq!(variant.as_ref(), *expected);
        }
    }

    #[test]
    fn test_snapshot_with_initial_input() {
        let snapshot =
            WorkflowSnapshot::with_initial_input("inst-1", "hash-1".into(), Bytes::from("hello"));
        assert_eq!(snapshot.initial_input_bytes(), Some(Bytes::from("hello")));
        assert!(snapshot.state.is_in_progress());
    }

    #[test]
    fn test_mark_task_completed_and_retrieve() {
        let mut snapshot = WorkflowSnapshot::new("inst-1", "hash-1".into());
        snapshot.mark_task_completed(crate::TaskId::from("task-1"), Bytes::from("output1"));

        let result = snapshot.get_task_result(&crate::TaskId::from("task-1"));
        assert!(result.is_some());
        assert_eq!(result.unwrap().output, Bytes::from("output1"));
        assert_eq!(result.unwrap().task_id, crate::TaskId::from("task-1"));
    }

    #[test]
    fn test_take_and_restore_task_outputs() {
        let mut snapshot = WorkflowSnapshot::new("inst-1", "hash-1".into());
        snapshot.mark_task_completed(crate::TaskId::from("t1"), Bytes::from("out1"));
        snapshot.mark_task_completed(crate::TaskId::from("t2"), Bytes::from("out2"));

        // Take moves outputs out, leaving the completed-task entries empty.
        let taken = snapshot.take_task_outputs();
        assert_eq!(taken.len(), 2);
        assert!(
            snapshot
                .get_task_result(&crate::TaskId::from("t1"))
                .unwrap()
                .output
                .is_empty()
        );

        // Hydrating the taken outputs back restores the snapshot exactly.
        snapshot.hydrate_task_outputs(taken);
        assert_eq!(
            snapshot.get_task_result_bytes(&crate::TaskId::from("t1")),
            Some(Bytes::from("out1"))
        );
        assert_eq!(
            snapshot.get_task_result_bytes(&crate::TaskId::from("t2")),
            Some(Bytes::from("out2"))
        );

        // Nothing left to take after outputs are already empty.
        snapshot.strip_task_outputs();
        assert!(snapshot.take_task_outputs().is_empty());
    }

    #[test]
    fn test_output_unflushed_marker() {
        // Fresh snapshot: nothing completed, nothing to flush.
        let mut snapshot = WorkflowSnapshot::new("inst-1", "hash-1".into());
        assert!(!snapshot.output_unflushed());

        // Completing a task flags its output as needing a flush.
        snapshot.mark_task_completed(crate::TaskId::from("task-1"), Bytes::from("out1"));
        assert!(snapshot.output_unflushed());

        // The marker is in-memory only: a decoded snapshot (serde-skipped
        // field) defaults to clean, since its outputs were already persisted
        // and are hydrated from the sidecar on load.
        let json = serde_json::to_string(&snapshot).unwrap();
        let decoded: WorkflowSnapshot = serde_json::from_str(&json).unwrap();
        assert!(!decoded.output_unflushed());
    }

    #[test]
    fn test_get_last_task_output() {
        let mut snapshot = WorkflowSnapshot::new("inst-1", "hash-1".into());
        assert!(snapshot.get_last_task_output().is_none());

        snapshot.mark_task_completed(crate::TaskId::from("task-1"), Bytes::from("out1"));
        assert_eq!(snapshot.get_last_task_output(), Some(Bytes::from("out1")));

        snapshot.mark_task_completed(crate::TaskId::from("task-2"), Bytes::from("out2"));
        assert_eq!(snapshot.get_last_task_output(), Some(Bytes::from("out2")));
    }

    #[test]
    fn test_get_task_result_bytes() {
        let mut snapshot = WorkflowSnapshot::new("inst-1", "hash-1".into());
        snapshot.mark_task_completed(crate::TaskId::from("task-1"), Bytes::from("data"));
        assert_eq!(
            snapshot.get_task_result_bytes(&crate::TaskId::from("task-1")),
            Some(Bytes::from("data"))
        );
        assert!(
            snapshot
                .get_task_result_bytes(&crate::TaskId::from("nonexistent"))
                .is_none()
        );
    }

    #[test]
    fn test_mark_completed() {
        let mut snapshot = WorkflowSnapshot::new("inst-1", "hash-1".into());
        snapshot.mark_completed(Bytes::from("final"));
        assert!(snapshot.state.is_completed());
        assert!(snapshot.state.is_terminal());
        assert!(!snapshot.state.is_in_progress());
        assert_eq!(snapshot.final_output_bytes(), Some(Bytes::from("final")));
    }

    #[test]
    fn test_mark_failed() {
        let mut snapshot = WorkflowSnapshot::new("inst-1", "hash-1".into());
        snapshot.mark_failed("something went wrong".into());
        assert!(snapshot.state.is_failed());
        assert!(snapshot.state.is_terminal());
        assert!(!snapshot.state.is_in_progress());
        assert!(snapshot.final_output_bytes().is_none());
    }

    #[test]
    fn test_mark_cancelled_preserves_completed_tasks() {
        let mut snapshot = WorkflowSnapshot::new("inst-1", "hash-1".into());
        snapshot.mark_task_completed(crate::TaskId::from("task-1"), Bytes::from("output1"));
        snapshot.mark_cancelled(
            Some("user request".into()),
            Some("admin".into()),
            Some("task-2".into()),
        );

        assert!(snapshot.state.is_cancelled());
        assert!(snapshot.state.is_terminal());
        // Completed tasks should be preserved in cancelled state
        assert!(
            snapshot
                .get_task_result(&crate::TaskId::from("task-1"))
                .is_some()
        );
    }

    #[test]
    fn test_cancellation_details() {
        let mut snapshot = WorkflowSnapshot::new("inst-1", "hash-1".into());
        assert!(snapshot.state.cancellation_details().is_none());

        snapshot.mark_cancelled(Some("timeout".into()), Some("system".into()), None);
        let details = snapshot.state.cancellation_details();
        assert!(details.is_some());
        let (reason, by) = details.unwrap();
        assert_eq!(reason, Some("timeout".into()));
        assert_eq!(by, Some("system".into()));
    }

    #[test]
    fn test_update_position() {
        let mut snapshot = WorkflowSnapshot::new("inst-1", "hash-1".into());
        snapshot.update_position(ExecutionPosition::AtTask {
            task_id: crate::TaskId::from("task-1"),
        });
        match &snapshot.state {
            WorkflowSnapshotState::InProgress { position, .. } => match position {
                ExecutionPosition::AtTask { task_id } => {
                    assert_eq!(*task_id, crate::TaskId::from("task-1"));
                }
                _ => panic!("Expected AtTask"),
            },
            _ => panic!("Expected InProgress"),
        }
    }

    #[test]
    fn test_update_position_noop_on_terminal() {
        let mut snapshot = WorkflowSnapshot::new("inst-1", "hash-1".into());
        snapshot.mark_completed(Bytes::from("done"));
        snapshot.update_position(ExecutionPosition::AtTask {
            task_id: crate::TaskId::from("task-1"),
        });
        // Position update should be a no-op on completed state
        assert!(snapshot.state.is_completed());
    }

    #[test]
    fn test_mark_task_completed_noop_on_terminal() {
        let mut snapshot = WorkflowSnapshot::new("inst-1", "hash-1".into());
        snapshot.mark_completed(Bytes::from("done"));
        snapshot.mark_task_completed(crate::TaskId::from("task-1"), Bytes::from("output"));
        // Should not crash, just be a no-op
        assert!(snapshot.state.is_completed());
    }

    #[test]
    fn test_get_task_result_on_completed_state() {
        let mut snapshot = WorkflowSnapshot::new("inst-1", "hash-1".into());
        snapshot.mark_completed(Bytes::from("done"));
        assert!(
            snapshot
                .get_task_result(&crate::TaskId::from("task-1"))
                .is_none()
        );
    }

    #[test]
    fn test_get_task_result_on_failed_state() {
        let mut snapshot = WorkflowSnapshot::new("inst-1", "hash-1".into());
        snapshot.mark_failed("err".into());
        assert!(
            snapshot
                .get_task_result(&crate::TaskId::from("task-1"))
                .is_none()
        );
    }

    #[test]
    fn test_get_all_task_results_in_progress() {
        let mut snapshot = WorkflowSnapshot::new("inst-1", "hash-1".into());
        snapshot.mark_task_completed(crate::TaskId::from("task-1"), Bytes::from("out1"));
        snapshot.mark_task_completed(crate::TaskId::from("task-2"), Bytes::from("out2"));

        let results = snapshot.get_all_task_results();
        assert!(results.is_some());
        let results = results.unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(
            results[&crate::TaskId::from("task-1")].output,
            Bytes::from("out1")
        );
        assert_eq!(
            results[&crate::TaskId::from("task-2")].output,
            Bytes::from("out2")
        );
    }

    #[test]
    fn test_get_all_task_results_cancelled() {
        let mut snapshot = WorkflowSnapshot::new("inst-1", "hash-1".into());
        snapshot.mark_task_completed(crate::TaskId::from("task-1"), Bytes::from("out1"));
        snapshot.mark_cancelled(Some("reason".into()), None, Some("task-2".into()));

        let results = snapshot.get_all_task_results();
        assert!(results.is_some());
        assert_eq!(results.unwrap().len(), 1);
        assert_eq!(
            results.unwrap()[&crate::TaskId::from("task-1")].output,
            Bytes::from("out1")
        );
    }

    #[test]
    fn test_get_all_task_results_paused() {
        let mut snapshot = WorkflowSnapshot::new("inst-1", "hash-1".into());
        snapshot.mark_task_completed(crate::TaskId::from("task-1"), Bytes::from("out1"));
        let request = PauseRequest::new(Some("maintenance".into()), None);
        snapshot.mark_paused(&request);

        let results = snapshot.get_all_task_results();
        assert!(results.is_some());
        assert_eq!(results.unwrap().len(), 1);
    }

    #[test]
    fn test_get_all_task_results_completed() {
        let mut snapshot = WorkflowSnapshot::new("inst-1", "hash-1".into());
        snapshot.mark_task_completed(crate::TaskId::from("task-1"), Bytes::from("out1"));
        snapshot.mark_completed(Bytes::from("final"));
        assert!(snapshot.get_all_task_results().is_none());
    }

    #[test]
    fn test_get_all_task_results_failed() {
        let mut snapshot = WorkflowSnapshot::new("inst-1", "hash-1".into());
        snapshot.mark_task_completed(crate::TaskId::from("task-1"), Bytes::from("out1"));
        snapshot.mark_failed("error".into());
        assert!(snapshot.get_all_task_results().is_none());
    }

    #[test]
    fn test_state_transitions() {
        let state = WorkflowSnapshotState::InProgress {
            position: ExecutionPosition::NotStarted,
            completed_tasks: HashMap::new(),
            last_completed_task_id: None,
        };
        assert!(state.is_in_progress());
        assert!(!state.is_completed());
        assert!(!state.is_failed());
        assert!(!state.is_cancelled());
        assert!(!state.is_terminal());

        let state = WorkflowSnapshotState::Completed {
            final_output: Bytes::new(),
        };
        assert!(!state.is_in_progress());
        assert!(state.is_completed());
        assert!(state.is_terminal());

        let state = WorkflowSnapshotState::Failed {
            error: "err".into(),
        };
        assert!(state.is_failed());
        assert!(state.is_terminal());

        let state = WorkflowSnapshotState::Cancelled {
            reason: None,
            cancelled_by: None,
            cancelled_at: Utc::now(),
            completed_tasks: HashMap::new(),
            interrupted_at_task: None,
        };
        assert!(state.is_cancelled());
        assert!(state.is_terminal());
    }

    #[test]
    fn test_timestamps_are_set() {
        let snapshot = WorkflowSnapshot::new("inst-1", "hash-1".into());
        assert!(snapshot.created_at > 0);
        assert!(snapshot.updated_at > 0);
        assert_eq!(snapshot.created_at, snapshot.updated_at);
    }

    // ========================================================================
    // From conversion tests
    // ========================================================================

    #[test]
    fn test_cancellation_request_to_signal_request() {
        let cr = CancellationRequest::new(Some("timeout".into()), Some("admin".into()));
        let ts = cr.requested_at;
        let sr: SignalRequest = cr.into();
        assert_eq!(sr.reason, Some("timeout".into()));
        assert_eq!(sr.requested_by, Some("admin".into()));
        assert_eq!(sr.requested_at, ts);
    }

    #[test]
    fn test_pause_request_to_signal_request() {
        let pr = PauseRequest::new(Some("maintenance".into()), Some("ops".into()));
        let ts = pr.requested_at;
        let sr: SignalRequest = pr.into();
        assert_eq!(sr.reason, Some("maintenance".into()));
        assert_eq!(sr.requested_by, Some("ops".into()));
        assert_eq!(sr.requested_at, ts);
    }

    #[test]
    fn test_signal_request_to_cancellation_request() {
        let sr = SignalRequest::new(Some("done".into()), Some("user".into()));
        let ts = sr.requested_at;
        let cr: CancellationRequest = sr.into();
        assert_eq!(cr.reason, Some("done".into()));
        assert_eq!(cr.requested_by, Some("user".into()));
        assert_eq!(cr.requested_at, ts);
    }

    #[test]
    fn test_signal_request_to_pause_request() {
        let sr = SignalRequest::new(Some("scaling".into()), Some("system".into()));
        let ts = sr.requested_at;
        let pr: PauseRequest = sr.into();
        assert_eq!(pr.reason, Some("scaling".into()));
        assert_eq!(pr.requested_by, Some("system".into()));
        assert_eq!(pr.requested_at, ts);
    }

    #[test]
    fn test_cancellation_request_roundtrip() {
        let original = CancellationRequest::new(Some("reason".into()), Some("who".into()));
        let ts = original.requested_at;
        let signal: SignalRequest = original.into();
        let back: CancellationRequest = signal.into();
        assert_eq!(back.reason, Some("reason".into()));
        assert_eq!(back.requested_by, Some("who".into()));
        assert_eq!(back.requested_at, ts);
    }

    #[test]
    fn test_pause_request_roundtrip() {
        let original = PauseRequest::new(Some("reason".into()), Some("who".into()));
        let ts = original.requested_at;
        let signal: SignalRequest = original.into();
        let back: PauseRequest = signal.into();
        assert_eq!(back.reason, Some("reason".into()));
        assert_eq!(back.requested_by, Some("who".into()));
        assert_eq!(back.requested_at, ts);
    }

    #[test]
    fn test_from_conversion_with_none_fields() {
        let sr = SignalRequest::new(None, None);
        let cr: CancellationRequest = sr.into();
        assert!(cr.reason.is_none());
        assert!(cr.requested_by.is_none());
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    /// Strategy for arbitrary `TaskResult`.
    fn arb_task_result() -> impl Strategy<Value = TaskResult> {
        (
            "[a-z0-9]{1,8}",
            proptest::collection::vec(any::<u8>(), 0..32),
        )
            .prop_map(|(task_id, data)| TaskResult {
                task_id: crate::TaskId::from(task_id.as_str()),
                output: Bytes::from(data),
            })
    }

    /// Strategy for arbitrary `HashMap<TaskId, TaskResult>`.
    fn arb_completed_tasks() -> impl Strategy<Value = HashMap<crate::TaskId, TaskResult>> {
        proptest::collection::hash_map(
            "[a-z0-9]{1,8}".prop_map(|s| crate::TaskId::from(s.as_str())),
            arb_task_result(),
            0..4,
        )
    }

    /// Strategy for arbitrary `WorkflowSnapshotState`.
    fn arb_state() -> impl Strategy<Value = WorkflowSnapshotState> {
        prop_oneof![
            // InProgress
            arb_completed_tasks().prop_map(|tasks| {
                WorkflowSnapshotState::InProgress {
                    position: ExecutionPosition::NotStarted,
                    completed_tasks: tasks,
                    last_completed_task_id: None,
                }
            }),
            // Completed
            proptest::collection::vec(any::<u8>(), 0..32).prop_map(|data| {
                WorkflowSnapshotState::Completed {
                    final_output: Bytes::from(data),
                }
            }),
            // Failed
            "[a-zA-Z0-9 ]{0,32}".prop_map(|error| WorkflowSnapshotState::Failed { error }),
            // Cancelled
            (
                prop::option::of("[a-zA-Z0-9 ]{0,32}"),
                prop::option::of("[a-zA-Z0-9 ]{0,32}"),
                arb_completed_tasks(),
                prop::option::of("[a-z0-9]{1,8}".prop_map(|s| crate::TaskId::from(s.as_str()))),
            )
                .prop_map(
                    |(reason, cancelled_by, completed_tasks, interrupted_at_task)| {
                        WorkflowSnapshotState::Cancelled {
                            reason,
                            cancelled_by,
                            cancelled_at: Utc::now(),
                            completed_tasks,
                            interrupted_at_task,
                        }
                    }
                ),
            // Paused
            (
                prop::option::of("[a-zA-Z0-9 ]{0,32}"),
                prop::option::of("[a-zA-Z0-9 ]{0,32}"),
                arb_completed_tasks(),
                prop::option::of("[a-z0-9]{1,8}".prop_map(|s| crate::TaskId::from(s.as_str()))),
            )
                .prop_map(
                    |(reason, paused_by, completed_tasks, last_completed_task_id)| {
                        WorkflowSnapshotState::Paused {
                            reason,
                            paused_by,
                            paused_at: Utc::now(),
                            completed_tasks,
                            position: ExecutionPosition::NotStarted,
                            last_completed_task_id,
                        }
                    }
                ),
        ]
    }

    proptest! {
        // Property 8: Exactly one predicate is true for any state.
        #[test]
        fn exactly_one_predicate_true(state in arb_state()) {
            let flags = [
                state.is_in_progress(),
                state.is_completed(),
                state.is_failed(),
                state.is_cancelled(),
                state.is_paused(),
            ];
            let count = flags.iter().filter(|&&f| f).count();
            prop_assert!(count == 1, "Expected exactly 1 true predicate, got {}: {:?}", count, flags);
        }

        // Property 9: `is_terminal()` is equivalent to completed, failed, or cancelled.
        #[test]
        fn terminal_consistency(state in arb_state()) {
            prop_assert_eq!(
                state.is_terminal(),
                state.is_completed() || state.is_failed() || state.is_cancelled(),
            );
        }

        // Property 10: `as_terminal_status().is_some() == !is_in_progress()`.
        #[test]
        fn as_terminal_status_consistency(state in arb_state()) {
            prop_assert_eq!(
                state.as_terminal_status().is_some(),
                !state.is_in_progress(),
            );
        }
    }
}
