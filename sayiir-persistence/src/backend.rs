//! Persistent backend traits for storing and retrieving workflow snapshots.
//!
//! The trait hierarchy is decomposed into focused sub-traits:
//!
//! - [`SnapshotStore`]: Core CRUD for workflow snapshots (5 methods).
//! - [`SignalStore`]: Cancel + pause signal primitives with default composite
//!   implementations (3 required + 3 default methods).
//! - [`TaskClaimStore`]: Distributed task claiming (4 methods, opt-in).
//! - [`PersistentBackend`]: Supertrait = `SnapshotStore + SignalStore`, blanket-implemented.
//!
//! A minimal backend only needs to implement `SnapshotStore` + 3 `SignalStore` primitives
//! (8 methods total) to satisfy `PersistentBackend`.

use chrono::Duration;
use sayiir_core::snapshot::{
    PauseRequest, SignalKind, SignalRequest, WorkflowSnapshot, WorkflowSnapshotState,
};
use sayiir_core::task_claim::{AvailableTask, TaskClaim};

/// Wire-side representation of a 32-byte SHA-256 hash.
///
/// The semantic newtypes [`sayiir_core::TaskId`] / [`sayiir_core::DefinitionHash`]
/// don't cross this boundary as-is (would couple `sayiir-core` to nanoserde).
/// Producers convert with `*hash.as_bytes()`; consumers wrap back with
/// `TaskId::from_bytes(field)` / `DefinitionHash::from_bytes(field)`. The
/// type-system guarantees live on either side of this alias — over the wire
/// the payload is just 32 raw bytes.
pub type HashBytes = [u8; 32];

/// Routing-and-eligibility hint a producer attaches to a "task ready"
/// wakeup. Workers consume it to:
///
/// * **Filter** — drop the wake without polling if the worker's tags don't
///   match, or if the worker doesn't have the workflow registered. Cuts
///   PG load proportional to NOTIFY volume × fleet-tag-fragmentation.
/// * **Direct-claim** — call [`TaskClaimStore::find_hinted_task`] to skip
///   the full `find_available_tasks` scan on the happy path; the producer
///   already named the task that just became ready.
#[derive(Debug, Clone, PartialEq, Eq, nanoserde::SerBin, nanoserde::DeBin)]
pub struct TaskWakeupHint {
    /// Workflow instance the task belongs to (user-supplied, human-readable
    /// — *not* a hash; this is the same string the user passes to
    /// `runner.run("…")` and that appears in Postgres `instance_id` columns
    /// and log spans).
    pub instance_id: String,
    /// SHA-256 of the task node id — wraps to [`sayiir_core::TaskId`].
    pub task_id: HashBytes,
    /// SHA-256 of the workflow definition — wraps to [`sayiir_core::DefinitionHash`].
    /// Workers without this definition registered drop the wake without
    /// touching the DB.
    pub definition_hash: HashBytes,
    /// Task tags. A worker can handle the task only if its tag set is a
    /// superset of `tags` (untagged tasks are claimable by anyone).
    pub tags: Vec<String>,
}

/// Wire-format version byte prepended to the nanoserde blob. Bump on
/// any breaking change to [`TaskWakeupHint`]'s field layout so decoders
/// reject payloads they don't understand instead of silently
/// misparsing them.
const HINT_WIRE_VERSION: u8 = 2;

impl TaskWakeupHint {
    /// Encode to a base64-wrapped binary blob, suitable for any text-only
    /// transport (e.g. PG's `pg_notify` payload).
    ///
    /// Wire layout: `[version u8][nanoserde SerBin bytes]`. nanoserde
    /// uses length-prefixed (u64 LE) encoding for strings and `Vec`,
    /// little-endian for primitives. Typical hint encodes to ~70–100
    /// bytes raw and ~95–135 bytes base64 — comfortably under PG's
    /// 8 kB `NOTIFY` payload cap.
    #[must_use]
    pub fn encode(&self) -> String {
        use base64::Engine;
        use nanoserde::SerBin;

        let mut buf = Vec::with_capacity(96);
        buf.push(HINT_WIRE_VERSION);
        self.ser_bin(&mut buf);
        base64::engine::general_purpose::STANDARD_NO_PAD.encode(&buf)
    }

    /// Decode from the wire format produced by [`Self::encode`].
    ///
    /// Returns `Err` with a human-readable reason for any corrupt,
    /// truncated, or version-mismatched payload. The caller should log
    /// and treat the failure as a missed wakeup — the fallback poll
    /// will catch up.
    pub fn decode(payload: &str) -> Result<Self, String> {
        use base64::Engine;
        use nanoserde::DeBin;

        let bytes = base64::engine::general_purpose::STANDARD_NO_PAD
            .decode(payload)
            .map_err(|e| format!("base64: {e}"))?;
        let Some((&version, body)) = bytes.split_first() else {
            return Err("empty payload".into());
        };
        if version != HINT_WIRE_VERSION {
            return Err(format!("unsupported wire version: {version}"));
        }
        Self::deserialize_bin(body).map_err(|e| format!("nanoserde: {e}"))
    }
}

#[cfg(test)]
mod hint_tests {
    use super::TaskWakeupHint;

    fn sample() -> TaskWakeupHint {
        TaskWakeupHint {
            instance_id: "wf-abc-123".to_string(),
            task_id: [0x42; 32],
            definition_hash: [0xab; 32],
            tags: vec!["gpu".to_string(), "cuda-12".to_string()],
        }
    }

    #[test]
    fn roundtrip() {
        let hint = sample();
        let encoded = hint.encode();
        let decoded = TaskWakeupHint::decode(&encoded).expect("roundtrip");
        assert_eq!(hint, decoded);
    }

    #[test]
    fn roundtrip_empty_tags() {
        let mut hint = sample();
        hint.tags.clear();
        let decoded = TaskWakeupHint::decode(&hint.encode()).unwrap();
        assert_eq!(hint, decoded);
    }

    #[test]
    fn rejects_garbage_payload() {
        assert!(TaskWakeupHint::decode("not base64!@#").is_err());
        assert!(TaskWakeupHint::decode("").is_err());
    }

    #[test]
    fn rejects_unknown_version() {
        use base64::Engine;
        let buf = vec![99u8, 0, 0, 0, 0, 0, 0, 0, 0];
        let payload = base64::engine::general_purpose::STANDARD_NO_PAD.encode(&buf);
        let err = TaskWakeupHint::decode(&payload).unwrap_err();
        assert!(err.contains("unsupported wire version"));
    }
}

/// Error type for backend operations.
#[derive(Debug, thiserror::Error)]
pub enum BackendError {
    /// Snapshot not found.
    #[error("Snapshot not found: {0}")]
    NotFound(String),
    /// Serialization/deserialization error.
    #[error("Serialization error: {0}")]
    Serialization(String),
    /// Backend-specific error.
    #[error("Backend error: {0}")]
    Backend(String),
    /// Cannot cancel workflow in current state.
    #[error("Cannot cancel workflow in state: {0}")]
    CannotCancel(String),
    /// Cannot pause workflow in current state.
    #[error("Cannot pause workflow in state: {0}")]
    CannotPause(String),
    /// A claim operation found the claim owned by another worker — the
    /// caller's lease is structurally lost (stolen after expiry), not a
    /// transient failure.
    #[error("Claim lost: {0}")]
    ClaimLost(String),
}

// ---------------------------------------------------------------------------
// SnapshotStore — core CRUD, every backend implements this
// ---------------------------------------------------------------------------

/// Core snapshot CRUD operations.
///
/// Every persistent backend must implement these 5 methods.
pub trait SnapshotStore: Send + Sync {
    /// Save a workflow snapshot.
    ///
    /// If a snapshot with the same instance_id already exists, it should be overwritten.
    ///
    /// Takes `&mut` so backends can encode the blob without cloning the
    /// snapshot (strip task outputs in place, encode, restore) and clear
    /// any in-memory "output unflushed" marker once the write lands. The
    /// snapshot is logically unchanged on return.
    fn save_snapshot(
        &self,
        snapshot: &mut WorkflowSnapshot,
    ) -> impl Future<Output = Result<(), BackendError>> + Send;

    /// Save a single task result atomically.
    ///
    /// This is more granular than `save_snapshot` and allows concurrent task
    /// completions (e.g., in fork branches) without overwriting each other.
    fn save_task_result(
        &self,
        instance_id: &str,
        task_id: &sayiir_core::TaskId,
        output: bytes::Bytes,
    ) -> impl Future<Output = Result<(), BackendError>> + Send;

    /// Load a workflow snapshot by instance ID.
    fn load_snapshot(
        &self,
        instance_id: &str,
    ) -> impl Future<Output = Result<WorkflowSnapshot, BackendError>> + Send;

    /// Delete a workflow snapshot.
    fn delete_snapshot(
        &self,
        instance_id: &str,
    ) -> impl Future<Output = Result<(), BackendError>> + Send;

    /// List all snapshot instance IDs.
    fn list_snapshots(&self) -> impl Future<Output = Result<Vec<String>, BackendError>> + Send;
}

// ---------------------------------------------------------------------------
// SignalStore — cancel + pause via SignalKind
// ---------------------------------------------------------------------------

/// Outcome of a combined cancel-then-pause check (see
/// [`SignalStore::check_control_signals`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlSignal {
    /// A pending cancel was found and applied.
    Cancelled,
    /// A pending pause was found and applied.
    Paused,
}

/// Signal storage for cancel and pause workflows.
///
/// Backends implement the 3 primitives (`store_signal`, `get_signal`,
/// `clear_signal`). The 3 composite methods (`check_and_cancel`,
/// `check_and_pause`, `unpause`) have default implementations built from
/// the primitives + `SnapshotStore`. Backends may override them for atomicity.
pub trait SignalStore: SnapshotStore {
    // --- 3 primitives (backend must implement) ---

    /// Store a signal (cancel or pause) for a workflow instance.
    fn store_signal(
        &self,
        instance_id: &str,
        kind: SignalKind,
        request: SignalRequest,
    ) -> impl Future<Output = Result<(), BackendError>> + Send;

    /// Get the pending signal of the given kind, if any.
    fn get_signal(
        &self,
        instance_id: &str,
        kind: SignalKind,
    ) -> impl Future<Output = Result<Option<SignalRequest>, BackendError>> + Send;

    /// Clear the signal of the given kind.
    fn clear_signal(
        &self,
        instance_id: &str,
        kind: SignalKind,
    ) -> impl Future<Output = Result<(), BackendError>> + Send;

    /// Send an external event to a workflow instance.
    ///
    /// Events are buffered per `(instance_id, signal_name)` in FIFO order.
    /// Sending to a nonexistent or terminal instance silently stores the event.
    fn send_event(
        &self,
        instance_id: &str,
        signal_name: &str,
        payload: bytes::Bytes,
    ) -> impl Future<Output = Result<(), BackendError>> + Send;

    /// Consume the oldest buffered event for the given signal name, if any.
    ///
    /// Returns `Some(payload)` if an event was consumed, `None` otherwise.
    fn consume_event(
        &self,
        instance_id: &str,
        signal_name: &str,
    ) -> impl Future<Output = Result<Option<bytes::Bytes>, BackendError>> + Send;

    // --- 3 composites with default impls (overridable for atomicity) ---

    /// Atomically check for cancellation and transition to cancelled state.
    ///
    /// Returns `true` if the workflow was cancelled, `false` if no cancellation
    /// was pending.
    fn check_and_cancel(
        &self,
        instance_id: &str,
        interrupted_at_task: Option<sayiir_core::TaskId>,
    ) -> impl Future<Output = Result<bool, BackendError>> + Send {
        async move {
            let Some(request) = self.get_signal(instance_id, SignalKind::Cancel).await? else {
                return Ok(false);
            };
            let mut snapshot = self.load_snapshot(instance_id).await?;
            if !snapshot.state.is_in_progress() {
                return Ok(false);
            }
            snapshot.mark_cancelled(request.reason, request.requested_by, interrupted_at_task);
            self.save_snapshot(&mut snapshot).await?;
            self.clear_signal(instance_id, SignalKind::Cancel).await?;
            Ok(true)
        }
    }

    /// Atomically check for a pause request and transition to paused state.
    ///
    /// Returns `true` if the workflow was paused, `false` if no pause was pending.
    fn check_and_pause(
        &self,
        instance_id: &str,
    ) -> impl Future<Output = Result<bool, BackendError>> + Send {
        async move {
            let Some(request) = self.get_signal(instance_id, SignalKind::Pause).await? else {
                return Ok(false);
            };
            let mut snapshot = self.load_snapshot(instance_id).await?;
            if !snapshot.state.is_in_progress() {
                return Ok(false);
            }
            let pause_request: PauseRequest = request.into();
            snapshot.mark_paused(&pause_request);
            self.save_snapshot(&mut snapshot).await?;
            self.clear_signal(instance_id, SignalKind::Pause).await?;
            Ok(true)
        }
    }

    /// Check for a pending cancel, then a pending pause, applying the first
    /// one found. Backends can override this to collapse the two checks into
    /// fewer round-trips — the common case on the worker hot path is "no
    /// signal at all", which an override can answer with a single probe.
    fn check_control_signals(
        &self,
        instance_id: &str,
        interrupted_at_task: Option<sayiir_core::TaskId>,
    ) -> impl Future<Output = Result<Option<ControlSignal>, BackendError>> + Send {
        async move {
            if self
                .check_and_cancel(instance_id, interrupted_at_task)
                .await?
            {
                return Ok(Some(ControlSignal::Cancelled));
            }
            if self.check_and_pause(instance_id).await? {
                return Ok(Some(ControlSignal::Paused));
            }
            Ok(None)
        }
    }

    /// Transition a paused workflow back to in-progress and return the updated snapshot.
    fn unpause(
        &self,
        instance_id: &str,
    ) -> impl Future<Output = Result<WorkflowSnapshot, BackendError>> + Send {
        async move {
            let mut snapshot = self.load_snapshot(instance_id).await?;
            if !snapshot.state.is_paused() {
                let state_name = match &snapshot.state {
                    WorkflowSnapshotState::InProgress { .. } => "InProgress",
                    WorkflowSnapshotState::Completed { .. } => "Completed",
                    WorkflowSnapshotState::Failed { .. } => "Failed",
                    WorkflowSnapshotState::Cancelled { .. } => "Cancelled",
                    WorkflowSnapshotState::Paused { .. } => "Paused",
                };
                return Err(BackendError::CannotPause(format!(
                    "Workflow is not paused (current state: {state_name:?})"
                )));
            }
            snapshot.mark_unpaused();
            self.save_snapshot(&mut snapshot).await?;
            Ok(snapshot)
        }
    }
}

// ---------------------------------------------------------------------------
// TaskClaimStore — only for distributed workers
// ---------------------------------------------------------------------------

/// Task claiming for distributed multi-worker execution.
///
/// Only needed when using [`PooledWorker`](crate). Single-process backends
/// (used with `CheckpointingRunner`) do not need to implement this.
///
/// `SnapshotStore` is a supertrait: claim stores hand out decoded
/// snapshots (`find_available_tasks`) and gate snapshot writes on claim
/// ownership (`save_snapshot_fenced`), so a claim store that can't store
/// snapshots is incoherent.
pub trait TaskClaimStore: SnapshotStore {
    /// Claim a task for execution by a worker node.
    ///
    /// Returns `Ok(Some(claim))` if successful, `Ok(None)` if already claimed.
    fn claim_task(
        &self,
        instance_id: &str,
        task_id: &sayiir_core::TaskId,
        worker_id: &str,
        ttl: Option<Duration>,
    ) -> impl Future<Output = Result<Option<TaskClaim>, BackendError>> + Send;

    /// Release a task claim.
    fn release_task_claim(
        &self,
        instance_id: &str,
        task_id: &sayiir_core::TaskId,
        worker_id: &str,
    ) -> impl Future<Output = Result<(), BackendError>> + Send;

    /// Extend a task claim's expiration time.
    fn extend_task_claim(
        &self,
        instance_id: &str,
        task_id: &sayiir_core::TaskId,
        worker_id: &str,
        additional_duration: Duration,
    ) -> impl Future<Output = Result<(), BackendError>> + Send;

    /// Find available tasks across all workflow instances.
    ///
    /// `aging_interval` controls starvation prevention: lower-priority tasks
    /// that have been waiting longer than this interval effectively gain one
    /// priority level per interval elapsed. Pass `Duration::MAX` to disable aging.
    ///
    /// # Constraints
    ///
    /// `aging_interval` **must be positive** (non-zero). Implementations may
    /// divide by this value; passing zero or a negative duration can cause
    /// division-by-zero or nonsensical ordering. Implementations should
    /// defensively clamp to a minimum of 1 second, but callers must not rely
    /// on this.
    fn find_available_tasks(
        &self,
        worker_id: &str,
        limit: usize,
        aging_interval: Duration,
        worker_tags: &[String],
    ) -> impl Future<Output = Result<Vec<AvailableTask>, BackendError>> + Send;

    /// Block until a wakeup arrives or `timeout` elapses. `Some(hint)`
    /// when a producer attached one; `None` for hint-less wakeups,
    /// timeout, or backends without a notification channel.
    ///
    /// Default returns a future that never resolves (`std::future::pending`)
    /// — the worker's `interval.tick()` arm provides the cadence, so
    /// non-overriding backends keep their fixed-interval poll. Keeps
    /// `sayiir-persistence` runtime-agnostic.
    fn wait_for_wakeup(
        &self,
        _timeout: std::time::Duration,
    ) -> impl Future<Output = Result<Option<TaskWakeupHint>, BackendError>> + Send {
        async move {
            std::future::pending::<Option<TaskWakeupHint>>().await;
            Ok(None)
        }
    }

    /// Resolve a wakeup hint into a claimable [`AvailableTask`], or
    /// `None` if the snapshot moved on / is claim-blocked / signal-blocked.
    /// Does NOT claim — the caller runs the normal claim+execute pipeline.
    /// Default returns `None`; backends opt in for the targeted lookup.
    fn find_hinted_task(
        &self,
        _hint: &TaskWakeupHint,
    ) -> impl Future<Output = Result<Option<AvailableTask>, BackendError>> + Send {
        async move { Ok(None) }
    }

    /// Save `snapshot` only while `worker_id` still holds a live claim on
    /// `(instance_id, task_id)`. Returns `Ok(false)` without writing when
    /// the claim is gone — a worker whose lease expired mid-execution must
    /// not overwrite the new claimant's progress.
    ///
    /// The default is an unfenced save (today's behaviour) for backends
    /// without claim-aware transactions.
    fn save_snapshot_fenced(
        &self,
        snapshot: &mut WorkflowSnapshot,
        task_id: &sayiir_core::TaskId,
        worker_id: &str,
    ) -> impl Future<Output = Result<bool, BackendError>> + Send {
        let _ = (task_id, worker_id);
        async move {
            self.save_snapshot(snapshot).await?;
            Ok(true)
        }
    }
}

// ---------------------------------------------------------------------------
// TaskResultStore — opt-in, like TaskClaimStore
// ---------------------------------------------------------------------------

/// Read-only access to individual task results from a workflow instance.
///
/// This trait allows retrieving completed task outputs (intermediate step
/// results) without loading the full snapshot. For in-progress, cancelled, or
/// paused workflows the results come straight from the current snapshot. For
/// completed or failed workflows the results are recovered from history (e.g.
/// the Postgres snapshot history table) or from an in-memory cache.
///
/// Implementations must also implement [`SnapshotStore`] because the current
/// snapshot is the primary source of truth for non-terminal states.
pub trait TaskResultStore: SnapshotStore {
    /// Load a single task result by task ID.
    ///
    /// Returns `Ok(Some(bytes))` if the task completed, `Ok(None)` if the task
    /// was never executed or is not found, and `Err` on I/O failure.
    fn load_task_result(
        &self,
        instance_id: &str,
        task_id: &sayiir_core::TaskId,
    ) -> impl Future<Output = Result<Option<bytes::Bytes>, BackendError>> + Send;
}

// ---------------------------------------------------------------------------
// PersistentBackend — supertrait + blanket impl
// ---------------------------------------------------------------------------

/// Supertrait combining [`SnapshotStore`] and [`SignalStore`].
///
/// This is the bound used by `CheckpointingRunner` and most of the runtime.
/// It is blanket-implemented for any type that implements both sub-traits,
/// so backends never need to implement it directly.
pub trait PersistentBackend: SnapshotStore + SignalStore {}

impl<T: SnapshotStore + SignalStore> PersistentBackend for T {}

// Re-export Future so the trait method return types resolve.
use std::future::Future;
