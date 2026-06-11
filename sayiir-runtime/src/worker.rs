//! Pooled worker for distributed, multi-worker workflow execution.
//!
//! A pooled worker is part of a worker pool that collaboratively executes workflows.
//! Each worker polls the backend for available tasks, claims them (to prevent duplicates),
//! executes them, and updates the snapshot. Multiple workers can process tasks from
//! the same workflow instance in parallel.
//!
//! **Use this when**: You need horizontal scaling with multiple workers processing
//! tasks concurrently across machines or processes.
//!
//! **Use [`CheckpointingRunner`](crate::runner::distributed::CheckpointingRunner) instead when**:
//! You want a single process to run an entire workflow with crash recovery.

use std::collections::HashMap;

use bytes::Bytes;
use chrono;
use futures::FutureExt;
use sayiir_core::codec::sealed;
use sayiir_core::codec::{Codec, EnvelopeCodec, LoopDecision};
use sayiir_core::context::{TaskExecutionContext, with_task_context};
use sayiir_core::error::{BoxError, CodecError, WorkflowError};
use sayiir_core::registry::TaskRegistry;
use sayiir_core::snapshot::{
    ExecutionPosition, SignalKind, SignalRequest, TaskDeadline, TaskHint, WorkflowSnapshot,
};
use sayiir_core::task_claim::AvailableTask;
use sayiir_core::workflow::{Workflow, WorkflowContinuation, WorkflowStatus};
use sayiir_persistence::{ControlSignal, PersistentBackend, TaskClaimStore, TaskWakeupHint};
use std::num::NonZeroUsize;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time;

/// Fallback poll-tick interval, phase-shifted per worker so a fleet spawned
/// together doesn't fire the `find_available_tasks` scan in lockstep (a
/// synchronized DB herd). Offset is `hash(worker_id) % poll_interval`. Keeps
/// the default `Burst` missed-tick behavior: a worker that falls behind catches
/// up immediately, which is what drives saturated back-to-back pickup.
fn staggered_poll_interval(worker_id: &str, poll_interval: Duration) -> time::Interval {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    worker_id.hash(&mut hasher);
    let period_ns = u64::try_from(poll_interval.as_nanos())
        .unwrap_or(u64::MAX)
        .max(1);
    let offset = Duration::from_nanos(hasher.finish() % period_ns);
    time::interval_at(time::Instant::now() + offset, poll_interval)
}

/// A list of workflow definitions keyed by their definition hash.
pub type WorkflowRegistry<C, Input, M> =
    Vec<(sayiir_core::DefinitionHash, Arc<Workflow<C, Input, M>>)>;

/// Workflow definition for binding-friendly worker API.
///
/// Contains only the structural information (definition hash + continuation tree)
/// needed by `PooledWorker` for position tracking, completion detection, retry
/// policies, and timeouts. Task execution is delegated to an external executor.
#[derive(Clone)]
pub struct ExternalWorkflow {
    /// The continuation tree describing the workflow structure.
    pub continuation: Arc<WorkflowContinuation>,
    /// Per-workflow `TaskId → metadata` index. Built once when the workflow
    /// is registered; replaces the O(N) tree walk + per-node SHA-256 that
    /// the worker would otherwise do for every task dispatch.
    pub task_index: Arc<sayiir_core::TaskIndex>,
    /// Human-readable workflow name (for log/error display and FFI task executor).
    pub workflow_id: Arc<str>,
    /// Optional JSON-encoded workflow-level metadata.
    pub metadata_json: Option<Arc<str>>,
}

/// Workflow index keyed by definition hash for O(1) lookup during task dispatch.
pub type WorkflowIndex = HashMap<sayiir_core::DefinitionHash, ExternalWorkflow>;

/// Definition-hash containment over either [`WorkflowIndex`] (`HashMap`,
/// O(1)) or [`WorkflowRegistry`] (`Vec`, O(n)). Lets `can_handle_hint`
/// accept any worker-registered workflows collection uniformly instead
/// of taking a hand-rolled closure per call site.
pub(crate) trait WorkflowLookup {
    fn contains_definition_hash(&self, hash: &sayiir_core::DefinitionHash) -> bool;
}

impl WorkflowLookup for WorkflowIndex {
    fn contains_definition_hash(&self, hash: &sayiir_core::DefinitionHash) -> bool {
        self.contains_key(hash)
    }
}

impl<C, Input, M> WorkflowLookup for WorkflowRegistry<C, Input, M> {
    fn contains_definition_hash(&self, hash: &sayiir_core::DefinitionHash) -> bool {
        self.iter().any(|(k, _)| k == hash)
    }
}

/// External task executor function signature.
///
/// Receives the task ID and input bytes, returns the output bytes.
/// Used by language bindings (Python, Node.js) to delegate task execution
/// to the host language's runtime.
pub type ExternalTaskExecutor = Arc<
    dyn Fn(
            &str,
            Bytes,
        ) -> Pin<Box<dyn std::future::Future<Output = Result<Bytes, BoxError>> + Send>>
        + Send
        + Sync,
>;

/// Internal command sent from [`WorkerHandle`] to the actor loop.
enum WorkerCommand {
    Shutdown,
}

/// What woke an actor loop up.
enum LoopEvent {
    /// Shutdown requested (or every handle dropped).
    Shutdown,
    /// Time to look for work, with an optional targeted hint.
    Wake(Option<TaskWakeupHint>),
}

struct WorkerHandleInner<B> {
    backend: Arc<B>,
    shutdown_tx: mpsc::Sender<WorkerCommand>,
    join_handle:
        tokio::sync::Mutex<Option<tokio::task::JoinHandle<Result<(), crate::error::RuntimeError>>>>,
}

/// A cloneable handle for interacting with a running [`PooledWorker`].
///
/// Obtained from [`PooledWorker::spawn`]. The handle is cheap to clone and can
/// be shared across tasks. Dropping **all** handles triggers a graceful
/// shutdown of the worker (equivalent to calling [`shutdown`](Self::shutdown)).
pub struct WorkerHandle<B> {
    inner: Arc<WorkerHandleInner<B>>,
}

impl<B> Clone for WorkerHandle<B> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<B> WorkerHandle<B> {
    /// Request a graceful shutdown of the worker.
    ///
    /// The worker will finish its current task (if any) and then exit.
    /// This is a non-async, fire-and-forget operation — errors are ignored
    /// (the actor may have already stopped).
    pub fn shutdown(&self) {
        let _ = self.inner.shutdown_tx.try_send(WorkerCommand::Shutdown);
    }

    /// Wait for the worker task to finish.
    ///
    /// The first caller gets the real result; subsequent callers get `Ok(())`.
    ///
    /// # Errors
    ///
    /// Returns an error if the worker task panicked or returned an error.
    pub async fn join(&self) -> Result<(), crate::error::RuntimeError> {
        let jh = self.inner.join_handle.lock().await.take();
        match jh {
            Some(jh) => Ok(jh.await??),
            None => Ok(()),
        }
    }

    /// Get a reference to the backend.
    #[must_use]
    pub fn backend(&self) -> &Arc<B> {
        &self.inner.backend
    }
}

/// Owns a claimed task and provides explicit release methods.
///
/// No `Drop` impl — callers must explicitly call `release()` or `release_quietly()`.
struct ActiveTaskClaim<'a, B> {
    backend: &'a B,
    instance_id: std::sync::Arc<str>,
    task_id: sayiir_core::TaskId,
    worker_id: String,
}

impl<B: TaskClaimStore> ActiveTaskClaim<'_, B> {
    /// Release the claim, propagating backend errors.
    async fn release(self) -> Result<(), crate::error::RuntimeError> {
        self.backend
            .release_task_claim(&self.instance_id, &self.task_id, &self.worker_id)
            .await?;
        Ok(())
    }

    /// Release the claim, silently ignoring errors. Use for error/panic paths.
    async fn release_quietly(self) {
        let _ = self.release().await;
    }
}

/// Outcome of running a task through `execute_with_deadline`.
enum ExecutionOutcome {
    /// Task completed successfully.
    Success(Bytes),
    /// Task execution returned an error.
    TaskError(crate::error::RuntimeError),
    /// Task panicked.
    Panic(Box<dyn std::any::Any + Send>),
    /// Heartbeat detected an expired deadline (active cancellation).
    Timeout(crate::error::RuntimeError),
    /// Heartbeat saw a pending cancel signal and dropped the task future.
    Cancelled,
    /// The claim lease was lost mid-execution (stolen or unrenewable);
    /// the local result must be abandoned, not persisted.
    ClaimLost,
}

impl From<HeartbeatInterrupt> for ExecutionOutcome {
    fn from(interrupt: HeartbeatInterrupt) -> Self {
        match interrupt {
            HeartbeatInterrupt::Timeout(err) => Self::Timeout(err),
            HeartbeatInterrupt::Cancelled => Self::Cancelled,
            HeartbeatInterrupt::ClaimLost => Self::ClaimLost,
        }
    }
}

/// Why `run_with_heartbeat` aborted the task future.
enum HeartbeatInterrupt {
    /// The task's deadline expired.
    Timeout(crate::error::RuntimeError),
    /// A cancel signal arrived for the instance.
    Cancelled,
    /// The claim could not be kept alive (stolen, released, or repeated
    /// extension failures past the tolerance window).
    ClaimLost,
}

/// Heartbeats fire at `claim_ttl / 4`, capped at this interval, so a
/// crashed worker's claim expires (and a live worker's stays fresh)
/// independently of how long the TTL is.
const HEARTBEAT_MAX_INTERVAL: Duration = Duration::from_secs(15);

/// Consecutive heartbeat-extension failures tolerated before the worker
/// assumes the lease is lost and abandons the task.
const HEARTBEAT_FAILURE_LIMIT: u32 = 3;

/// Extract a human-readable message from a panic payload.
fn extract_panic_message(payload: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        s.to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "Task panicked with unknown payload".to_string()
    }
}

/// A pooled worker that claims and executes tasks from a shared backend.
///
/// `PooledWorker` is designed for horizontal scaling: multiple workers can run
/// across different machines/processes, all polling the same backend for tasks.
/// Task claiming with TTL prevents duplicate execution while allowing automatic
/// recovery when workers crash.
///
/// # When to Use
///
/// - **Horizontal scaling**: Multiple workers process tasks concurrently
/// - **Fault tolerance**: Failed workers' tasks are automatically reclaimed
/// - **Load balancing**: Tasks distributed across available workers
///
/// For single-process execution with checkpointing, use
/// [`CheckpointingRunner`](crate::runner::distributed::CheckpointingRunner).
///
/// # Example
///
/// ```rust,no_run
/// # use sayiir_runtime::prelude::*;
/// # use std::sync::Arc;
/// # use std::time::Duration;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let backend = InMemoryBackend::new();
/// let registry = TaskRegistry::new();
/// let worker = PooledWorker::new("worker-1", backend, registry);
///
/// let ctx = WorkflowContext::new("my-wf", Arc::new(JsonCodec), Arc::new(()));
/// let workflow = WorkflowBuilder::new(ctx)
///     .then("step1", |i: u32| async move { Ok(i + 1) })
///     .build()?;
/// let workflows = vec![(*workflow.definition_hash(), Arc::new(workflow))];
///
/// // Spawn the worker and get a handle for lifecycle control
/// let handle = worker.spawn(Duration::from_secs(1), workflows);
/// // ... later ...
/// handle.shutdown();
/// handle.join().await?;
/// # Ok(())
/// # }
/// ```
pub struct PooledWorker<B> {
    worker_id: String,
    backend: Arc<B>,
    #[allow(unused)]
    registry: Arc<TaskRegistry>,
    claim_ttl: Option<Duration>,
    batch_size: NonZeroUsize,
    max_concurrent_tasks: Option<NonZeroUsize>,
    aging_interval: Duration,
    tags: Vec<String>,
}

impl<B> PooledWorker<B>
where
    B: PersistentBackend + TaskClaimStore + 'static,
{
    /// Create a new worker node.
    ///
    /// # Parameters
    ///
    /// - `worker_id`: Unique identifier for this worker node
    /// - `backend`: The persistent backend to use
    /// - `registry`: Task registry containing all task implementations
    ///
    /// # Heartbeat
    ///
    /// Heartbeats are derived automatically from `claim_ttl` (TTL / 4,
    /// capped at 15 s), so a crashed worker's claim becomes reclaimable
    /// quickly while a live worker renews well within margin.
    ///
    pub fn new(worker_id: impl Into<String>, backend: B, registry: TaskRegistry) -> Self {
        Self {
            worker_id: worker_id.into(),
            backend: Arc::new(backend),
            registry: Arc::new(registry),
            claim_ttl: Some(Duration::from_mins(5)), // Default 5 minutes
            batch_size: NonZeroUsize::MIN,           // Default: fetch one task at a time (1)
            max_concurrent_tasks: None,              // Default: follow batch_size
            aging_interval: Duration::from_mins(5),  // Default 5 minutes
            tags: vec![],
        }
    }

    /// Set the TTL for task claims.
    ///
    /// `None` means "never expires" — a crashed worker holding such a
    /// claim pins the workflow undispatchable until the claim row is
    /// manually released. Prefer a finite TTL (default 5 minutes) so
    /// crashed-worker recovery happens via the eligibility predicate's
    /// `expires_at > now()` check.
    #[must_use]
    pub fn with_claim_ttl(mut self, ttl: Option<Duration>) -> Self {
        if ttl.is_none() {
            tracing::warn!(
                "PooledWorker::with_claim_ttl(None) disables claim expiry; \
                 a crashed worker will pin its workflow until manual release"
            );
        }
        self.claim_ttl = ttl;
        self
    }

    /// Set the aging interval for priority-based scheduling.
    ///
    /// Lower-priority tasks that have been waiting longer than this interval
    /// get their effective priority boosted, preventing starvation.
    /// Default: 5 minutes (300 seconds).
    ///
    /// # Panics
    ///
    /// Panics if `interval` is zero.
    #[must_use]
    pub fn with_aging_interval(mut self, interval: Duration) -> Self {
        assert!(!interval.is_zero(), "aging interval must be non-zero");
        self.aging_interval = interval;
        self
    }

    /// Set the default in-flight task cap (default: 1).
    ///
    /// The worker fetches up to its free concurrency slots per poll, so
    /// this knob now seeds [`with_max_concurrent_tasks`](Self::with_max_concurrent_tasks)
    /// when that isn't set explicitly.
    #[must_use]
    pub fn with_batch_size(mut self, size: NonZeroUsize) -> Self {
        self.batch_size = size;
        self
    }

    /// Set how many tasks this worker executes concurrently (default:
    /// follows `batch_size`). Each in-flight task runs on its own tokio
    /// task with its own claim + heartbeat; throughput on I/O-bound tasks
    /// scales roughly linearly with this knob until the backend or the
    /// executor saturates.
    #[must_use]
    pub fn with_max_concurrent_tasks(mut self, max: NonZeroUsize) -> Self {
        self.max_concurrent_tasks = Some(max);
        self
    }

    /// Effective in-flight task cap: explicit setting, else `batch_size`.
    fn max_concurrent(&self) -> usize {
        self.max_concurrent_tasks.unwrap_or(self.batch_size).get()
    }

    /// Set affinity tags for this worker.
    ///
    /// When tags are set, the worker only picks up tasks whose tags are a
    /// subset of the worker's tags (or tasks with no tags). When no tags are
    /// set (the default), the worker accepts all tasks.
    #[must_use]
    pub fn with_tags(mut self, tags: Vec<String>) -> Self {
        self.tags = tags;
        self
    }

    /// Request cancellation of a workflow.
    ///
    /// This requests cancellation of the specified workflow instance.
    /// Running tasks will complete, but no new tasks will be started.
    ///
    /// # Parameters
    ///
    /// - `instance_id`: The workflow instance ID to cancel
    /// - `reason`: Optional reason for the cancellation
    /// - `cancelled_by`: Optional identifier of who requested the cancellation
    ///
    /// # Errors
    ///
    /// Returns an error if the workflow cannot be cancelled (not found or in terminal state).
    pub async fn cancel_workflow(
        &self,
        instance_id: &str,
        reason: Option<String>,
        cancelled_by: Option<String>,
    ) -> Result<(), crate::error::RuntimeError> {
        self.backend
            .store_signal(
                instance_id,
                SignalKind::Cancel,
                SignalRequest::new(reason, cancelled_by),
            )
            .await?;

        Ok(())
    }

    /// Request pausing of a workflow.
    ///
    /// This requests pausing of the specified workflow instance.
    /// Running tasks will complete, but no new tasks will be started.
    ///
    /// # Parameters
    ///
    /// - `instance_id`: The workflow instance ID to pause
    /// - `reason`: Optional reason for the pause
    /// - `paused_by`: Optional identifier of who requested the pause
    ///
    /// # Errors
    ///
    /// Returns an error if the workflow cannot be paused (not found or in terminal/paused state).
    pub async fn pause_workflow(
        &self,
        instance_id: &str,
        reason: Option<String>,
        paused_by: Option<String>,
    ) -> Result<(), crate::error::RuntimeError> {
        self.backend
            .store_signal(
                instance_id,
                SignalKind::Pause,
                SignalRequest::new(reason, paused_by),
            )
            .await?;

        Ok(())
    }

    /// Get a reference to the backend.
    #[must_use]
    pub fn backend(&self) -> &Arc<B> {
        &self.backend
    }

    /// Spawn the worker as a background task and return a handle.
    ///
    /// Consumes `self`, creates an internal command channel, and spawns the
    /// actor loop on the Tokio runtime. Returns a cloneable [`WorkerHandle`]
    /// for lifecycle control — call [`WorkerHandle::shutdown`] to request
    /// graceful shutdown and [`WorkerHandle::join`] to await completion.
    ///
    /// The worker runs until:
    /// - [`WorkerHandle::shutdown`] is called, or
    /// - All clones of the handle are dropped, or
    /// - A fatal backend error occurs.
    ///
    /// # Parameters
    ///
    /// - `poll_interval`: How often to poll for new tasks
    /// - `workflows`: Map of workflow definition hash to workflow
    #[must_use]
    pub fn spawn<C, Input, M>(
        self,
        poll_interval: Duration,
        workflows: WorkflowRegistry<C, Input, M>,
    ) -> WorkerHandle<B>
    where
        Input: Send + Sync + 'static,
        M: Send + Sync + 'static,
        C: Codec
            + EnvelopeCodec
            + sealed::DecodeValue<Input>
            + sealed::EncodeValue<Input>
            + 'static,
    {
        let (tx, rx) = mpsc::channel(1);
        let backend = Arc::clone(&self.backend);
        let worker = Arc::new(self);
        let join_handle =
            tokio::spawn(async move { worker.run_actor_loop(poll_interval, workflows, rx).await });
        WorkerHandle {
            inner: Arc::new(WorkerHandleInner {
                backend,
                shutdown_tx: tx,
                join_handle: tokio::sync::Mutex::new(Some(join_handle)),
            }),
        }
    }

    /// Spawn the worker with an external executor and return a handle.
    ///
    /// Like [`spawn`](Self::spawn) but instead of executing tasks via typed
    /// `Workflow` closures, delegates all task execution to the provided
    /// `executor`. This is used by language bindings (Python, Node.js) where
    /// task functions live in the host language.
    ///
    /// # Parameters
    ///
    /// - `poll_interval`: How often to poll for new tasks
    /// - `workflows`: Workflow definitions (hash + continuation tree)
    /// - `executor`: Closure that executes a task by ID given input bytes
    #[must_use]
    pub fn spawn_with_executor(
        self,
        poll_interval: Duration,
        workflows: WorkflowIndex,
        executor: ExternalTaskExecutor,
    ) -> WorkerHandle<B> {
        let (tx, rx) = mpsc::channel(1);
        let backend = Arc::clone(&self.backend);
        let worker = Arc::new(self);
        let join_handle = tokio::spawn(async move {
            worker
                .run_external_actor_loop(poll_interval, workflows, executor, rx)
                .await
        });
        WorkerHandle {
            inner: Arc::new(WorkerHandleInner {
                backend,
                shutdown_tx: tx,
                join_handle: tokio::sync::Mutex::new(Some(join_handle)),
            }),
        }
    }

    /// Reap finished tasks, wait below capacity, then wait for the next
    /// wakeup signal. Shared verbatim by both actor loops so their
    /// concurrency/shutdown semantics can't drift.
    async fn next_loop_event(
        &self,
        interval: &mut time::Interval,
        cmd_rx: &mut mpsc::Receiver<WorkerCommand>,
        inflight: &mut tokio::task::JoinSet<()>,
        max_inflight: usize,
        poll_interval: Duration,
    ) -> LoopEvent {
        loop {
            while inflight.try_join_next().is_some() {}

            // At capacity: wait for a slot (or shutdown) instead of polling.
            if inflight.len() >= max_inflight {
                tokio::select! {
                    biased;
                    _ = cmd_rx.recv() => return LoopEvent::Shutdown,
                    _ = inflight.join_next() => {}
                }
                continue;
            }

            return tokio::select! {
                biased;

                cmd = cmd_rx.recv() => {
                    match cmd {
                        Some(WorkerCommand::Shutdown) | None => LoopEvent::Shutdown,
                    }
                }
                _ = interval.tick() => {
                    tracing::trace!(worker_id = %self.worker_id, "fallback poll tick");
                    LoopEvent::Wake(None)
                }
                hint = self.backend.wait_for_wakeup(poll_interval) => {
                    match hint {
                        Ok(hint) => {
                            tracing::debug!(
                                worker_id = %self.worker_id,
                                has_hint = hint.is_some(),
                                "wakeup notification",
                            );
                            LoopEvent::Wake(hint)
                        }
                        Err(e) => {
                            tracing::warn!(worker_id = %self.worker_id, error = %e, "wakeup wait failed");
                            LoopEvent::Wake(None)
                        }
                    }
                }
            };
        }
    }

    /// Fetch up to `limit` dispatchable tasks, logging (not propagating)
    /// backend errors so a transient DB outage doesn't kill the worker.
    async fn poll_batch(&self, limit: usize) -> Vec<AvailableTask> {
        match self
            .backend
            .find_available_tasks(
                &self.worker_id,
                limit,
                chrono::Duration::from_std(self.aging_interval).unwrap_or(chrono::Duration::MAX),
                &self.tags,
            )
            .await
        {
            Ok(tasks) => tasks,
            Err(e) => {
                tracing::warn!(worker_id = %self.worker_id, error = %e, "task poll failed; will retry");
                Vec::new()
            }
        }
    }

    /// Actor loop for external executor mode.
    ///
    /// Up to [`max_concurrent`](Self::max_concurrent) tasks run
    /// simultaneously, each on its own tokio task with its own claim and
    /// heartbeat. Task failures (including timeouts) fail or retry that
    /// workflow only — they never take the worker down. Backend errors on
    /// the polling path are logged and retried on the next tick, so a
    /// transient DB outage doesn't kill the fleet either.
    async fn run_external_actor_loop(
        self: Arc<Self>,
        poll_interval: Duration,
        workflows: WorkflowIndex,
        executor: ExternalTaskExecutor,
        mut cmd_rx: mpsc::Receiver<WorkerCommand>,
    ) -> Result<(), crate::error::RuntimeError> {
        let mut interval = staggered_poll_interval(&self.worker_id, poll_interval);
        let max_inflight = self.max_concurrent();
        let mut inflight = tokio::task::JoinSet::new();

        loop {
            let hint = match self
                .next_loop_event(
                    &mut interval,
                    &mut cmd_rx,
                    &mut inflight,
                    max_inflight,
                    poll_interval,
                )
                .await
            {
                LoopEvent::Shutdown => return self.drain_inflight(inflight).await,
                LoopEvent::Wake(hint) => hint,
            };

            if let Some(h) = hint.as_ref()
                && !self.can_handle_hint(h, &workflows)
            {
                // This optimization cuts PG load proportional to NOTIFY volume x fleet-tag-fragmentation.
                tracing::trace!(
                    worker_id = %self.worker_id,
                    instance_id = %h.instance_id,
                    "skipping wakeup, hint not handleable here",
                );
                continue;
            }

            if let Some(h) = hint.as_ref() {
                match self
                    .spawn_hinted_task(h, &workflows, &executor, &mut inflight)
                    .await
                {
                    Ok(true) => continue,
                    Ok(false) => {}
                    Err(e) => {
                        tracing::warn!(worker_id = %self.worker_id, error = %e, "hinted lookup failed");
                    }
                }
            }

            // Fetch up to the free slots — `batch_size` only seeds the
            // default concurrency; the in-flight cap is the real budget.
            let limit = max_inflight - inflight.len();
            for task in self.poll_batch(limit).await {
                if let Ok(WorkerCommand::Shutdown) | Err(mpsc::error::TryRecvError::Disconnected) =
                    cmd_rx.try_recv()
                {
                    return self.drain_inflight(inflight).await;
                }

                if let Some(ext_wf) = workflows.get(&task.workflow_definition_hash) {
                    self.spawn_external_task(ext_wf.clone(), &executor, task, &mut inflight);
                }
            }
        }
    }

    /// Finish all in-flight tasks, then exit. Callers needing a bound
    /// should wrap [`WorkerHandle::join`] in `tokio::time::timeout`;
    /// per-task deadlines keep this finite for deadline-bearing tasks.
    async fn drain_inflight(
        &self,
        mut inflight: tokio::task::JoinSet<()>,
    ) -> Result<(), crate::error::RuntimeError> {
        tracing::info!(
            worker_id = %self.worker_id,
            inflight = inflight.len(),
            "Worker shutting down; draining in-flight tasks"
        );
        while inflight.join_next().await.is_some() {}
        Ok(())
    }

    /// Spawn one externally-executed task onto the in-flight set.
    fn spawn_external_task(
        self: &Arc<Self>,
        ext_wf: ExternalWorkflow,
        executor: &ExternalTaskExecutor,
        task: AvailableTask,
        inflight: &mut tokio::task::JoinSet<()>,
    ) {
        let worker = Arc::clone(self);
        let executor = Arc::clone(executor);
        inflight.spawn(async move {
            let hash = task.workflow_definition_hash;
            match worker
                .execute_external_task(&ext_wf, &hash, &executor, task)
                .await
            {
                Ok(_) => tracing::info!(worker_id = %worker.worker_id, "completed task"),
                Err(e) => {
                    tracing::error!(worker_id = %worker.worker_id, error = %e, "task execution failed");
                }
            }
        });
    }

    /// True if the worker has the hint's workflow registered and its
    /// tag set covers the hint's tags.
    fn can_handle_hint(&self, hint: &TaskWakeupHint, workflows: &impl WorkflowLookup) -> bool {
        let hash = sayiir_core::DefinitionHash::from_bytes(hint.definition_hash);
        if !workflows.contains_definition_hash(&hash) {
            return false;
        }
        hint.tags.iter().all(|t| self.tags.contains(t))
    }

    /// `Ok(true)` if the hinted task resolved and was spawned for
    /// execution, `Ok(false)` to fall through to the full polling scan.
    async fn spawn_hinted_task(
        self: &Arc<Self>,
        hint: &TaskWakeupHint,
        workflows: &WorkflowIndex,
        executor: &ExternalTaskExecutor,
        inflight: &mut tokio::task::JoinSet<()>,
    ) -> Result<bool, crate::error::RuntimeError> {
        let Some(task) = self.backend.find_hinted_task(hint).await? else {
            return Ok(false);
        };
        let Some(ext_wf) = workflows.get(&task.workflow_definition_hash) else {
            return Ok(false);
        };
        self.spawn_external_task(ext_wf.clone(), executor, task, inflight);
        Ok(true)
    }

    /// Execute a single task using an external executor.
    #[tracing::instrument(
        name = "workflow",
        skip_all,
        fields(
            worker_id = %self.worker_id,
            instance_id = %available_task.instance_id,
            task_id = %available_task.task_id,
            definition_hash = %definition_hash,
        ),
    )]
    async fn execute_external_task(
        &self,
        ext_wf: &ExternalWorkflow,
        definition_hash: &sayiir_core::DefinitionHash,
        executor: &ExternalTaskExecutor,
        mut available_task: AvailableTask,
    ) -> Result<WorkflowStatus, crate::error::RuntimeError> {
        // Link current span to the workflow's trace context (cross-worker propagation)
        #[cfg(feature = "otel")]
        if let Some(ref tp) = available_task.trace_parent {
            use tracing_opentelemetry::OpenTelemetrySpanExt;
            let remote_ctx = crate::trace_context::context_from_trace_parent(tp);
            let _ = tracing::Span::current().set_parent(remote_ctx);
        }

        // Workers that lose the claim race drop the `available_task`
        // (and its `Arc<WorkflowSnapshot>`) cheaply — extract the owned
        // snapshot strictly past the claim_task gate.
        let already_completed = Self::validate_task_preconditions(
            definition_hash,
            &ext_wf.task_index,
            &available_task,
            &available_task.snapshot,
        )?;
        if already_completed {
            return Ok(WorkflowStatus::InProgress);
        }

        let Some(claim) = self.claim_task(&available_task).await? else {
            return Ok(WorkflowStatus::InProgress);
        };

        if let Some(status) = self.check_post_claim_guards(&available_task).await? {
            claim.release_quietly().await;
            return Ok(status);
        }

        // Past the claim gate — take the snapshot out of its Arc (a move,
        // not a deep clone: the dispatch Arc is never shared).
        let mut snapshot = available_task.take_snapshot();

        tracing::debug!(
            instance_id = %available_task.instance_id,
            task_id = %available_task.task_id,
            "Executing task (external)"
        );

        let execution_result = self
            .execute_with_deadline_ext(ext_wf, executor, &available_task, &mut snapshot, &claim)
            .await;

        self.settle_execution_result_ext(
            execution_result,
            &ext_wf.continuation,
            &ext_wf.task_index,
            &available_task,
            &mut snapshot,
            claim,
        )
        .await
    }

    /// Run the external executor with an optional deadline.
    async fn execute_with_deadline_ext(
        &self,
        ext_wf: &ExternalWorkflow,
        executor: &ExternalTaskExecutor,
        available_task: &AvailableTask,
        snapshot: &mut WorkflowSnapshot,
        claim: &ActiveTaskClaim<'_, B>,
    ) -> ExecutionOutcome {
        let task_id = available_task.task_id;
        let input = available_task.input.clone();
        // Resolve the human-readable task name from the prebuilt index so the
        // FFI executor can look up the Python/Node function by name. No tree
        // walk and no per-node SHA-256 — this is a single hash-map probe.
        let indexed_meta = ext_wf.task_index.get(&task_id);
        let task_name: Arc<str> =
            indexed_meta.map_or_else(|| Arc::from(task_id.to_hex()), |m| Arc::clone(m.name()));

        let deadline =
            if let Some(timeout) = indexed_meta.and_then(sayiir_core::TaskNodeMetadata::timeout) {
                snapshot.set_task_deadline(task_id, timeout);
                snapshot.refresh_task_deadline();
                // Deadline lives in-process for `run_with_heartbeat`; it
                // is persisted on the next save_task_result (which holds
                // a FOR UPDATE lock and so can't race with check_and_*).
                // A bare save_snapshot here would clobber a
                // check_and_cancel that landed between
                // check_post_claim_guards and now, silently resurrecting
                // a cancelled workflow into InProgress.
                snapshot.task_deadline.clone()
            } else {
                None
            };

        let task_ctx = TaskExecutionContext {
            workflow_id: Arc::clone(&ext_wf.workflow_id),
            instance_id: Arc::clone(&available_task.instance_id),
            task_id: Arc::clone(&task_name),
            metadata: ext_wf.task_index.build_task_metadata(&task_id),
            workflow_metadata_json: ext_wf.metadata_json.clone(),
        };

        let execution_future = with_task_context(task_ctx, executor(&task_name, input));

        let heartbeat_result = self
            .run_with_heartbeat(
                claim,
                deadline.as_ref(),
                AssertUnwindSafe(execution_future).catch_unwind(),
            )
            .await;

        snapshot.clear_task_deadline();

        match heartbeat_result {
            Err(interrupt) => interrupt.into(),
            Ok(Err(panic_payload)) => ExecutionOutcome::Panic(panic_payload),
            Ok(Ok(Err(e))) => ExecutionOutcome::TaskError(e.into()),
            Ok(Ok(Ok(output))) => ExecutionOutcome::Success(output),
        }
    }

    /// Settle execution result for external executor mode.
    #[tracing::instrument(
        name = "settle_result",
        skip_all,
        fields(worker_id = %self.worker_id, instance_id = %available_task.instance_id, task_id = %available_task.task_id),
    )]
    async fn settle_execution_result_ext(
        &self,
        outcome: ExecutionOutcome,
        continuation: &WorkflowContinuation,
        task_index: &sayiir_core::TaskIndex,
        available_task: &AvailableTask,
        snapshot: &mut WorkflowSnapshot,
        claim: ActiveTaskClaim<'_, B>,
    ) -> Result<WorkflowStatus, crate::error::RuntimeError> {
        tracing::debug!("settling execution result");
        match outcome {
            ExecutionOutcome::Timeout(err) => {
                match self
                    .settle_failure(
                        task_index,
                        available_task,
                        snapshot,
                        claim,
                        &err.to_string(),
                    )
                    .await
                {
                    Some(status) => Ok(status),
                    None => Err(err),
                }
            }
            ExecutionOutcome::Panic(panic_payload) => {
                let panic_msg = extract_panic_message(&panic_payload);
                match self
                    .settle_failure(task_index, available_task, snapshot, claim, &panic_msg)
                    .await
                {
                    Some(status) => Ok(status),
                    None => Err(WorkflowError::TaskPanicked(panic_msg).into()),
                }
            }
            ExecutionOutcome::TaskError(e) => {
                match self
                    .settle_failure(task_index, available_task, snapshot, claim, &e.to_string())
                    .await
                {
                    Some(status) => Ok(status),
                    None => Err(e),
                }
            }
            ExecutionOutcome::Cancelled => self.settle_cancelled(available_task, claim).await,
            ExecutionOutcome::ClaimLost => Ok(Self::settle_claim_lost(available_task)),
            ExecutionOutcome::Success(output) => {
                snapshot.clear_retry_state(&available_task.task_id);
                if !self
                    .commit_task_result(
                        continuation,
                        available_task,
                        snapshot,
                        output.clone(),
                        claim,
                    )
                    .await?
                {
                    return Ok(Self::settle_claim_lost(available_task));
                }
                self.determine_post_task_status(continuation, available_task, snapshot, output)
                    .await
            }
        }
    }

    /// Retry-or-fail policy shared by every failure outcome: schedule a
    /// retry if the policy allows (returning the resulting status), else
    /// fail the workflow and return `None` so the caller surfaces the
    /// final error.
    async fn settle_failure(
        &self,
        task_index: &sayiir_core::TaskIndex,
        available_task: &AvailableTask,
        snapshot: &mut WorkflowSnapshot,
        claim: ActiveTaskClaim<'_, B>,
        error_msg: &str,
    ) -> Option<WorkflowStatus> {
        if let Ok(Some(status)) = self
            .try_schedule_retry(task_index, available_task, snapshot, error_msg)
            .await
        {
            claim.release_quietly().await;
            return Some(status);
        }
        self.fail_workflow(available_task, snapshot, claim, error_msg)
            .await;
        None
    }

    /// Terminal-failure path shared by timeout/panic/error settles once
    /// retries are exhausted (or absent): mark the workflow failed so the
    /// poison task stops being re-offered at poll speed, persist under the
    /// claim fence, release.
    async fn fail_workflow(
        &self,
        available_task: &AvailableTask,
        snapshot: &mut WorkflowSnapshot,
        claim: ActiveTaskClaim<'_, B>,
        error_msg: &str,
    ) {
        tracing::error!(
            instance_id = %available_task.instance_id,
            task_id = %available_task.task_id,
            error = %error_msg,
            "Task failed with no retries left — marking workflow failed"
        );
        snapshot.mark_failed(error_msg.to_string());
        match self
            .backend
            .save_snapshot_fenced(snapshot, &claim.task_id, &claim.worker_id)
            .await
        {
            Ok(true) => {}
            Ok(false) => tracing::warn!(
                instance_id = %available_task.instance_id,
                "claim lost; failure state not persisted (new claimant owns the task)"
            ),
            Err(e) => tracing::warn!(
                instance_id = %available_task.instance_id,
                error = %e,
                "failed to persist failure state"
            ),
        }
        claim.release_quietly().await;
    }

    /// A cancel signal interrupted the running task: release the claim and
    /// apply the cancellation (the dropped future did no further work).
    async fn settle_cancelled(
        &self,
        available_task: &AvailableTask,
        claim: ActiveTaskClaim<'_, B>,
    ) -> Result<WorkflowStatus, crate::error::RuntimeError> {
        claim.release_quietly().await;
        if self
            .backend
            .check_and_cancel(&available_task.instance_id, Some(available_task.task_id))
            .await?
        {
            return Ok(self
                .load_cancelled_status(&available_task.instance_id)
                .await);
        }
        // Signal vanished between detection and here — the task simply
        // re-dispatches.
        Ok(WorkflowStatus::InProgress)
    }

    /// The lease was lost mid-execution: another worker owns the task now.
    /// Abandon the local result; do NOT release (the claim isn't ours).
    fn settle_claim_lost(available_task: &AvailableTask) -> WorkflowStatus {
        tracing::warn!(
            instance_id = %available_task.instance_id,
            task_id = %available_task.task_id,
            "task claim lost mid-execution; abandoning local result"
        );
        WorkflowStatus::InProgress
    }

    /// The actor loop: poll for tasks, execute them, respond to shutdown.
    ///
    /// Same concurrency and resilience model as the external loop: up to
    /// [`max_concurrent`](Self::max_concurrent) tasks in flight at once,
    /// task failures fail that workflow only, and polling-path backend
    /// errors are logged and retried on the next tick.
    #[allow(clippy::too_many_lines)]
    async fn run_actor_loop<C, Input, M>(
        self: Arc<Self>,
        poll_interval: Duration,
        workflows: WorkflowRegistry<C, Input, M>,
        mut cmd_rx: mpsc::Receiver<WorkerCommand>,
    ) -> Result<(), crate::error::RuntimeError>
    where
        Input: Send + Sync + 'static,
        M: Send + Sync + 'static,
        C: Codec
            + EnvelopeCodec
            + sealed::DecodeValue<Input>
            + sealed::EncodeValue<Input>
            + 'static,
    {
        let mut interval = staggered_poll_interval(&self.worker_id, poll_interval);
        let max_inflight = self.max_concurrent();
        let mut inflight = tokio::task::JoinSet::new();

        loop {
            // In-process mode only filters irrelevant wakes; the direct-
            // claim shortcut lives on the external-executor path.
            let hint = match self
                .next_loop_event(
                    &mut interval,
                    &mut cmd_rx,
                    &mut inflight,
                    max_inflight,
                    poll_interval,
                )
                .await
            {
                LoopEvent::Shutdown => return self.drain_inflight(inflight).await,
                LoopEvent::Wake(hint) => hint,
            };

            if let Some(h) = hint.as_ref()
                && !self.can_handle_hint(h, &workflows)
            {
                // This optimization cuts PG load proportional to NOTIFY volume x fleet-tag-fragmentation.
                tracing::trace!(
                    worker_id = %self.worker_id,
                    instance_id = %h.instance_id,
                    "skipping wakeup, hint not handleable here",
                );
                continue;
            }

            // Fetch up to the free slots — `batch_size` only seeds the
            // default concurrency; the in-flight cap is the real budget.
            let limit = max_inflight - inflight.len();
            for task in self.poll_batch(limit).await {
                if let Ok(WorkerCommand::Shutdown) | Err(mpsc::error::TryRecvError::Disconnected) =
                    cmd_rx.try_recv()
                {
                    return self.drain_inflight(inflight).await;
                }

                if let Some((_, workflow)) = workflows
                    .iter()
                    .find(|(hash, _)| *hash == task.workflow_definition_hash)
                // hash and task.workflow_definition_hash are both DefinitionHash
                {
                    let worker = Arc::clone(&self);
                    let workflow = Arc::clone(workflow);
                    inflight.spawn(async move {
                        match worker.execute_task(workflow.as_ref(), task).await {
                            Ok(_) => {
                                tracing::info!(worker_id = %worker.worker_id, "completed task");
                            }
                            Err(e) => {
                                tracing::error!(
                                    worker_id = %worker.worker_id,
                                    error = %e,
                                    "task execution failed"
                                );
                            }
                        }
                    });
                }
            }
        }
    }

    /// Load cancellation status from a snapshot.
    ///
    /// Attempts to load the snapshot and extract cancellation details.
    /// Returns `WorkflowStatus::Cancelled` with either the extracted details or defaults.
    async fn load_cancelled_status(&self, instance_id: &str) -> WorkflowStatus {
        if let Ok(snapshot) = self.backend.load_snapshot(instance_id).await
            && let Some((reason, cancelled_by)) = snapshot.state.cancellation_details()
        {
            return WorkflowStatus::Cancelled {
                reason,
                cancelled_by,
            };
        }
        WorkflowStatus::Cancelled {
            reason: None,
            cancelled_by: None,
        }
    }

    /// Load paused status from a snapshot.
    ///
    /// Attempts to load the snapshot and extract pause details.
    /// Returns `WorkflowStatus::Paused` with either the extracted details or defaults.
    async fn load_paused_status(&self, instance_id: &str) -> WorkflowStatus {
        if let Ok(snapshot) = self.backend.load_snapshot(instance_id).await
            && let Some((reason, paused_by)) = snapshot.state.pause_details()
        {
            return WorkflowStatus::Paused { reason, paused_by };
        }
        WorkflowStatus::Paused {
            reason: None,
            paused_by: None,
        }
    }

    /// Execute a single task from an available task.
    ///
    /// This claims the task, executes it, updates the snapshot, and releases the claim.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The task cannot be claimed
    /// - The workflow definition hash doesn't match
    /// - Task execution fails
    /// - Snapshot update fails
    #[tracing::instrument(
        name = "workflow",
        skip_all,
        fields(
            worker_id = %self.worker_id,
            instance_id = %available_task.instance_id,
            task_id = %available_task.task_id,
            definition_hash = %available_task.workflow_definition_hash,
        ),
    )]
    pub async fn execute_task<C, Input, M>(
        &self,
        workflow: &Workflow<C, Input, M>,
        mut available_task: AvailableTask,
    ) -> Result<WorkflowStatus, crate::error::RuntimeError>
    where
        Input: Send + 'static,
        M: Send + Sync + 'static,
        C: Codec
            + EnvelopeCodec
            + sealed::DecodeValue<Input>
            + sealed::EncodeValue<Input>
            + 'static,
    {
        // Link current span to the workflow's trace context (cross-worker propagation)
        #[cfg(feature = "otel")]
        if let Some(ref tp) = available_task.trace_parent {
            use tracing_opentelemetry::OpenTelemetrySpanExt;
            let remote_ctx = crate::trace_context::context_from_trace_parent(tp);
            let _ = tracing::Span::current().set_parent(remote_ctx);
        }

        // 1. Use the snapshot the dispatch SELECT already decoded.
        // Lost-race workers drop the `Arc<WorkflowSnapshot>` cheaply —
        // extract the owned snapshot strictly past the claim_task gate.
        let already_completed = Self::validate_task_preconditions(
            workflow.definition_hash(),
            workflow.task_index(),
            &available_task,
            &available_task.snapshot,
        )?;
        if already_completed {
            return Ok(WorkflowStatus::InProgress);
        }

        let Some(claim) = self.claim_task(&available_task).await? else {
            return Ok(WorkflowStatus::InProgress);
        };

        // 3. Post-claim guards (cancel/pause)
        if let Some(status) = self.check_post_claim_guards(&available_task).await? {
            claim.release_quietly().await;
            return Ok(status);
        }

        // Past the claim gate — take the snapshot out of its Arc (a move,
        // not a deep clone: the dispatch Arc is never shared).
        let mut snapshot = available_task.take_snapshot();

        tracing::debug!(
            instance_id = %available_task.instance_id,
            task_id = %available_task.task_id,
            "Executing task"
        );

        // 4. Execute with deadline + heartbeat, then settle the result
        let execution_result = self
            .execute_with_deadline(workflow, &available_task, &mut snapshot, &claim)
            .await;

        self.settle_execution_result(
            execution_result,
            workflow,
            &available_task,
            &mut snapshot,
            claim,
        )
        .await
    }

    /// Run the task future with an optional deadline, returning the panic-wrapped result.
    ///
    /// Sets a deadline on the snapshot (if the task has a timeout), persists it,
    /// then runs the future inside `run_with_heartbeat`. On heartbeat-level timeout
    /// the task future is dropped and an `Err` is returned.
    async fn execute_with_deadline<C, Input, M>(
        &self,
        workflow: &Workflow<C, Input, M>,
        available_task: &AvailableTask,
        snapshot: &mut WorkflowSnapshot,
        claim: &ActiveTaskClaim<'_, B>,
    ) -> ExecutionOutcome
    where
        Input: Send + 'static,
        M: Send + Sync + 'static,
        C: Codec
            + EnvelopeCodec
            + sealed::DecodeValue<Input>
            + sealed::EncodeValue<Input>
            + 'static,
    {
        let continuation = workflow.continuation();
        let task_index = workflow.task_index();
        let task_id = available_task.task_id;
        // If this task is the entry of a Fork's join, rebuild the
        // `NamedBranchResults` envelope the join body deserialises into
        // `BranchOutputs`. The PooledWorker dispatch path otherwise
        // hands the join the LAST branch's output (whatever
        // `get_last_task_output` saw), which fails to deserialise as
        // soon as any branch result is read by id. The in-process
        // runner builds this through `resolve_join`; for the worker
        // path we have to do it explicitly because each task is its
        // own dispatch and there is no in-memory `branch_results`.
        let input = match Self::find_fork_branches_for_join(continuation, &task_id) {
            Some(branches) => {
                let build = || -> Result<Bytes, crate::error::RuntimeError> {
                    let mut results = Vec::with_capacity(branches.len());
                    for branch in branches {
                        // The join envelope keys results by `branch.id()`
                        // (the branch entry node) so user code can look
                        // up `results["branch-a"]`, but the actual output
                        // is stored under the branch's TERMINAL task —
                        // for multi-step branches the entry task's
                        // output is an intermediate value, not the
                        // branch's "result". TaskId::from(branch.id())
                        // works only for single-Task branches and either
                        // returns the wrong bytes (entry task's output)
                        // or surfaces a TaskNotFound that hides the real
                        // problem.
                        let branch_name = branch.id().to_string();
                        let terminal_tid = sayiir_core::TaskId::from(branch.terminal_task_id());
                        let output = snapshot
                            .get_task_result_bytes(&terminal_tid)
                            .ok_or_else(|| WorkflowError::TaskNotFound(branch_name.clone()))?;
                        results.push((branch_name, output));
                    }
                    crate::execution::serialize_branch_results(&results, workflow.codec().as_ref())
                };
                match build() {
                    Ok(bytes) => bytes,
                    Err(e) => return ExecutionOutcome::TaskError(e),
                }
            }
            None => available_task.input.clone(),
        };
        // Resolve human-readable task name from the prebuilt index for the
        // task context exposed to user code (no tree walk, no rehashing).
        let indexed_meta = task_index.get(&task_id);
        let task_name: Arc<str> =
            indexed_meta.map_or_else(|| Arc::from(task_id.to_hex()), |m| Arc::clone(m.name()));

        // Set deadline if task has a timeout configured
        let deadline =
            if let Some(timeout) = indexed_meta.and_then(sayiir_core::TaskNodeMetadata::timeout) {
                snapshot.set_task_deadline(task_id, timeout);
                snapshot.refresh_task_deadline();
                // Deadline lives in-process for `run_with_heartbeat`; it
                // is persisted on the next save_task_result (which holds
                // a FOR UPDATE lock and so can't race with check_and_*).
                // A bare save_snapshot here would clobber a
                // check_and_cancel that landed between
                // check_post_claim_guards and now, silently resurrecting
                // a cancelled workflow into InProgress.
                snapshot.task_deadline.clone()
            } else {
                None
            };

        let task_ctx = TaskExecutionContext {
            workflow_id: Arc::from(workflow.context().workflow_id()),
            instance_id: Arc::clone(&available_task.instance_id),
            task_id: Arc::clone(&task_name),
            metadata: task_index.build_task_metadata(&task_id),
            workflow_metadata_json: workflow.context().metadata_json.clone(),
        };

        let execution_future = with_task_context(task_ctx, async move {
            Self::execute_task_by_id(continuation, &task_name, input).await
        });

        let heartbeat_result = self
            .run_with_heartbeat(
                claim,
                deadline.as_ref(),
                AssertUnwindSafe(execution_future).catch_unwind(),
            )
            .await;

        snapshot.clear_task_deadline();

        match heartbeat_result {
            Err(interrupt) => interrupt.into(),
            Ok(Err(panic_payload)) => ExecutionOutcome::Panic(panic_payload),
            Ok(Ok(Err(e))) => ExecutionOutcome::TaskError(e),
            Ok(Ok(Ok(output))) => ExecutionOutcome::Success(output),
        }
    }

    /// Try to schedule a retry for a failed task.
    ///
    /// Looks up the retry policy in the task index. If retries are available,
    /// records the retry state on the snapshot, clears the deadline, saves the
    /// snapshot, releases the claim, and returns `Ok(Some(InProgress))`.
    /// Otherwise returns `Ok(None)` (caller falls through to existing error handling).
    async fn try_schedule_retry(
        &self,
        task_index: &sayiir_core::TaskIndex,
        available_task: &AvailableTask,
        snapshot: &mut WorkflowSnapshot,
        error_msg: &str,
    ) -> Result<Option<WorkflowStatus>, crate::error::RuntimeError> {
        let Some(policy) = task_index.retry_policy(&available_task.task_id) else {
            return Ok(None);
        };

        if snapshot.retries_exhausted(&available_task.task_id) {
            return Ok(None);
        }

        let next_retry_at = snapshot.record_retry(
            available_task.task_id,
            policy,
            error_msg,
            Some(&self.worker_id),
        );
        snapshot.clear_task_deadline();
        // Fenced: if the lease was lost the new claimant owns retry state.
        let _ = self
            .backend
            .save_snapshot_fenced(snapshot, &available_task.task_id, &self.worker_id)
            .await;

        tracing::info!(
            instance_id = %available_task.instance_id,
            task_id = %available_task.task_id,
            attempt = snapshot.get_retry_state(&available_task.task_id).map_or(0, |rs| rs.attempts),
            max_retries = policy.max_retries,
            %next_retry_at,
            "Scheduling retry"
        );

        Ok(Some(WorkflowStatus::InProgress))
    }

    /// Settle the outcome of task execution: persist results or errors, release claim.
    #[tracing::instrument(
        name = "settle_result",
        skip_all,
        fields(worker_id = %self.worker_id, instance_id = %available_task.instance_id, task_id = %available_task.task_id),
    )]
    async fn settle_execution_result<C, Input, M>(
        &self,
        outcome: ExecutionOutcome,
        workflow: &Workflow<C, Input, M>,
        available_task: &AvailableTask,
        snapshot: &mut WorkflowSnapshot,
        claim: ActiveTaskClaim<'_, B>,
    ) -> Result<WorkflowStatus, crate::error::RuntimeError>
    where
        Input: Send + 'static,
        M: Send + Sync + 'static,
        C: Codec
            + EnvelopeCodec
            + sealed::DecodeValue<Input>
            + sealed::EncodeValue<Input>
            + 'static,
    {
        tracing::debug!("settling execution result");
        match outcome {
            ExecutionOutcome::Timeout(err) => {
                match self
                    .settle_failure(
                        workflow.task_index(),
                        available_task,
                        snapshot,
                        claim,
                        &err.to_string(),
                    )
                    .await
                {
                    Some(status) => Ok(status),
                    None => Err(err),
                }
            }
            ExecutionOutcome::Panic(panic_payload) => {
                let panic_msg = extract_panic_message(&panic_payload);
                match self
                    .settle_failure(
                        workflow.task_index(),
                        available_task,
                        snapshot,
                        claim,
                        &panic_msg,
                    )
                    .await
                {
                    Some(status) => Ok(status),
                    None => Err(WorkflowError::TaskPanicked(panic_msg).into()),
                }
            }
            ExecutionOutcome::TaskError(e) => {
                match self
                    .settle_failure(
                        workflow.task_index(),
                        available_task,
                        snapshot,
                        claim,
                        &e.to_string(),
                    )
                    .await
                {
                    Some(status) => Ok(status),
                    None => Err(e),
                }
            }
            ExecutionOutcome::Cancelled => self.settle_cancelled(available_task, claim).await,
            ExecutionOutcome::ClaimLost => Ok(Self::settle_claim_lost(available_task)),
            ExecutionOutcome::Success(output) => {
                snapshot.clear_retry_state(&available_task.task_id);
                if !self
                    .commit_task_result(
                        workflow.continuation(),
                        available_task,
                        snapshot,
                        output.clone(),
                        claim,
                    )
                    .await?
                {
                    return Ok(Self::settle_claim_lost(available_task));
                }
                // After saving the body task result, resolve any parent loop
                // nodes so that is_workflow_complete() can detect loop completion.
                Self::resolve_loop_completions(
                    workflow.continuation(),
                    snapshot,
                    self.backend.as_ref(),
                )
                .await?;
                self.determine_post_task_status(
                    workflow.continuation(),
                    available_task,
                    snapshot,
                    output,
                )
                .await
            }
        }
    }

    /// Validate task preconditions without side effects.
    ///
    /// Checks definition hash match, task existence in continuation,
    /// and that the task is not already completed in the snapshot.
    /// Returns `Ok(true)` if the task should be skipped (already completed).
    fn validate_task_preconditions(
        definition_hash: &sayiir_core::DefinitionHash,
        task_index: &sayiir_core::TaskIndex,
        available_task: &AvailableTask,
        snapshot: &WorkflowSnapshot,
    ) -> Result<bool, crate::error::RuntimeError> {
        if available_task.workflow_definition_hash != *definition_hash {
            return Err(WorkflowError::DefinitionMismatch {
                expected: *definition_hash,
                found: available_task.workflow_definition_hash,
            }
            .into());
        }

        if !task_index.contains(&available_task.task_id) {
            tracing::error!(
                instance_id = %available_task.instance_id,
                task_id = %available_task.task_id,
                "Task does not exist in workflow"
            );
            return Err(WorkflowError::TaskNotFound(available_task.task_id.to_hex()).into());
        }

        if snapshot.get_task_result(&available_task.task_id).is_some() {
            tracing::debug!(
                instance_id = %available_task.instance_id,
                task_id = %available_task.task_id,
                "Task already completed, skipping"
            );
            return Ok(true);
        }

        Ok(false)
    }

    /// Acquire a claim on the task, returning an `ActiveTaskClaim`.
    ///
    /// Returns `None` if already claimed by another worker.
    async fn claim_task(
        &self,
        available_task: &AvailableTask,
    ) -> Result<Option<ActiveTaskClaim<'_, B>>, crate::error::RuntimeError> {
        let claim = self
            .backend
            .claim_task(
                &available_task.instance_id,
                &available_task.task_id,
                &self.worker_id,
                self.claim_ttl
                    .and_then(|d| chrono::Duration::from_std(d).ok()),
            )
            .await?;

        if claim.is_some() {
            tracing::debug!(
                instance_id = %available_task.instance_id,
                task_id = %available_task.task_id,
                "Claim successful"
            );
            Ok(Some(ActiveTaskClaim {
                backend: &self.backend,
                instance_id: Arc::clone(&available_task.instance_id),
                task_id: available_task.task_id,
                worker_id: self.worker_id.clone(),
            }))
        } else {
            tracing::debug!(
                instance_id = %available_task.instance_id,
                task_id = %available_task.task_id,
                "Task was already claimed by another worker"
            );
            Ok(None)
        }
    }

    /// Check cancel/pause guards after claiming.
    ///
    /// Returns `Some(status)` if the workflow is cancelled or paused
    /// (caller should release claim and return status).
    /// Returns `None` if execution should proceed.
    ///
    /// Uses the combined `check_control_signals`, which backends can
    /// answer with a single probe in the (overwhelmingly common) case of
    /// no pending signal.
    async fn check_post_claim_guards(
        &self,
        available_task: &AvailableTask,
    ) -> Result<Option<WorkflowStatus>, crate::error::RuntimeError> {
        match self
            .backend
            .check_control_signals(&available_task.instance_id, Some(available_task.task_id))
            .await?
        {
            Some(signal) => Ok(Some(
                self.signal_status(signal, available_task, "releasing claim")
                    .await,
            )),
            None => Ok(None),
        }
    }

    /// Resolve an applied control signal into the workflow status to
    /// report, with a uniform log line (`context` says where it was caught).
    async fn signal_status(
        &self,
        signal: ControlSignal,
        available_task: &AvailableTask,
        context: &str,
    ) -> WorkflowStatus {
        match signal {
            ControlSignal::Cancelled => {
                tracing::info!(
                    instance_id = %available_task.instance_id,
                    task_id = %available_task.task_id,
                    "Workflow was cancelled, {context}"
                );
                self.load_cancelled_status(&available_task.instance_id)
                    .await
            }
            ControlSignal::Paused => {
                tracing::info!(
                    instance_id = %available_task.instance_id,
                    task_id = %available_task.task_id,
                    "Workflow was paused, {context}"
                );
                self.load_paused_status(&available_task.instance_id).await
            }
        }
    }

    /// Execute a future while periodically extending the task claim.
    ///
    /// Heartbeats fire at `claim_ttl / 4`, capped at
    /// [`HEARTBEAT_MAX_INTERVAL`], so a crashed peer's stall window is
    /// bounded by the TTL while a live worker renews with margin to spare.
    /// Each tick also:
    ///
    /// - checks the task deadline — if expired, the future is dropped
    ///   (active cancellation) and `Timeout` is returned;
    /// - probes for a pending cancel signal — long-running tasks get
    ///   cancelled cooperatively instead of running to completion;
    /// - escalates lease loss: a `NotFound`/ownership failure from
    ///   `extend_task_claim`, or [`HEARTBEAT_FAILURE_LIMIT`] consecutive
    ///   failures of any kind, aborts the task as `ClaimLost` so a stale
    ///   worker can't keep computing a result it may no longer commit.
    #[tracing::instrument(
        name = "task",
        skip_all,
        fields(worker_id = %self.worker_id, instance_id = %claim.instance_id, task_id = %claim.task_id),
    )]
    async fn run_with_heartbeat<F, T>(
        &self,
        claim: &ActiveTaskClaim<'_, B>,
        deadline: Option<&TaskDeadline>,
        future: F,
    ) -> Result<T, HeartbeatInterrupt>
    where
        F: std::future::Future<Output = T>,
    {
        tracing::debug!("running task with heartbeat");
        let Some(ttl) = self.claim_ttl else {
            return Ok(future.await);
        };
        let Some(chrono_ttl) = chrono::Duration::from_std(ttl).ok() else {
            return Ok(future.await);
        };

        let interval_duration = (ttl / 4)
            .min(HEARTBEAT_MAX_INTERVAL)
            .max(Duration::from_millis(1));
        let mut heartbeat_timer = time::interval(interval_duration);
        heartbeat_timer.tick().await; // skip first immediate tick
        let mut consecutive_failures: u32 = 0;

        tokio::pin!(future);

        loop {
            tokio::select! {
                result = &mut future => break Ok(result),
                _ = heartbeat_timer.tick() => {
                    // Check deadline during heartbeat
                    if let Some(dl) = deadline
                        && chrono::Utc::now() >= dl.deadline
                    {
                        tracing::warn!(
                            instance_id = %claim.instance_id,
                            task_id = %dl.task_id,
                            "Task deadline expired during heartbeat, cancelling"
                        );
                        return Err(HeartbeatInterrupt::Timeout(
                            crate::error::RuntimeError::from(WorkflowError::TaskTimedOut {
                                task_id: dl.task_id,
                                timeout: std::time::Duration::from_millis(dl.timeout_ms),
                            }),
                        ));
                    }

                    tracing::trace!(
                        instance_id = %claim.instance_id,
                        task_id = %claim.task_id,
                        "Extending task claim via heartbeat"
                    );
                    // Cooperative-cancel probe and lease extension are
                    // independent — run them in one round-trip's latency.
                    let (cancel, extended) = tokio::join!(
                        self.backend
                            .get_signal(&claim.instance_id, SignalKind::Cancel),
                        self.backend.extend_task_claim(
                            &claim.instance_id,
                            &claim.task_id,
                            &claim.worker_id,
                            chrono_ttl,
                        ),
                    );

                    // Don't let a long task run to completion under a
                    // cancel that arrived mid-flight.
                    if let Ok(Some(_)) = cancel {
                        tracing::info!(
                            instance_id = %claim.instance_id,
                            task_id = %claim.task_id,
                            "Cancel signal during heartbeat, dropping task future"
                        );
                        return Err(HeartbeatInterrupt::Cancelled);
                    }

                    match extended {
                        Ok(()) => consecutive_failures = 0,
                        // Claim row gone or re-owned: the lease is
                        // structurally lost, not flaky — abort now.
                        Err(
                            e @ (sayiir_persistence::BackendError::NotFound(_)
                            | sayiir_persistence::BackendError::ClaimLost(_)),
                        ) => {
                            tracing::warn!(
                                instance_id = %claim.instance_id,
                                task_id = %claim.task_id,
                                error = %e,
                                "Claim no longer held, abandoning task"
                            );
                            return Err(HeartbeatInterrupt::ClaimLost);
                        }
                        Err(e) => {
                            consecutive_failures += 1;
                            tracing::warn!(
                                instance_id = %claim.instance_id,
                                task_id = %claim.task_id,
                                error = %e,
                                consecutive_failures,
                                "Failed to extend task claim"
                            );
                            if consecutive_failures >= HEARTBEAT_FAILURE_LIMIT {
                                return Err(HeartbeatInterrupt::ClaimLost);
                            }
                        }
                    }
                }
            }
        }
    }

    /// Persist task result and release the claim.
    ///
    /// Returns `Ok(false)` (without persisting) when the claim fence
    /// rejects the write — the lease was lost mid-execution and the new
    /// claimant owns the task's outcome.
    async fn commit_task_result(
        &self,
        continuation: &WorkflowContinuation,
        available_task: &AvailableTask,
        snapshot: &mut WorkflowSnapshot,
        output: Bytes,
        claim: ActiveTaskClaim<'_, B>,
    ) -> Result<bool, crate::error::RuntimeError> {
        snapshot.mark_task_completed(available_task.task_id, output);
        tracing::debug!(
            instance_id = %available_task.instance_id,
            task_id = %available_task.task_id,
            "Task completed"
        );

        Self::update_position_after_task(continuation, &available_task.task_id, snapshot)?;
        #[cfg(feature = "otel")]
        {
            snapshot.trace_parent = crate::trace_context::current_trace_parent();
        }
        if !self
            .backend
            .save_snapshot_fenced(snapshot, &claim.task_id, &claim.worker_id)
            .await?
        {
            return Ok(false);
        }

        // If we just entered AtSignal, drain any event that was
        // buffered while the snapshot was still at AtTask. The
        // race: send_event takes the lock during this worker's task
        // body (status='InProgress', position=AtTask), sees no
        // AtSignal to resume, INSERTs into sayiir_workflow_events,
        // and commits. Then this save_snapshot advances to AtSignal,
        // but PooledWorker has no AwaitSignal poll loop, so the
        // workflow would sit forever with a matching event sitting
        // unconsumed in the buffer. Closing the race here keeps the
        // signal-driven dispatch path single-source (send_event is
        // still primary; this is the worker's belt-and-suspenders).
        self.drain_pending_signal(&available_task.instance_id, snapshot)
            .await?;

        claim.release().await?;
        Ok(true)
    }

    /// If `snapshot` is parked at `AtSignal { signal_name }` and a
    /// matching event sits buffered in `sayiir_workflow_events`,
    /// consume it and advance the snapshot to its next position. Loops
    /// in case advancement lands on another `AtSignal` whose buffered
    /// event is also already waiting.
    async fn drain_pending_signal(
        &self,
        instance_id: &Arc<str>,
        snapshot: &mut WorkflowSnapshot,
    ) -> Result<(), crate::error::RuntimeError> {
        loop {
            let (signal_id, signal_name, next_task_id) = match &snapshot.state {
                sayiir_core::snapshot::WorkflowSnapshotState::InProgress {
                    position:
                        sayiir_core::snapshot::ExecutionPosition::AtSignal {
                            signal_id,
                            signal_name,
                            next_task_id,
                            ..
                        },
                    ..
                } => (*signal_id, signal_name.clone(), *next_task_id),
                _ => return Ok(()),
            };

            let Some(payload) = self
                .backend
                .consume_event(instance_id, &signal_name)
                .await?
            else {
                return Ok(());
            };

            tracing::debug!(
                instance_id = %instance_id,
                %signal_name,
                "draining buffered signal that landed during the AtTask→AtSignal transition"
            );
            snapshot.mark_task_completed(signal_id, payload.clone());
            if let Some(next_id) = next_task_id {
                snapshot.update_position(sayiir_core::snapshot::ExecutionPosition::AtTask {
                    task_id: next_id,
                });
            } else {
                snapshot.mark_completed(payload);
            }
            self.backend.save_snapshot(snapshot).await?;
        }
    }

    /// Determine workflow status after a task completes.
    ///
    /// Checks cancel/pause guards (single combined round-trip in the
    /// common no-signal case) and workflow completion.
    async fn determine_post_task_status(
        &self,
        continuation: &WorkflowContinuation,
        available_task: &AvailableTask,
        snapshot: &mut WorkflowSnapshot,
        output: Bytes,
    ) -> Result<WorkflowStatus, crate::error::RuntimeError> {
        if let Some(signal) = self
            .backend
            .check_control_signals(&available_task.instance_id, None)
            .await?
        {
            return Ok(self
                .signal_status(signal, available_task, "after task completion")
                .await);
        }

        if Self::is_workflow_complete(continuation, snapshot) {
            tracing::info!(
                instance_id = %available_task.instance_id,
                task_id = %available_task.task_id,
                "Workflow complete"
            );
            snapshot.mark_completed(output);
            self.backend.save_snapshot(snapshot).await?;
            Ok(WorkflowStatus::Completed)
        } else {
            tracing::debug!(
                instance_id = %available_task.instance_id,
                task_id = %available_task.task_id,
                "Task completed, workflow continues"
            );
            Ok(WorkflowStatus::InProgress)
        }
    }

    /// If `task_id` is the entry task of a `Fork`'s join continuation,
    /// return the slice of branches whose outputs feed the join. The
    /// `PooledWorker` dispatches each task with `available_task.input`
    /// set to the previous task's output — for a join, that's just one
    /// branch's output, not the `NamedBranchResults` the join body
    /// expects. Callers use this to rebuild the join input from
    /// `snapshot.completed_tasks` before invoking the join task.
    fn find_fork_branches_for_join<'a>(
        continuation: &'a WorkflowContinuation,
        task_id: &sayiir_core::TaskId,
    ) -> Option<&'a [Arc<WorkflowContinuation>]> {
        match continuation {
            WorkflowContinuation::Task { next, .. }
            | WorkflowContinuation::Delay { next, .. }
            | WorkflowContinuation::AwaitSignal { next, .. } => next
                .as_deref()
                .and_then(|n| Self::find_fork_branches_for_join(n, task_id)),
            WorkflowContinuation::Fork { branches, join, .. } => {
                if let Some(join_cont) = join {
                    let join_first = sayiir_core::TaskId::from(join_cont.first_task_id());
                    if join_first == *task_id {
                        return Some(&branches[..]);
                    }
                    if let Some(b) = Self::find_fork_branches_for_join(join_cont, task_id) {
                        return Some(b);
                    }
                }
                for branch in branches {
                    if let Some(b) = Self::find_fork_branches_for_join(branch, task_id) {
                        return Some(b);
                    }
                }
                None
            }
            WorkflowContinuation::Branch {
                branches,
                default,
                next,
                ..
            } => {
                for branch_cont in branches.values() {
                    if let Some(b) = Self::find_fork_branches_for_join(branch_cont, task_id) {
                        return Some(b);
                    }
                }
                if let Some(def) = default
                    && let Some(b) = Self::find_fork_branches_for_join(def, task_id)
                {
                    return Some(b);
                }
                next.as_deref()
                    .and_then(|n| Self::find_fork_branches_for_join(n, task_id))
            }
            WorkflowContinuation::Loop { body, next, .. } => {
                Self::find_fork_branches_for_join(body, task_id).or_else(|| {
                    next.as_deref()
                        .and_then(|n| Self::find_fork_branches_for_join(n, task_id))
                })
            }
            WorkflowContinuation::ChildWorkflow { child, next, .. } => {
                Self::find_fork_branches_for_join(child, task_id).or_else(|| {
                    next.as_deref()
                        .and_then(|n| Self::find_fork_branches_for_join(n, task_id))
                })
            }
        }
    }

    /// Find a task function in the workflow continuation and return a reference.
    ///
    /// Tree-walk predicate: does `task_id` appear anywhere inside `continuation`?
    ///
    /// Only used by [`execute_task_by_id`] to pick which subtree to descend into
    /// at a `Fork`/`Branch`/`Loop`/`ChildWorkflow` boundary. Validation on the
    /// dispatch hot path uses [`sayiir_core::TaskIndex::contains`] instead.
    fn find_task_id_in_continuation(
        continuation: &WorkflowContinuation,
        task_id: &sayiir_core::TaskId,
    ) -> bool {
        match continuation {
            WorkflowContinuation::Task { id, next, .. }
            | WorkflowContinuation::Delay { id, next, .. }
            | WorkflowContinuation::AwaitSignal { id, next, .. } => {
                if sayiir_core::TaskId::from(id.as_str()) == *task_id {
                    return true;
                }
                next.as_ref()
                    .is_some_and(|n| Self::find_task_id_in_continuation(n, task_id))
            }
            WorkflowContinuation::Fork { branches, join, .. } => {
                for branch in branches {
                    if Self::find_task_id_in_continuation(branch, task_id) {
                        return true;
                    }
                }
                if let Some(join_cont) = join {
                    Self::find_task_id_in_continuation(join_cont, task_id)
                } else {
                    false
                }
            }
            WorkflowContinuation::Branch {
                branches,
                default,
                next,
                ..
            } => {
                for branch_cont in branches.values() {
                    if Self::find_task_id_in_continuation(branch_cont, task_id) {
                        return true;
                    }
                }
                if let Some(def) = default
                    && Self::find_task_id_in_continuation(def, task_id)
                {
                    return true;
                }
                next.as_ref()
                    .is_some_and(|n| Self::find_task_id_in_continuation(n, task_id))
            }
            WorkflowContinuation::Loop { body, next, .. } => {
                if Self::find_task_id_in_continuation(body, task_id) {
                    return true;
                }
                next.as_ref()
                    .is_some_and(|n| Self::find_task_id_in_continuation(n, task_id))
            }
            WorkflowContinuation::ChildWorkflow { child, next, .. } => {
                if Self::find_task_id_in_continuation(child, task_id) {
                    return true;
                }
                next.as_ref()
                    .is_some_and(|n| Self::find_task_id_in_continuation(n, task_id))
            }
        }
    }

    /// Execute a task by ID from the workflow continuation (iterative, no boxing).
    #[allow(clippy::manual_async_fn)]
    fn execute_task_by_id<'a>(
        continuation: &'a WorkflowContinuation,
        task_id: &'a str,
        input: Bytes,
    ) -> impl std::future::Future<Output = Result<Bytes, crate::error::RuntimeError>> + Send + 'a
    {
        async move {
            let task_id_hash = sayiir_core::TaskId::from(task_id);
            let task_id = &task_id_hash;
            let mut current = continuation;

            loop {
                match current {
                    WorkflowContinuation::Task { id, func, next, .. } => {
                        if sayiir_core::TaskId::from(id.as_str()) == *task_id {
                            let func = func
                                .as_ref()
                                .ok_or_else(|| WorkflowError::TaskNotImplemented(id.clone()))?;
                            return Ok(func.run(input).await?);
                        } else if let Some(next_cont) = next {
                            current = next_cont;
                        } else {
                            return Err(WorkflowError::TaskNotFound(task_id.to_string()).into());
                        }
                    }
                    WorkflowContinuation::Delay { next, .. }
                    | WorkflowContinuation::AwaitSignal { next, .. } => {
                        // Skip over delay/signal nodes when searching for a task
                        if let Some(next_cont) = next {
                            current = next_cont;
                        } else {
                            return Err(WorkflowError::TaskNotFound(task_id.to_string()).into());
                        }
                    }
                    WorkflowContinuation::Fork { branches, join, .. } => {
                        // Check branches
                        let mut found_in_branch = false;
                        for branch in branches {
                            if Self::find_task_id_in_continuation(branch, task_id) {
                                current = branch;
                                found_in_branch = true;
                                break;
                            }
                        }
                        if found_in_branch {
                            continue;
                        }
                        // Check join
                        if let Some(join_cont) = join {
                            current = join_cont;
                        } else {
                            return Err(WorkflowError::TaskNotFound(task_id.to_string()).into());
                        }
                    }
                    WorkflowContinuation::Branch {
                        branches,
                        default,
                        next,
                        ..
                    } => {
                        // Search branch sub-continuations for the task
                        let mut found = false;
                        for branch_cont in branches.values() {
                            if Self::find_task_id_in_continuation(branch_cont, task_id) {
                                current = branch_cont;
                                found = true;
                                break;
                            }
                        }
                        if found {
                            continue;
                        }
                        if let Some(def) = default
                            && Self::find_task_id_in_continuation(def, task_id)
                        {
                            current = def;
                            continue;
                        }
                        if let Some(next_cont) = next {
                            current = next_cont;
                        } else {
                            return Err(WorkflowError::TaskNotFound(task_id.to_string()).into());
                        }
                    }
                    WorkflowContinuation::Loop { body, next, .. } => {
                        if Self::find_task_id_in_continuation(body, task_id) {
                            current = body;
                            continue;
                        }
                        if let Some(next_cont) = next {
                            current = next_cont;
                        } else {
                            return Err(WorkflowError::TaskNotFound(task_id.to_string()).into());
                        }
                    }
                    WorkflowContinuation::ChildWorkflow { child, next, .. } => {
                        if Self::find_task_id_in_continuation(child, task_id) {
                            current = child;
                            continue;
                        }
                        if let Some(next_cont) = next {
                            current = next_cont;
                        } else {
                            return Err(WorkflowError::TaskNotFound(task_id.to_string()).into());
                        }
                    }
                }
            }
        }
    }

    /// Set the snapshot's execution position to reflect "execution is now at
    /// the head of `cont`".
    ///
    /// For Task heads (and Branch / Fork / Loop / `ChildWorkflow` which all
    /// start with a Task-shaped node), set `AtTask(first_task_hint)`. For
    /// `Delay` / `AwaitSignal` heads, enter the corresponding park state
    /// directly — using `first_task_hint(...).id` would name the Delay or
    /// Signal node itself, but `execute_task_by_id` skips past those when
    /// dispatching, so the worker would loop on a phantom task ID.
    fn set_position_at(
        cont: &WorkflowContinuation,
        snapshot: &mut WorkflowSnapshot,
    ) -> Result<(), crate::error::RuntimeError> {
        use crate::execution::control_flow::{compute_signal_timeout, compute_wake_at};
        match cont {
            WorkflowContinuation::Delay { id, duration, next } => {
                let wake_at = compute_wake_at(duration)?;
                let entered_at = chrono::Utc::now();
                let next_hint = next.as_deref().map(WorkflowContinuation::first_task_hint);
                let next_task_id = next_hint.as_ref().map(|h| h.id);
                // Update task_priority/task_tags so backends advancing
                // AtDelay -> AtTask inherit the next task's routing hints
                // (mirrors save_park_checkpoint).
                snapshot.set_task_hint(next_hint.as_ref().unwrap_or(&TaskHint::default()));
                let delay_id = sayiir_core::TaskId::from(id.as_str());
                snapshot.update_position(ExecutionPosition::AtDelay {
                    delay_id,
                    entered_at,
                    wake_at,
                    next_task_id,
                });
                // Mirror the executor's behaviour when it parks at a delay:
                // mark the delay node as completed with the passthrough input
                // (which is the *last* completed task's output, sitting at the
                // top of `completed_tasks`) so downstream nodes see the same
                // continuous output chain.
                let passthrough = snapshot.get_last_task_output().unwrap_or_default();
                snapshot.mark_task_completed(delay_id, passthrough);
            }
            WorkflowContinuation::AwaitSignal {
                id,
                signal_name,
                timeout,
                next,
            } => {
                let wake_at = compute_signal_timeout(timeout.as_ref());
                let next_hint = next.as_deref().map(WorkflowContinuation::first_task_hint);
                let next_task_id = next_hint.as_ref().map(|h| h.id);
                // Update task_priority/task_tags so backends advancing
                // AtSignal -> AtTask inherit the next task's routing hints
                // (mirrors save_park_checkpoint).
                snapshot.set_task_hint(next_hint.as_ref().unwrap_or(&TaskHint::default()));
                snapshot.update_position(ExecutionPosition::AtSignal {
                    signal_id: sayiir_core::TaskId::from(id.as_str()),
                    signal_name: signal_name.clone(),
                    wake_at,
                    next_task_id,
                });
            }
            _ => {
                let hint = cont.first_task_hint();
                snapshot.update_position(ExecutionPosition::AtTask { task_id: hint.id });
                snapshot.set_task_hint(&hint);
            }
        }
        Ok(())
    }

    /// Update execution position after a task completes.
    fn update_position_after_task(
        continuation: &WorkflowContinuation,
        completed_task_id: &sayiir_core::TaskId,
        snapshot: &mut WorkflowSnapshot,
    ) -> Result<(), crate::error::RuntimeError> {
        match continuation {
            WorkflowContinuation::Task { id, next, .. }
            | WorkflowContinuation::Delay { id, next, .. }
            | WorkflowContinuation::AwaitSignal { id, next, .. } => {
                if sayiir_core::TaskId::from(id.as_str()) == *completed_task_id {
                    if let Some(next_cont) = next.as_deref() {
                        Self::set_position_at(next_cont, snapshot)?;
                    }
                } else if let Some(next_cont) = next {
                    Self::update_position_after_task(next_cont, completed_task_id, snapshot)?;
                }
            }
            WorkflowContinuation::Fork { branches, join, .. } => {
                // First let recursion advance position WITHIN a branch (for
                // multi-task branch chains) or within the join continuation.
                for branch in branches {
                    Self::update_position_after_task(branch, completed_task_id, snapshot)?;
                }
                if let Some(join_cont) = join {
                    Self::update_position_after_task(join_cont, completed_task_id, snapshot)?;
                }

                // If recursion didn't move the position off the just-completed
                // task, we finished a branch's terminal node — the inner Task
                // arm has no `next` to advance to. Drive the fork forward at
                // this level: pick the next branch that hasn't started, or
                // step into the join when every branch is done. Without this
                // step, single-Task branches (the common fan-out shape) leave
                // the snapshot pinned to `AtTask(branch_n)` forever and the
                // next dispatch just re-discovers the same already-completed
                // task — workflow stalls indefinitely.
                let still_at_completed = snapshot
                    .current_task_id()
                    .is_some_and(|c| c == *completed_task_id);
                if !still_at_completed {
                    return Ok(());
                }

                // Sequential branch execution: the first branch whose entry
                // task has no result yet is where we resume. Matches
                // `collect_cached_branches`' lookup so the two views of "branch
                // is done" stay in sync.
                for branch in branches {
                    let first_tid = sayiir_core::TaskId::from(branch.first_task_id());
                    if snapshot.get_task_result(&first_tid).is_none() {
                        Self::set_position_at(branch, snapshot)?;
                        return Ok(());
                    }
                }

                // Every branch finished — advance to the join. If the fork has
                // no join, leave the position at the last completed task and
                // let `is_workflow_complete` mark the workflow done.
                if let Some(join_cont) = join {
                    Self::set_position_at(join_cont, snapshot)?;
                }
            }
            WorkflowContinuation::Branch {
                branches,
                default,
                next,
                ..
            } => {
                for branch_cont in branches.values() {
                    Self::update_position_after_task(branch_cont, completed_task_id, snapshot)?;
                }
                if let Some(def) = default {
                    Self::update_position_after_task(def, completed_task_id, snapshot)?;
                }
                if let Some(next_cont) = next {
                    Self::update_position_after_task(next_cont, completed_task_id, snapshot)?;
                }
            }
            WorkflowContinuation::Loop { body, next, .. } => {
                Self::update_position_after_task(body, completed_task_id, snapshot)?;
                if let Some(next_cont) = next {
                    Self::update_position_after_task(next_cont, completed_task_id, snapshot)?;
                }
            }
            WorkflowContinuation::ChildWorkflow { child, next, .. } => {
                Self::update_position_after_task(child, completed_task_id, snapshot)?;
                if let Some(next_cont) = next {
                    Self::update_position_after_task(next_cont, completed_task_id, snapshot)?;
                }
            }
        }
        Ok(())
    }

    /// Create a builder with sensible defaults.
    ///
    /// By default, the worker ID is derived from `{hostname}-{pid}`.
    /// Override with [`PooledWorkerBuilder::worker_id`].
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use sayiir_runtime::prelude::*;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// // Auto-generated worker ID from hostname + PID
    /// let worker = PooledWorker::builder(InMemoryBackend::new(), TaskRegistry::new()).build();
    ///
    /// // Or override with explicit ID
    /// let worker = PooledWorker::builder(InMemoryBackend::new(), TaskRegistry::new())
    ///     .worker_id("custom-worker-1")
    ///     .build();
    /// # Ok(())
    /// # }
    /// ```
    pub fn builder(backend: B, registry: TaskRegistry) -> PooledWorkerBuilder<B> {
        PooledWorkerBuilder {
            worker_id: None,
            backend,
            registry,
            claim_ttl: Some(Duration::from_mins(5)),
            batch_size: NonZeroUsize::MIN,
            max_concurrent_tasks: None,
            aging_interval: Duration::from_mins(5),
            tags: vec![],
        }
    }

    /// Walk the continuation tree and resolve any Loop nodes whose body has
    /// completed. When the terminal body task's output decodes to
    /// `LoopDecision::Done`, the loop node is marked as completed in the
    /// snapshot. For `Again`, body task results are cleared and the iteration
    /// counter is advanced so the body becomes available for re-execution.
    async fn resolve_loop_completions(
        continuation: &WorkflowContinuation,
        snapshot: &mut WorkflowSnapshot,
        backend: &B,
    ) -> Result<(), crate::error::RuntimeError> {
        Self::resolve_loops_recursive(continuation, snapshot, backend).await
    }

    #[allow(clippy::too_many_lines)]
    fn resolve_loops_recursive<'a>(
        continuation: &'a WorkflowContinuation,
        snapshot: &'a mut WorkflowSnapshot,
        backend: &'a B,
    ) -> Pin<
        Box<dyn std::future::Future<Output = Result<(), crate::error::RuntimeError>> + Send + 'a>,
    > {
        Box::pin(async move {
            match continuation {
                WorkflowContinuation::Loop {
                    id,
                    body,
                    max_iterations,
                    on_max,
                    next,
                } => {
                    // Only resolve if the loop isn't already marked complete.
                    if snapshot
                        .get_task_result(&sayiir_core::TaskId::from(id))
                        .is_none()
                    {
                        let terminal_id = body.terminal_task_id();
                        if let Some(result) =
                            snapshot.get_task_result(&sayiir_core::TaskId::from(terminal_id))
                        {
                            let output = result.output.clone();
                            match crate::execution::decode_loop_envelope(&output) {
                                Ok((LoopDecision::Done, inner)) => {
                                    snapshot.clear_loop_iteration(&sayiir_core::TaskId::from(id));
                                    snapshot
                                        .mark_task_completed(sayiir_core::TaskId::from(id), inner);
                                    backend.save_snapshot(snapshot).await?;
                                }
                                Ok((LoopDecision::Again, again_value)) => {
                                    let current_iter =
                                        snapshot.loop_iteration(&sayiir_core::TaskId::from(id));
                                    let next_iter = current_iter + 1;
                                    if next_iter >= *max_iterations {
                                        match on_max {
                                            sayiir_core::workflow::MaxIterationsPolicy::Fail => {
                                                return Err(WorkflowError::MaxIterationsExceeded {
                                                    loop_id: sayiir_core::TaskId::from(id),
                                                    max_iterations: *max_iterations,
                                                }
                                                .into());
                                            }
                                            sayiir_core::workflow::MaxIterationsPolicy::ExitWithLast => {
                                                snapshot.clear_loop_iteration(&sayiir_core::TaskId::from(id));
                                                snapshot.mark_task_completed(
                                                    sayiir_core::TaskId::from(id.as_str()),
                                                    again_value,
                                                );
                                                backend.save_snapshot(snapshot).await?;
                                            }
                                        }
                                    } else {
                                        // Clear body task results so the body becomes
                                        // available for re-execution on the next poll.
                                        let body_ser = body.to_serializable();
                                        for tid in &body_ser.task_ids() {
                                            snapshot.remove_task_result(
                                                &sayiir_core::TaskId::from(*tid),
                                            );
                                        }
                                        snapshot.set_loop_iteration(
                                            sayiir_core::TaskId::from(id),
                                            next_iter,
                                        );
                                        backend.save_snapshot(snapshot).await?;
                                    }
                                }
                                Err(e) => {
                                    return Err(CodecError::DecodeFailed {
                                        task_id: sayiir_core::TaskId::from(id),
                                        expected_type: "LoopEnvelope",
                                        source: e,
                                    }
                                    .into());
                                }
                            }
                        }
                    }
                    // Recurse into body and next.
                    Self::resolve_loops_recursive(body, snapshot, backend).await?;
                    if let Some(next) = next {
                        Self::resolve_loops_recursive(next, snapshot, backend).await?;
                    }
                }
                WorkflowContinuation::Task { next, .. }
                | WorkflowContinuation::Delay { next, .. }
                | WorkflowContinuation::AwaitSignal { next, .. }
                | WorkflowContinuation::Branch { next, .. } => {
                    if let Some(next) = next {
                        Self::resolve_loops_recursive(next, snapshot, backend).await?;
                    }
                }
                WorkflowContinuation::Fork { branches, join, .. } => {
                    for branch in branches {
                        Self::resolve_loops_recursive(branch, snapshot, backend).await?;
                    }
                    if let Some(join) = join {
                        Self::resolve_loops_recursive(join, snapshot, backend).await?;
                    }
                }
                WorkflowContinuation::ChildWorkflow { child, next, .. } => {
                    Self::resolve_loops_recursive(child, snapshot, backend).await?;
                    if let Some(next) = next {
                        Self::resolve_loops_recursive(next, snapshot, backend).await?;
                    }
                }
            }
            Ok(())
        })
    }

    /// Check if the workflow is complete based on the snapshot.
    fn is_workflow_complete(
        continuation: &WorkflowContinuation,
        snapshot: &WorkflowSnapshot,
    ) -> bool {
        // Check if all tasks in the continuation are completed
        match continuation {
            WorkflowContinuation::Task { id, next, .. } => {
                if snapshot
                    .get_task_result(&sayiir_core::TaskId::from(id))
                    .is_none()
                {
                    return false;
                }
                if let Some(next_cont) = next {
                    Self::is_workflow_complete(next_cont, snapshot)
                } else {
                    true // Last task completed
                }
            }
            WorkflowContinuation::Delay { id, next, .. }
            | WorkflowContinuation::AwaitSignal { id, next, .. } => {
                if snapshot
                    .get_task_result(&sayiir_core::TaskId::from(id))
                    .is_none()
                {
                    return false;
                }
                next.as_ref()
                    .is_none_or(|n| Self::is_workflow_complete(n, snapshot))
            }
            WorkflowContinuation::Fork { branches, join, .. } => {
                // All branches must be completed (recursively check entire branch chain)
                for branch in branches {
                    if !Self::is_workflow_complete(branch, snapshot) {
                        return false;
                    }
                }
                // Join must be completed if it exists
                if let Some(join_cont) = join {
                    Self::is_workflow_complete(join_cont, snapshot)
                } else {
                    true
                }
            }
            WorkflowContinuation::Branch { id, next, .. } => {
                // Branch is complete when the branch node itself has a cached result
                if snapshot
                    .get_task_result(&sayiir_core::TaskId::from(id))
                    .is_none()
                {
                    return false;
                }
                next.as_ref()
                    .is_none_or(|n| Self::is_workflow_complete(n, snapshot))
            }
            WorkflowContinuation::Loop { id, next, .. } => {
                // Loop is complete when the loop node itself has a cached result
                if snapshot
                    .get_task_result(&sayiir_core::TaskId::from(id))
                    .is_none()
                {
                    return false;
                }
                next.as_ref()
                    .is_none_or(|n| Self::is_workflow_complete(n, snapshot))
            }
            WorkflowContinuation::ChildWorkflow { id, next, .. } => {
                // ChildWorkflow is complete when the node itself has a cached result
                if snapshot
                    .get_task_result(&sayiir_core::TaskId::from(id))
                    .is_none()
                {
                    return false;
                }
                next.as_ref()
                    .is_none_or(|n| Self::is_workflow_complete(n, snapshot))
            }
        }
    }
}

/// Generate a default worker ID from `{hostname}-{pid}`.
fn default_worker_id() -> String {
    let host = hostname::get()
        .ok()
        .and_then(|h| h.into_string().ok())
        .unwrap_or_else(|| "unknown".to_string());
    format!("{host}-{}", std::process::id())
}

/// Builder for [`PooledWorker`] with sensible defaults.
///
/// By default, derives the worker ID from `{hostname}-{pid}`.
/// Override with [`worker_id`](Self::worker_id).
///
/// Created via [`PooledWorker::builder`].
pub struct PooledWorkerBuilder<B> {
    worker_id: Option<String>,
    backend: B,
    registry: TaskRegistry,
    claim_ttl: Option<Duration>,
    batch_size: NonZeroUsize,
    max_concurrent_tasks: Option<NonZeroUsize>,
    aging_interval: Duration,
    tags: Vec<String>,
}

impl<B> PooledWorkerBuilder<B>
where
    B: PersistentBackend + TaskClaimStore + 'static,
{
    /// Set an explicit worker ID.
    ///
    /// If not called, the ID is auto-generated from `{hostname}-{pid}`.
    #[must_use]
    pub fn worker_id(mut self, id: impl Into<String>) -> Self {
        self.worker_id = Some(id.into());
        self
    }

    /// Set the TTL for task claims (default: 5 minutes).
    #[must_use]
    pub fn claim_ttl(mut self, ttl: Option<Duration>) -> Self {
        self.claim_ttl = ttl;
        self
    }

    /// Set the default in-flight task cap (default: 1); see
    /// [`PooledWorker::with_batch_size`].
    #[must_use]
    pub fn batch_size(mut self, size: NonZeroUsize) -> Self {
        self.batch_size = size;
        self
    }

    /// Set the in-flight task cap (default: follows `batch_size`).
    #[must_use]
    pub fn max_concurrent_tasks(mut self, max: NonZeroUsize) -> Self {
        self.max_concurrent_tasks = Some(max);
        self
    }

    /// Set the aging interval for priority-based scheduling (default: 300s).
    ///
    /// # Panics
    ///
    /// Panics if `interval` is zero.
    #[must_use]
    pub fn aging_interval(mut self, interval: Duration) -> Self {
        assert!(!interval.is_zero(), "aging interval must be non-zero");
        self.aging_interval = interval;
        self
    }

    /// Set affinity tags for this worker.
    ///
    /// When tags are set, the worker only picks up tasks whose tags are a
    /// subset of the worker's tags (or tasks with no tags). When no tags are
    /// set (the default), the worker accepts all tasks.
    #[must_use]
    pub fn tags(mut self, tags: Vec<String>) -> Self {
        self.tags = tags;
        self
    }

    /// Build the [`PooledWorker`].
    ///
    /// If no `worker_id` was set, generates one from `{hostname}-{pid}`.
    #[must_use]
    pub fn build(self) -> PooledWorker<B> {
        let worker_id = self.worker_id.unwrap_or_else(default_worker_id);
        PooledWorker {
            worker_id,
            backend: Arc::new(self.backend),
            registry: Arc::new(self.registry),
            claim_ttl: self.claim_ttl,
            batch_size: self.batch_size,
            max_concurrent_tasks: self.max_concurrent_tasks,
            aging_interval: self.aging_interval,
            tags: self.tags,
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::serialization::JsonCodec;
    use sayiir_core::registry::TaskRegistry;
    use sayiir_core::snapshot::WorkflowSnapshot;
    use sayiir_persistence::{InMemoryBackend, SignalStore, SnapshotStore};

    type EmptyWorkflows = WorkflowRegistry<JsonCodec, (), ()>;

    fn make_worker() -> PooledWorker<InMemoryBackend> {
        let backend = InMemoryBackend::new();
        let registry = TaskRegistry::new();
        PooledWorker::new("test-worker", backend, registry)
    }

    #[tokio::test]
    async fn test_spawn_and_shutdown() {
        let worker = make_worker();
        let handle = worker.spawn(Duration::from_millis(50), EmptyWorkflows::new());

        handle.shutdown();

        let result = tokio::time::timeout(Duration::from_secs(5), handle.join()).await;
        assert!(result.is_ok(), "Worker should exit cleanly after shutdown");
        assert!(result.unwrap().is_ok());
    }

    #[tokio::test]
    async fn test_handle_is_clone_and_send() {
        let worker = make_worker();
        let handle = worker.spawn(Duration::from_millis(50), EmptyWorkflows::new());

        let handle2 = handle.clone();
        let remote = tokio::spawn(async move {
            handle2.shutdown();
        });
        remote.await.ok();

        let result = tokio::time::timeout(Duration::from_secs(5), handle.join()).await;
        assert!(result.is_ok_and(|r| r.is_ok()));
    }

    #[tokio::test]
    async fn test_cancel_via_client() {
        let backend = InMemoryBackend::new();
        let registry = TaskRegistry::new();

        // Create a workflow snapshot so store_signal can validate it
        let mut snapshot = WorkflowSnapshot::new("wf-1", "hash-1".into());
        backend.save_snapshot(&mut snapshot).await.ok();

        let worker = PooledWorker::new("test-worker", backend, registry);
        let handle = worker.spawn(Duration::from_millis(50), EmptyWorkflows::new());

        // Cancel via WorkflowClient instead of handle
        let client = crate::WorkflowClient::from_shared(std::sync::Arc::clone(handle.backend()));
        client
            .cancel(
                "wf-1",
                Some("test reason".to_string()),
                Some("tester".to_string()),
            )
            .await
            .ok();

        // Verify the signal was stored
        let signal = handle
            .backend()
            .get_signal("wf-1", SignalKind::Cancel)
            .await;
        assert!(signal.is_ok_and(|s| s.is_some()));

        handle.shutdown();
        tokio::time::timeout(Duration::from_secs(5), handle.join())
            .await
            .ok();
    }

    #[test]
    fn test_builder_auto_generates_worker_id() {
        let backend = InMemoryBackend::new();
        let registry = TaskRegistry::new();
        let worker = PooledWorker::builder(backend, registry).build();

        // Should contain PID
        let pid = std::process::id().to_string();
        assert!(
            worker.worker_id.contains(&pid),
            "Auto-generated ID '{}' should contain PID '{}'",
            worker.worker_id,
            pid
        );
    }

    #[test]
    fn test_builder_explicit_worker_id() {
        let backend = InMemoryBackend::new();
        let registry = TaskRegistry::new();
        let worker = PooledWorker::builder(backend, registry)
            .worker_id("my-worker")
            .build();

        assert_eq!(worker.worker_id, "my-worker");
    }

    #[test]
    fn test_builder_custom_settings() {
        let backend = InMemoryBackend::new();
        let registry = TaskRegistry::new();
        let worker = PooledWorker::builder(backend, registry)
            .worker_id("w1")
            .claim_ttl(Some(Duration::from_mins(2)))
            .batch_size(NonZeroUsize::new(8).unwrap())
            .build();

        assert_eq!(worker.worker_id, "w1");
        assert_eq!(worker.claim_ttl, Some(Duration::from_mins(2)));
        assert_eq!(worker.batch_size.get(), 8);
    }

    #[tokio::test]
    async fn test_dropped_handle_shuts_down_worker() {
        let worker = make_worker();
        let handle = worker.spawn(Duration::from_millis(50), EmptyWorkflows::new());

        // Extract the join handle before dropping so we can still await completion
        let join_handle = handle.inner.join_handle.lock().await.take().unwrap();
        drop(handle);

        let result = tokio::time::timeout(Duration::from_secs(5), join_handle)
            .await
            .ok()
            .and_then(Result::ok);
        assert!(
            result.is_some(),
            "Worker should exit when all handles are dropped"
        );
        assert!(result.is_some_and(|r| r.is_ok()));
    }
}
