//! O(1) `TaskId → metadata` lookups built once per `Workflow`.
//!
//! `find_task` and the metadata getters on [`WorkflowContinuation`] walk the
//! continuation tree and recompute `TaskId::from(id.as_str())` (SHA-256) for
//! every visited node. That's fine for occasional introspection, but it adds
//! up on the dispatch hot path — every task claim does a `build_task_metadata`
//! plus optional `get_task_timeout` lookup.
//!
//! [`TaskIndex`] flattens the tree once at workflow build time into a
//! `HashMap<TaskId, TaskNodeMetadata>` so the per-dispatch lookups become a
//! single hash-map probe.
//!
//! The index lives behind an `Arc` on [`Workflow`](crate::workflow::Workflow)
//! and is shared cheaply with anything that needs the same view (the worker's
//! `ExternalWorkflow`, the distributed runner's hot loop, etc.).

use std::collections::HashMap;
use std::sync::Arc;

use crate::TaskId;
use crate::task::RetryPolicy;
use crate::workflow::WorkflowContinuation;

/// Metadata captured per `Task` node, keyed by [`TaskId`] in a [`TaskIndex`].
#[derive(Debug, Clone, Default)]
pub struct TaskNodeMetadata {
    /// Human-readable node id (the `id` argument given to the builder).
    ///
    /// Stored as `Arc<str>` so the FFI layer can clone it cheaply into
    /// `TaskExecutionContext` without re-allocating the string per dispatch.
    pub name: Arc<str>,
    /// Optional per-task timeout.
    pub timeout: Option<std::time::Duration>,
    /// Optional execution priority (1–5; lower runs first).
    pub priority: Option<u8>,
    /// Affinity tags for worker-pool routing.
    pub tags: Vec<String>,
    /// Optional retry policy.
    pub retry_policy: Option<RetryPolicy>,
    /// Optional version string.
    pub version: Option<String>,
}

/// O(1) lookup table from [`TaskId`] to per-node metadata.
///
/// Built once via [`TaskIndex::build`] at workflow construction. Stored behind
/// an `Arc` on [`Workflow`](crate::workflow::Workflow) and shared with
/// anything that needs to resolve `TaskId` → name / timeout / priority / tags
/// on the dispatch hot path.
#[derive(Debug, Clone, Default)]
pub struct TaskIndex(HashMap<TaskId, TaskNodeMetadata>);

impl TaskIndex {
    /// Build the index by walking `continuation` once and hashing each `Task`
    /// node's `id` to its [`TaskId`].
    #[must_use]
    pub fn build(continuation: &WorkflowContinuation) -> Self {
        let mut map = HashMap::new();
        collect(continuation, &mut map);
        Self(map)
    }

    /// `true` if the index knows about `task_id`.
    #[must_use]
    pub fn contains(&self, task_id: &TaskId) -> bool {
        self.0.contains_key(task_id)
    }

    /// Borrow the metadata for `task_id`.
    #[must_use]
    pub fn get(&self, task_id: &TaskId) -> Option<&TaskNodeMetadata> {
        self.0.get(task_id)
    }

    /// Human-readable node id for `task_id`, if any.
    #[must_use]
    pub fn name(&self, task_id: &TaskId) -> Option<&Arc<str>> {
        self.0.get(task_id).map(|m| &m.name)
    }

    /// Look up the priority configured on `task_id`.
    #[must_use]
    pub fn priority(&self, task_id: &TaskId) -> Option<u8> {
        self.0.get(task_id).and_then(|m| m.priority)
    }

    /// Look up the timeout configured on `task_id`.
    #[must_use]
    pub fn timeout(&self, task_id: &TaskId) -> Option<std::time::Duration> {
        self.0.get(task_id).and_then(|m| m.timeout)
    }

    /// Look up the affinity tags configured on `task_id`.
    #[must_use]
    pub fn tags(&self, task_id: &TaskId) -> Vec<String> {
        self.0
            .get(task_id)
            .map(|m| m.tags.clone())
            .unwrap_or_default()
    }

    /// Look up the retry policy configured on `task_id`.
    #[must_use]
    pub fn retry_policy(&self, task_id: &TaskId) -> Option<&RetryPolicy> {
        self.0.get(task_id).and_then(|m| m.retry_policy.as_ref())
    }

    /// Build a [`TaskMetadata`](crate::task::TaskMetadata) from the indexed
    /// fields. Equivalent to
    /// [`WorkflowContinuation::build_task_metadata`](crate::workflow::WorkflowContinuation::build_task_metadata)
    /// but O(1) instead of O(N) tree walk + per-node SHA-256.
    #[must_use]
    pub fn build_task_metadata(&self, task_id: &TaskId) -> crate::task::TaskMetadata {
        match self.0.get(task_id) {
            Some(m) => crate::task::TaskMetadata {
                timeout: m.timeout,
                retries: m.retry_policy.clone(),
                version: m.version.clone(),
                priority: m.priority.and_then(crate::priority::Priority::from_u8),
                tags: m.tags.clone(),
                ..Default::default()
            },
            None => crate::task::TaskMetadata::default(),
        }
    }

    /// Number of indexed task nodes.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// `true` when no task nodes are indexed.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

fn collect(cont: &WorkflowContinuation, out: &mut HashMap<TaskId, TaskNodeMetadata>) {
    match cont {
        WorkflowContinuation::Task {
            id,
            timeout,
            retry_policy,
            version,
            priority,
            tags,
            next,
            ..
        } => {
            out.insert(
                TaskId::from(id.as_str()),
                TaskNodeMetadata {
                    name: Arc::from(id.as_str()),
                    timeout: *timeout,
                    priority: *priority,
                    tags: tags.clone(),
                    retry_policy: retry_policy.clone(),
                    version: version.clone(),
                },
            );
            if let Some(n) = next.as_deref() {
                collect(n, out);
            }
        }
        WorkflowContinuation::Delay { id, next, .. }
        | WorkflowContinuation::AwaitSignal { id, next, .. } => {
            // Non-Task nodes still hash their id so worker-side
            // `contains(&task_id)` validation succeeds for delay/signal nodes.
            out.insert(
                TaskId::from(id.as_str()),
                TaskNodeMetadata {
                    name: Arc::from(id.as_str()),
                    ..TaskNodeMetadata::default()
                },
            );
            if let Some(n) = next.as_deref() {
                collect(n, out);
            }
        }
        WorkflowContinuation::Fork { branches, join, .. } => {
            for branch in branches {
                collect(branch, out);
            }
            if let Some(j) = join.as_deref() {
                collect(j, out);
            }
        }
        WorkflowContinuation::Branch {
            branches,
            default,
            next,
            ..
        } => {
            for branch in branches.values() {
                collect(branch, out);
            }
            if let Some(d) = default.as_deref() {
                collect(d, out);
            }
            if let Some(n) = next.as_deref() {
                collect(n, out);
            }
        }
        WorkflowContinuation::Loop { body, next, .. } => {
            collect(body, out);
            if let Some(n) = next.as_deref() {
                collect(n, out);
            }
        }
        WorkflowContinuation::ChildWorkflow { child, next, .. } => {
            collect(child, out);
            if let Some(n) = next.as_deref() {
                collect(n, out);
            }
        }
    }
}
