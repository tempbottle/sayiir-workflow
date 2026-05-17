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
    /// `Arc<str>` so the FFI layer can clone it cheaply into
    /// `TaskExecutionContext` without re-allocating per dispatch.
    pub(crate) name: Arc<str>,
    pub(crate) timeout: Option<std::time::Duration>,
    pub(crate) priority: Option<u8>,
    pub(crate) tags: Vec<String>,
    pub(crate) retry_policy: Option<RetryPolicy>,
    pub(crate) version: Option<String>,
}

impl TaskNodeMetadata {
    /// Human-readable node id (`Arc<str>` for cheap FFI cloning).
    #[must_use]
    pub fn name(&self) -> &Arc<str> {
        &self.name
    }
    /// Configured timeout, if any.
    #[must_use]
    pub fn timeout(&self) -> Option<std::time::Duration> {
        self.timeout
    }
    /// Configured priority (1–5; lower runs first).
    #[must_use]
    pub fn priority(&self) -> Option<u8> {
        self.priority
    }
    /// Affinity tags for worker-pool routing.
    #[must_use]
    pub fn tags(&self) -> &[String] {
        &self.tags
    }
    /// Configured retry policy.
    #[must_use]
    pub fn retry_policy(&self) -> Option<&RetryPolicy> {
        self.retry_policy.as_ref()
    }
    /// Optional version string.
    #[must_use]
    pub fn version(&self) -> Option<&str> {
        self.version.as_deref()
    }
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
        // Single traversal via `iter_nodes` — keeps tree-walking logic in one
        // place so a new continuation variant only needs to teach `NodeIter`
        // about itself, not a parallel walker. We index every id-carrying
        // node (Task / Delay / AwaitSignal) so worker-side `contains` lookups
        // succeed for delay and signal positions too.
        let map: HashMap<TaskId, TaskNodeMetadata> = continuation
            .iter_nodes()
            .filter(|n| {
                matches!(
                    n.kind,
                    crate::workflow::NodeKind::Task
                        | crate::workflow::NodeKind::Delay
                        | crate::workflow::NodeKind::AwaitSignal
                )
            })
            .map(|n| {
                let metadata = TaskNodeMetadata {
                    name: Arc::from(n.id),
                    timeout: n
                        .timeout
                        .filter(|_| n.kind == crate::workflow::NodeKind::Task),
                    priority: n.priority,
                    tags: n.tags.to_vec(),
                    retry_policy: n.retry_policy.cloned(),
                    version: n.version.map(str::to_owned),
                };
                (TaskId::from(n.id), metadata)
            })
            .collect();
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
    ///
    /// Returns a borrow into the index — callers that need to own the tags
    /// clone explicitly (e.g. when feeding into `TaskHint::new`).
    #[must_use]
    pub fn tags(&self, task_id: &TaskId) -> &[String] {
        self.0.get(task_id).map_or(&[], |m| m.tags.as_slice())
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
            Some(m) => crate::task::TaskMetadata::from_node_fields(
                m.timeout,
                m.retry_policy.clone(),
                m.version.clone(),
                m.priority,
                m.tags.clone(),
            ),
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
