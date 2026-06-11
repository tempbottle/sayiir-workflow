//! Task claiming mechanism for multi-node collaboration.
//!
//! This module provides structures for claiming and tracking task execution
//! across multiple nodes, preventing duplicate execution.

use chrono::{Duration, Utc};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// A claim on a task by a worker node.
///
/// When a worker claims a task, it has exclusive rights to execute it
/// until the claim expires or is released.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskClaim {
    /// The workflow instance ID (not to be confused with the workflow ID)
    pub instance_id: Arc<str>,
    /// The task ID being claimed.
    pub task_id: crate::TaskId,
    /// The worker node ID that claimed this task.
    pub worker_id: String,
    /// When the claim was created (Unix timestamp).
    pub claimed_at: u64,
    /// When the claim expires (Unix timestamp).
    /// If None, the claim never expires.
    pub expires_at: Option<u64>,
}

#[allow(clippy::cast_sign_loss)] // Timestamps are always positive
impl TaskClaim {
    /// Create a new task claim.
    #[must_use]
    pub fn new(
        instance_id: &str,
        task_id: crate::TaskId,
        worker_id: String,
        ttl: Option<Duration>,
    ) -> Self {
        let instance_id: Arc<str> = Arc::from(instance_id);
        let now = Utc::now();
        let claimed_at = now.timestamp() as u64;
        let expires_at = ttl.and_then(|duration| {
            now.checked_add_signed(duration)
                .map(|expiry| expiry.timestamp() as u64)
        });

        Self {
            instance_id,
            task_id,
            worker_id,
            claimed_at,
            expires_at,
        }
    }

    /// Check if this claim has expired.
    #[must_use]
    pub fn is_expired(&self) -> bool {
        if let Some(expires_at) = self.expires_at {
            let now = Utc::now().timestamp() as u64;
            now >= expires_at
        } else {
            false
        }
    }

    /// Check if this claim belongs to the given worker.
    #[must_use]
    pub fn is_owned_by(&self, worker_id: &str) -> bool {
        self.worker_id == worker_id
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::cast_sign_loss
)]
mod tests {
    use super::*;

    fn claim(worker: &str, expires_at: Option<u64>) -> TaskClaim {
        TaskClaim {
            instance_id: "inst-1".into(),
            task_id: crate::TaskId::from("task-1"),
            worker_id: worker.into(),
            claimed_at: 1_000_000,
            expires_at,
        }
    }

    #[test]
    fn no_ttl_never_expires() {
        assert!(!claim("w1", None).is_expired());
    }

    #[test]
    fn future_expiry_is_not_expired() {
        let far_future = Utc::now().timestamp() as u64 + 3600;
        assert!(!claim("w1", Some(far_future)).is_expired());
    }

    #[test]
    fn past_expiry_is_expired() {
        assert!(claim("w1", Some(0)).is_expired());
    }

    #[test]
    fn boundary_expiry_is_expired() {
        // expires_at == now should be expired (now >= expires_at)
        let now = Utc::now().timestamp() as u64;
        assert!(claim("w1", Some(now)).is_expired());
    }

    #[test]
    fn is_owned_by_matching_worker() {
        assert!(claim("worker-a", None).is_owned_by("worker-a"));
    }

    #[test]
    fn is_not_owned_by_different_worker() {
        assert!(!claim("worker-a", None).is_owned_by("worker-b"));
    }

    #[test]
    fn new_with_ttl_sets_expiry() {
        let c = TaskClaim::new(
            "i",
            crate::TaskId::from("t"),
            "w".into(),
            Some(Duration::seconds(60)),
        );
        assert!(c.expires_at.is_some());
        assert!(c.expires_at.unwrap() > c.claimed_at);
    }

    #[test]
    fn new_without_ttl_has_no_expiry() {
        let c = TaskClaim::new("i", crate::TaskId::from("t"), "w".into(), None);
        assert!(c.expires_at.is_none());
    }
}

/// Information about an available task ready for execution.
#[derive(Debug, Clone)]
pub struct AvailableTask {
    /// The workflow instance ID.
    pub instance_id: Arc<str>,
    /// The task ID.
    pub task_id: crate::TaskId,
    /// The input data for the task (serialized).
    pub input: bytes::Bytes,
    /// The workflow definition hash.
    pub workflow_definition_hash: crate::DefinitionHash,
    /// W3C `traceparent` header for distributed trace context propagation.
    pub trace_parent: Option<String>,
    /// Workflow state at dispatch time. Backends decode this from the
    /// history JOIN they already issue for the dispatch SELECT, so
    /// passing it through lets the worker skip a separate `load_snapshot`
    /// round-trip. Safe to use post-claim: nothing mutates the snapshot
    /// blob between dispatch and execution other than the worker
    /// itself, and signals (which don't touch the blob) are re-checked
    /// in the post-claim guard.
    ///
    /// Wrapped in `Arc` so the dispatch loop can move the owned
    /// snapshot in once, and so workers that lose the claim race drop
    /// their copy cheaply (refcount decrement). The winning worker
    /// extracts a mutable working copy via [`Self::take_snapshot`]
    /// without a deep clone.
    pub snapshot: Arc<crate::snapshot::WorkflowSnapshot>,
}

impl AvailableTask {
    /// Take ownership of the dispatch snapshot, leaving an empty
    /// placeholder behind. Backends build one `Arc` per dispatch, so the
    /// refcount is 1 and this is a move-out, not a deep clone (the clone
    /// fallback only fires if the `Arc` was shared).
    #[must_use]
    pub fn take_snapshot(&mut self) -> crate::snapshot::WorkflowSnapshot {
        let placeholder = Arc::new(crate::snapshot::WorkflowSnapshot::new(
            "",
            crate::DefinitionHash::default(),
        ));
        let arc = std::mem::replace(&mut self.snapshot, placeholder);
        Arc::try_unwrap(arc).unwrap_or_else(|shared| (*shared).clone())
    }
}
