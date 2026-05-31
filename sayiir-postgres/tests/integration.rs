#![allow(clippy::unwrap_used, clippy::expect_used)]

use bytes::Bytes;
use chrono::Duration;
use sayiir_core::snapshot::{ExecutionPosition, SignalKind, SignalRequest, WorkflowSnapshot};
use sayiir_persistence::{
    BackendError, SignalStore, SnapshotStore, TaskClaimStore, TaskResultStore,
};
use sayiir_postgres::PostgresBackend;
use sayiir_runtime::serialization::JsonCodec;
use sqlx::{PgPool, Row};
use testcontainers::ImageExt;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;

/// Minimum supported PostgreSQL version.
///
/// The schema requires `ALTER TABLE … ADD COLUMN IF NOT EXISTS` (9.6+) and
/// `INSERT … ON CONFLICT DO UPDATE` (9.5+). We set the floor at PostgreSQL 13,
/// which is the oldest major version still receiving security patches.
const MIN_PG_VERSION: &str = "13-alpine";

/// Default PostgreSQL version used by most tests.
const DEFAULT_PG_VERSION: &str = "17-alpine";

async fn setup_with(
    tag: &str,
) -> (
    testcontainers::ContainerAsync<Postgres>,
    PostgresBackend<JsonCodec>,
) {
    let (container, backend, _pool) = setup_with_pool(tag).await;
    (container, backend)
}

async fn setup_with_pool(
    tag: &str,
) -> (
    testcontainers::ContainerAsync<Postgres>,
    PostgresBackend<JsonCodec>,
    PgPool,
) {
    let container = Postgres::default().with_tag(tag).start().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgresql://postgres:postgres@127.0.0.1:{port}/postgres");
    let pool = PgPool::connect(&url).await.unwrap();
    let backend = PostgresBackend::<JsonCodec>::connect_with(pool.clone())
        .await
        .unwrap();
    (container, backend, pool)
}

async fn setup() -> (
    testcontainers::ContainerAsync<Postgres>,
    PostgresBackend<JsonCodec>,
) {
    setup_with(DEFAULT_PG_VERSION).await
}

/// Seed an `InProgress` snapshot for `instance_id` parked at `task_id`
/// so claim/extend/release tests have a row for the UPDATE to match.
async fn seed_snapshot_at_task(
    backend: &PostgresBackend<JsonCodec>,
    instance_id: &str,
    task_id: &sayiir_core::TaskId,
) {
    let mut snapshot = WorkflowSnapshot::new(instance_id, "test-hash".into());
    snapshot.update_position(ExecutionPosition::AtTask { task_id: *task_id });
    backend.save_snapshot(&mut snapshot).await.unwrap();
}

// ─── SnapshotStore ───────────────────────────────────────────────────────────

#[tokio::test]
async fn save_and_load_snapshot() {
    let (_c, backend) = setup().await;
    let mut snapshot = WorkflowSnapshot::new("test-1", "hash-1".into());

    backend.save_snapshot(&mut snapshot).await.unwrap();
    let loaded = backend.load_snapshot("test-1").await.unwrap();

    assert_eq!(&*loaded.instance_id, "test-1");
    assert_eq!(
        loaded.definition_hash,
        sayiir_core::DefinitionHash::from("hash-1")
    );
    assert!(loaded.state.is_in_progress());
}

#[tokio::test]
async fn load_not_found() {
    let (_c, backend) = setup().await;
    let result = backend.load_snapshot("nonexistent").await;
    assert!(matches!(result, Err(BackendError::NotFound(_))));
}

#[tokio::test]
async fn save_snapshot_overwrites() {
    let (_c, backend) = setup().await;
    let mut snapshot = WorkflowSnapshot::new("test-1", "hash-1".into());
    backend.save_snapshot(&mut snapshot).await.unwrap();

    snapshot.update_position(ExecutionPosition::AtTask {
        task_id: sayiir_core::TaskId::from("step-2"),
    });
    backend.save_snapshot(&mut snapshot).await.unwrap();

    let loaded = backend.load_snapshot("test-1").await.unwrap();
    match &loaded.state {
        sayiir_core::snapshot::WorkflowSnapshotState::InProgress { position, .. } => match position
        {
            ExecutionPosition::AtTask { task_id } => {
                assert_eq!(*task_id, sayiir_core::TaskId::from("step-2"))
            }
            other => panic!("expected AtTask, got {other:?}"),
        },
        other => panic!("expected InProgress, got {other:?}"),
    }
}

#[tokio::test]
async fn delete_snapshot() {
    let (_c, backend) = setup().await;
    let mut snapshot = WorkflowSnapshot::new("test-1", "hash-1".into());
    backend.save_snapshot(&mut snapshot).await.unwrap();

    backend.delete_snapshot("test-1").await.unwrap();
    let result = backend.load_snapshot("test-1").await;
    assert!(matches!(result, Err(BackendError::NotFound(_))));
}

#[tokio::test]
async fn delete_not_found() {
    let (_c, backend) = setup().await;
    let result = backend.delete_snapshot("nonexistent").await;
    assert!(matches!(result, Err(BackendError::NotFound(_))));
}

#[tokio::test]
async fn list_snapshots() {
    let (_c, backend) = setup().await;

    backend
        .save_snapshot(&mut WorkflowSnapshot::new("wf-1", "h".into()))
        .await
        .unwrap();
    backend
        .save_snapshot(&mut WorkflowSnapshot::new("wf-2", "h".into()))
        .await
        .unwrap();

    let mut list = backend.list_snapshots().await.unwrap();
    list.sort();
    assert_eq!(list, vec!["wf-1".to_string(), "wf-2".to_string()]);
}

#[tokio::test]
async fn save_task_result() {
    let (_c, backend) = setup().await;
    let mut snapshot = WorkflowSnapshot::new("wf-1", "hash-1".into());
    snapshot.update_position(ExecutionPosition::AtTask {
        task_id: sayiir_core::TaskId::from("task-1"),
    });
    backend.save_snapshot(&mut snapshot).await.unwrap();

    backend
        .save_task_result(
            "wf-1",
            &sayiir_core::TaskId::from("task-1"),
            Bytes::from(r#""done""#),
        )
        .await
        .unwrap();

    let loaded = backend.load_snapshot("wf-1").await.unwrap();
    let result = loaded.get_task_result(&sayiir_core::TaskId::from("task-1"));
    assert!(result.is_some());
    assert_eq!(result.unwrap().output, Bytes::from(r#""done""#));
}

/// Canonical-history contract: new saves must leave `snapshots.data` NULL
/// while history accumulates one row per version, each carrying a
/// `data_hash` equal to SHA-256 of the encoded blob. Loads must still
/// round-trip the snapshot through the history JOIN.
#[tokio::test]
async fn history_is_canonical_data_column_stays_null() {
    use sha2::{Digest, Sha256};

    let (_c, backend, pool) = setup_with_pool(DEFAULT_PG_VERSION).await;

    // Save once (creates version 1), then complete a task (creates
    // version 2 via save_task_result). Both writes must land in history
    // and leave snapshots.data NULL.
    let task_id = sayiir_core::TaskId::from("task-can");
    let mut snapshot = WorkflowSnapshot::new("wf-can", "hash-can".into());
    snapshot.update_position(ExecutionPosition::AtTask { task_id });
    backend.save_snapshot(&mut snapshot).await.unwrap();
    backend
        .save_task_result("wf-can", &task_id, Bytes::from_static(b"first-result"))
        .await
        .unwrap();

    let snap: (Option<Vec<u8>>, i32, Option<Vec<u8>>) = sqlx::query_as(
        "SELECT data, history_version, data_hash
         FROM sayiir_workflow_snapshots WHERE instance_id = $1",
    )
    .bind("wf-can")
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(
        snap.0.is_none(),
        "snapshots.data must stay NULL on new saves; got {:?}",
        snap.0,
    );
    assert_eq!(snap.1, 2, "history_version should advance per save");
    let snap_hash = snap.2.expect("snapshots.data_hash must be populated");

    // Both history rows carry the encoded blob and matching SHA-256.
    let history: Vec<(i32, Vec<u8>, Vec<u8>)> = sqlx::query_as(
        "SELECT version, data, data_hash
         FROM sayiir_workflow_snapshot_history
         WHERE instance_id = $1
         ORDER BY version",
    )
    .bind("wf-can")
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(history.len(), 2);
    for (version, data, hash) in &history {
        let expected: [u8; 32] = Sha256::digest(data).into();
        assert_eq!(hash.as_slice(), expected.as_slice(), "version {version}");
    }

    // snapshots.data_hash mirrors the latest history row's hash.
    let (_, _, latest_hash) = history.last().unwrap();
    assert_eq!(snap_hash.as_slice(), latest_hash.as_slice());

    // Round-trip through the JOIN'd load path.
    let loaded = backend.load_snapshot("wf-can").await.unwrap();
    assert_eq!(
        loaded.get_task_result(&task_id).unwrap().output,
        Bytes::from_static(b"first-result"),
    );
}

/// Dual-write: save_task_result must persist the output bytes into the
/// `sayiir_workflow_tasks.output` column in addition to the snapshot's
/// `completed_tasks` map. Once the cutover lands and the map is removed
/// from the blob, this column becomes the canonical source — this test
/// pins the contract that gets us there.
#[tokio::test]
async fn save_task_result_dual_writes_output_column() {
    let (_c, backend, pool) = setup_with_pool(DEFAULT_PG_VERSION).await;
    let task_id = sayiir_core::TaskId::from("task-dw");
    let mut snapshot = WorkflowSnapshot::new("wf-dw", "hash-dw".into());
    snapshot.update_position(ExecutionPosition::AtTask { task_id });
    backend.save_snapshot(&mut snapshot).await.unwrap();

    let payload = Bytes::from_static(b"\x00\x01\x02hello");
    backend
        .save_task_result("wf-dw", &task_id, payload.clone())
        .await
        .unwrap();

    let row: (Option<Vec<u8>>,) = sqlx::query_as(
        "SELECT output FROM sayiir_workflow_tasks WHERE instance_id = $1 AND task_id = $2",
    )
    .bind("wf-dw")
    .bind(task_id.as_bytes().as_slice())
    .fetch_one(&pool)
    .await
    .unwrap();
    let stored = row
        .0
        .expect("output column should be populated by dual-write");
    assert_eq!(stored.as_slice(), payload.as_ref());

    // The snapshot blob must still carry the result during the dual-write
    // phase — read paths haven't migrated yet.
    let loaded = backend.load_snapshot("wf-dw").await.unwrap();
    assert_eq!(loaded.get_task_result(&task_id).unwrap().output, payload,);
}

// ─── SignalStore ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn store_and_get_cancel_signal() {
    let (_c, backend) = setup().await;
    let mut snapshot = WorkflowSnapshot::new("wf-1", "hash-1".into());
    backend.save_snapshot(&mut snapshot).await.unwrap();

    backend
        .store_signal(
            "wf-1",
            SignalKind::Cancel,
            SignalRequest::new(Some("timeout".into()), Some("system".into())),
        )
        .await
        .unwrap();

    let signal = backend
        .get_signal("wf-1", SignalKind::Cancel)
        .await
        .unwrap();
    assert!(signal.is_some());
    let signal = signal.unwrap();
    assert_eq!(signal.reason, Some("timeout".into()));
    assert_eq!(signal.requested_by, Some("system".into()));
}

#[tokio::test]
async fn store_signal_on_nonexistent_workflow() {
    let (_c, backend) = setup().await;
    let result = backend
        .store_signal(
            "nonexistent",
            SignalKind::Cancel,
            SignalRequest::new(None, None),
        )
        .await;
    assert!(matches!(result, Err(BackendError::NotFound(_))));
}

#[tokio::test]
async fn store_cancel_on_completed_workflow() {
    let (_c, backend) = setup().await;
    let mut snapshot = WorkflowSnapshot::new("wf-1", "hash-1".into());
    snapshot.mark_completed(Bytes::from("result"));
    backend.save_snapshot(&mut snapshot).await.unwrap();

    let result = backend
        .store_signal("wf-1", SignalKind::Cancel, SignalRequest::new(None, None))
        .await;
    assert!(matches!(result, Err(BackendError::CannotCancel(_))));
}

#[tokio::test]
async fn store_pause_on_completed_workflow() {
    let (_c, backend) = setup().await;
    let mut snapshot = WorkflowSnapshot::new("wf-1", "hash-1".into());
    snapshot.mark_completed(Bytes::from("result"));
    backend.save_snapshot(&mut snapshot).await.unwrap();

    let result = backend
        .store_signal("wf-1", SignalKind::Pause, SignalRequest::new(None, None))
        .await;
    assert!(matches!(result, Err(BackendError::CannotPause(_))));
}

#[tokio::test]
async fn clear_signal() {
    let (_c, backend) = setup().await;
    let mut snapshot = WorkflowSnapshot::new("wf-1", "hash-1".into());
    backend.save_snapshot(&mut snapshot).await.unwrap();

    backend
        .store_signal(
            "wf-1",
            SignalKind::Cancel,
            SignalRequest::new(Some("test".into()), None),
        )
        .await
        .unwrap();

    backend
        .clear_signal("wf-1", SignalKind::Cancel)
        .await
        .unwrap();

    let signal = backend
        .get_signal("wf-1", SignalKind::Cancel)
        .await
        .unwrap();
    assert!(signal.is_none());
}

#[tokio::test]
async fn get_signal_returns_none_when_absent() {
    let (_c, backend) = setup().await;
    let signal = backend
        .get_signal("wf-1", SignalKind::Cancel)
        .await
        .unwrap();
    assert!(signal.is_none());
}

// ─── Composite signal operations ─────────────────────────────────────────────

#[tokio::test]
async fn check_and_cancel_success() {
    let (_c, backend) = setup().await;
    let mut snapshot = WorkflowSnapshot::new("wf-1", "hash-1".into());
    backend.save_snapshot(&mut snapshot).await.unwrap();

    backend
        .store_signal(
            "wf-1",
            SignalKind::Cancel,
            SignalRequest::new(Some("timeout".into()), Some("system".into())),
        )
        .await
        .unwrap();

    let cancelled = backend
        .check_and_cancel("wf-1", Some(sayiir_core::TaskId::from("task-1")))
        .await
        .unwrap();
    assert!(cancelled);

    let loaded = backend.load_snapshot("wf-1").await.unwrap();
    assert!(loaded.state.is_cancelled());

    let (reason, by) = loaded.state.cancellation_details().unwrap();
    assert_eq!(reason, Some("timeout".into()));
    assert_eq!(by, Some("system".into()));

    // Signal should be cleared
    let signal = backend
        .get_signal("wf-1", SignalKind::Cancel)
        .await
        .unwrap();
    assert!(signal.is_none());
}

#[tokio::test]
async fn check_and_cancel_no_signal() {
    let (_c, backend) = setup().await;
    let mut snapshot = WorkflowSnapshot::new("wf-1", "hash-1".into());
    backend.save_snapshot(&mut snapshot).await.unwrap();

    let cancelled = backend.check_and_cancel("wf-1", None).await.unwrap();
    assert!(!cancelled);

    let loaded = backend.load_snapshot("wf-1").await.unwrap();
    assert!(loaded.state.is_in_progress());
}

#[tokio::test]
async fn check_and_pause_success() {
    let (_c, backend) = setup().await;
    let mut snapshot = WorkflowSnapshot::new("wf-1", "hash-1".into());
    backend.save_snapshot(&mut snapshot).await.unwrap();

    backend
        .store_signal(
            "wf-1",
            SignalKind::Pause,
            SignalRequest::new(Some("maintenance".into()), Some("ops".into())),
        )
        .await
        .unwrap();

    let paused = backend.check_and_pause("wf-1").await.unwrap();
    assert!(paused);

    let loaded = backend.load_snapshot("wf-1").await.unwrap();
    assert!(loaded.state.is_paused());

    let (reason, by) = loaded.state.pause_details().unwrap();
    assert_eq!(reason, Some("maintenance".into()));
    assert_eq!(by, Some("ops".into()));
}

#[tokio::test]
async fn unpause_success() {
    let (_c, backend) = setup().await;
    let mut snapshot = WorkflowSnapshot::new("wf-1", "hash-1".into());
    backend.save_snapshot(&mut snapshot).await.unwrap();

    // Pause it first
    backend
        .store_signal(
            "wf-1",
            SignalKind::Pause,
            SignalRequest::new(Some("maintenance".into()), None),
        )
        .await
        .unwrap();
    backend.check_and_pause("wf-1").await.unwrap();

    // Unpause
    let unpaused = backend.unpause("wf-1").await.unwrap();
    assert!(unpaused.state.is_in_progress());

    let loaded = backend.load_snapshot("wf-1").await.unwrap();
    assert!(loaded.state.is_in_progress());
}

// ─── TaskClaimStore ──────────────────────────────────────────────────────────

#[tokio::test]
async fn claim_task_success() {
    let (_c, backend) = setup().await;
    seed_snapshot_at_task(&backend, "wf-1", &sayiir_core::TaskId::from("task-1")).await;

    let claim = backend
        .claim_task(
            "wf-1",
            &sayiir_core::TaskId::from("task-1"),
            "worker-1",
            Some(Duration::seconds(300)),
        )
        .await
        .unwrap();

    assert!(claim.is_some());
    let claim = claim.unwrap();
    assert_eq!(&*claim.instance_id, "wf-1");
    assert_eq!(claim.task_id, sayiir_core::TaskId::from("task-1"));
    assert_eq!(claim.worker_id, "worker-1");
    assert!(claim.expires_at.is_some());
}

#[tokio::test]
async fn claim_task_already_claimed() {
    let (_c, backend) = setup().await;
    seed_snapshot_at_task(&backend, "wf-1", &sayiir_core::TaskId::from("task-1")).await;

    let claim1 = backend
        .claim_task(
            "wf-1",
            &sayiir_core::TaskId::from("task-1"),
            "worker-1",
            Some(Duration::seconds(300)),
        )
        .await
        .unwrap();
    assert!(claim1.is_some());

    // Second claim by different worker should fail
    let claim2 = backend
        .claim_task(
            "wf-1",
            &sayiir_core::TaskId::from("task-1"),
            "worker-2",
            Some(Duration::seconds(300)),
        )
        .await
        .unwrap();
    assert!(claim2.is_none());
}

#[tokio::test]
async fn claim_task_expired_claim_replaced() {
    let (_c, backend) = setup().await;
    seed_snapshot_at_task(&backend, "wf-1", &sayiir_core::TaskId::from("task-1")).await;

    // Claim with 1-second TTL then wait for it to expire
    backend
        .claim_task(
            "wf-1",
            &sayiir_core::TaskId::from("task-1"),
            "worker-1",
            Some(Duration::seconds(1)),
        )
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // Second claim should succeed because first is expired
    let claim2 = backend
        .claim_task(
            "wf-1",
            &sayiir_core::TaskId::from("task-1"),
            "worker-2",
            Some(Duration::seconds(300)),
        )
        .await
        .unwrap();
    assert!(claim2.is_some());
    assert_eq!(claim2.unwrap().worker_id, "worker-2");
}

#[tokio::test]
async fn claim_task_no_ttl() {
    let (_c, backend) = setup().await;
    seed_snapshot_at_task(&backend, "wf-1", &sayiir_core::TaskId::from("task-1")).await;

    let claim = backend
        .claim_task(
            "wf-1",
            &sayiir_core::TaskId::from("task-1"),
            "worker-1",
            None,
        )
        .await
        .unwrap();

    assert!(claim.is_some());
    let claim = claim.unwrap();
    assert!(claim.expires_at.is_none());
    assert!(!claim.is_expired());
}

#[tokio::test]
async fn release_task_claim() {
    let (_c, backend) = setup().await;
    seed_snapshot_at_task(&backend, "wf-1", &sayiir_core::TaskId::from("task-1")).await;

    backend
        .claim_task(
            "wf-1",
            &sayiir_core::TaskId::from("task-1"),
            "worker-1",
            Some(Duration::seconds(300)),
        )
        .await
        .unwrap();

    backend
        .release_task_claim("wf-1", &sayiir_core::TaskId::from("task-1"), "worker-1")
        .await
        .unwrap();

    // Can claim again after release
    let claim = backend
        .claim_task(
            "wf-1",
            &sayiir_core::TaskId::from("task-1"),
            "worker-2",
            Some(Duration::seconds(300)),
        )
        .await
        .unwrap();
    assert!(claim.is_some());
}

#[tokio::test]
async fn release_task_claim_wrong_worker() {
    let (_c, backend) = setup().await;
    seed_snapshot_at_task(&backend, "wf-1", &sayiir_core::TaskId::from("task-1")).await;

    backend
        .claim_task(
            "wf-1",
            &sayiir_core::TaskId::from("task-1"),
            "worker-1",
            Some(Duration::seconds(300)),
        )
        .await
        .unwrap();

    let result = backend
        .release_task_claim("wf-1", &sayiir_core::TaskId::from("task-1"), "worker-2")
        .await;
    assert!(matches!(result, Err(BackendError::Backend(_))));
}

#[tokio::test]
async fn extend_task_claim() {
    let (_c, backend, pool) = setup_with_pool(DEFAULT_PG_VERSION).await;
    seed_snapshot_at_task(&backend, "wf-1", &sayiir_core::TaskId::from("task-1")).await;

    let claim = backend
        .claim_task(
            "wf-1",
            &sayiir_core::TaskId::from("task-1"),
            "worker-1",
            Some(Duration::seconds(10)),
        )
        .await
        .unwrap()
        .unwrap();
    let original_expiry_secs = claim.expires_at.unwrap();

    backend
        .extend_task_claim(
            "wf-1",
            &sayiir_core::TaskId::from("task-1"),
            "worker-1",
            Duration::seconds(300),
        )
        .await
        .unwrap();

    // Read the column directly: a "still held" reclaim probe would
    // pass even if extend was a silent no-op (the original 10s TTL
    // hasn't elapsed yet), so check the actual expiration moved by
    // ~the additional duration.
    let (claim_owner, claim_expires_at): (Option<String>, Option<chrono::DateTime<chrono::Utc>>) =
        sqlx::query_as(
            "SELECT worker_id, expires_at
         FROM sayiir_workflow_claims WHERE instance_id = $1",
        )
        .bind("wf-1")
        .fetch_one(&pool)
        .await
        .unwrap();

    assert_eq!(claim_owner.as_deref(), Some("worker-1"));
    let new_expiry_secs = claim_expires_at.unwrap().timestamp().cast_unsigned();
    assert_eq!(
        new_expiry_secs,
        original_expiry_secs + 300,
        "extend should move expires_at forward by exactly 300s"
    );
}

#[tokio::test]
async fn extend_task_claim_wrong_worker() {
    let (_c, backend) = setup().await;
    seed_snapshot_at_task(&backend, "wf-1", &sayiir_core::TaskId::from("task-1")).await;

    backend
        .claim_task(
            "wf-1",
            &sayiir_core::TaskId::from("task-1"),
            "worker-1",
            Some(Duration::seconds(300)),
        )
        .await
        .unwrap();

    let result = backend
        .extend_task_claim(
            "wf-1",
            &sayiir_core::TaskId::from("task-1"),
            "worker-2",
            Duration::seconds(300),
        )
        .await;
    assert!(matches!(result, Err(BackendError::Backend(_))));
}

#[tokio::test]
async fn find_available_tasks_basic() {
    let (_c, backend) = setup().await;

    let mut snapshot =
        WorkflowSnapshot::with_initial_input("wf-1", "hash-1".into(), Bytes::from(r#""input""#));
    snapshot.update_position(ExecutionPosition::AtTask {
        task_id: sayiir_core::TaskId::from("task-1"),
    });
    backend.save_snapshot(&mut snapshot).await.unwrap();

    let tasks = backend
        .find_available_tasks("worker-1", 10, chrono::Duration::seconds(300), &[])
        .await
        .unwrap();
    assert_eq!(tasks.len(), 1);
    assert_eq!(&*tasks[0].instance_id, "wf-1");
    assert_eq!(tasks[0].task_id, sayiir_core::TaskId::from("task-1"));
}

#[tokio::test]
async fn find_available_tasks_skips_cancelled() {
    let (_c, backend) = setup().await;

    // Workflow 1: in-progress with pending cancel signal
    let mut snapshot1 =
        WorkflowSnapshot::with_initial_input("wf-1", "hash-1".into(), Bytes::from(r#""input""#));
    snapshot1.update_position(ExecutionPosition::AtTask {
        task_id: sayiir_core::TaskId::from("task-1"),
    });
    backend.save_snapshot(&mut snapshot1).await.unwrap();
    backend
        .store_signal("wf-1", SignalKind::Cancel, SignalRequest::new(None, None))
        .await
        .unwrap();

    // Workflow 2: in-progress, no signals
    let mut snapshot2 =
        WorkflowSnapshot::with_initial_input("wf-2", "hash-1".into(), Bytes::from(r#""input""#));
    snapshot2.update_position(ExecutionPosition::AtTask {
        task_id: sayiir_core::TaskId::from("task-2"),
    });
    backend.save_snapshot(&mut snapshot2).await.unwrap();

    let tasks = backend
        .find_available_tasks("worker-1", 10, chrono::Duration::seconds(300), &[])
        .await
        .unwrap();
    assert!(!tasks.iter().any(|t| &*t.instance_id == "wf-1"));
    assert!(tasks.iter().any(|t| &*t.instance_id == "wf-2"));
}

#[tokio::test]
async fn find_available_tasks_skips_completed() {
    let (_c, backend) = setup().await;

    let mut snapshot = WorkflowSnapshot::new("wf-1", "hash-1".into());
    snapshot.mark_completed(Bytes::from("done"));
    backend.save_snapshot(&mut snapshot).await.unwrap();

    let tasks = backend
        .find_available_tasks("worker-1", 10, chrono::Duration::seconds(300), &[])
        .await
        .unwrap();
    assert!(tasks.is_empty());
}

// ─── Worker affinity (task tags) ─────────────────────────────────────────────

#[tokio::test]
async fn find_available_tasks_filters_by_worker_tags() {
    let (_c, backend) = setup().await;

    // Task tagged ["gpu"]
    let mut snap1 =
        WorkflowSnapshot::with_initial_input("wf-gpu", "h1".into(), Bytes::from(r#""1""#));
    snap1.update_position(ExecutionPosition::AtTask {
        task_id: sayiir_core::TaskId::from("t1"),
    });
    snap1.task_tags = vec!["gpu".into()];
    backend.save_snapshot(&mut snap1).await.unwrap();

    // Task tagged ["cpu"]
    let mut snap2 =
        WorkflowSnapshot::with_initial_input("wf-cpu", "h1".into(), Bytes::from(r#""2""#));
    snap2.update_position(ExecutionPosition::AtTask {
        task_id: sayiir_core::TaskId::from("t2"),
    });
    snap2.task_tags = vec!["cpu".into()];
    backend.save_snapshot(&mut snap2).await.unwrap();

    // Worker with ["gpu"] should only see the gpu task
    let tasks = backend
        .find_available_tasks("w1", 10, Duration::seconds(300), &["gpu".into()])
        .await
        .unwrap();
    assert_eq!(tasks.len(), 1);
    assert_eq!(&*tasks[0].instance_id, "wf-gpu");
}

#[tokio::test]
async fn find_available_tasks_untagged_worker_accepts_all() {
    let (_c, backend) = setup().await;

    let mut snap1 =
        WorkflowSnapshot::with_initial_input("wf-tagged", "h1".into(), Bytes::from(r#""1""#));
    snap1.update_position(ExecutionPosition::AtTask {
        task_id: sayiir_core::TaskId::from("t1"),
    });
    snap1.task_tags = vec!["gpu".into()];
    backend.save_snapshot(&mut snap1).await.unwrap();

    let mut snap2 =
        WorkflowSnapshot::with_initial_input("wf-plain", "h1".into(), Bytes::from(r#""2""#));
    snap2.update_position(ExecutionPosition::AtTask {
        task_id: sayiir_core::TaskId::from("t2"),
    });
    backend.save_snapshot(&mut snap2).await.unwrap();

    // Untagged worker should see both
    let tasks = backend
        .find_available_tasks("w1", 10, Duration::seconds(300), &[])
        .await
        .unwrap();
    assert_eq!(tasks.len(), 2);
}

#[tokio::test]
async fn find_available_tasks_untagged_tasks_accepted_by_tagged_worker() {
    let (_c, backend) = setup().await;

    // Untagged task
    let mut snap =
        WorkflowSnapshot::with_initial_input("wf-plain", "h1".into(), Bytes::from(r#""1""#));
    snap.update_position(ExecutionPosition::AtTask {
        task_id: sayiir_core::TaskId::from("t1"),
    });
    backend.save_snapshot(&mut snap).await.unwrap();

    // Tagged worker should still pick up untagged tasks
    let tasks = backend
        .find_available_tasks("w1", 10, Duration::seconds(300), &["gpu".into()])
        .await
        .unwrap();
    assert_eq!(tasks.len(), 1);
    assert_eq!(&*tasks[0].instance_id, "wf-plain");
}

#[tokio::test]
async fn find_available_tasks_multi_tag_subset() {
    let (_c, backend) = setup().await;

    // Task requiring ["gpu", "cuda"]
    let mut snap1 =
        WorkflowSnapshot::with_initial_input("wf-multi", "h1".into(), Bytes::from(r#""1""#));
    snap1.update_position(ExecutionPosition::AtTask {
        task_id: sayiir_core::TaskId::from("t1"),
    });
    snap1.task_tags = vec!["gpu".into(), "cuda".into()];
    backend.save_snapshot(&mut snap1).await.unwrap();

    // Task requiring only ["gpu"]
    let mut snap2 =
        WorkflowSnapshot::with_initial_input("wf-single", "h1".into(), Bytes::from(r#""2""#));
    snap2.update_position(ExecutionPosition::AtTask {
        task_id: sayiir_core::TaskId::from("t2"),
    });
    snap2.task_tags = vec!["gpu".into()];
    backend.save_snapshot(&mut snap2).await.unwrap();

    // Worker with ["gpu"] cannot run ["gpu","cuda"] (not a superset), but can run ["gpu"]
    let tasks = backend
        .find_available_tasks("w1", 10, Duration::seconds(300), &["gpu".into()])
        .await
        .unwrap();
    assert_eq!(tasks.len(), 1);
    assert_eq!(&*tasks[0].instance_id, "wf-single");

    // Worker with ["gpu","cuda","fast"] can run both (superset of both)
    let tasks = backend
        .find_available_tasks(
            "w2",
            10,
            Duration::seconds(300),
            &["gpu".into(), "cuda".into(), "fast".into()],
        )
        .await
        .unwrap();
    assert_eq!(tasks.len(), 2);
}

#[tokio::test]
async fn find_available_tasks_tags_persist_through_save_load() {
    let (_c, backend) = setup().await;

    let mut snap =
        WorkflowSnapshot::with_initial_input("wf-persist", "h1".into(), Bytes::from(r#""1""#));
    snap.update_position(ExecutionPosition::AtTask {
        task_id: sayiir_core::TaskId::from("t1"),
    });
    snap.task_tags = vec!["gpu".into(), "cuda".into()];
    backend.save_snapshot(&mut snap).await.unwrap();

    // Load and verify tags survived the roundtrip
    let loaded = backend.load_snapshot("wf-persist").await.unwrap();
    assert_eq!(
        loaded.task_tags,
        vec!["gpu".to_string(), "cuda".to_string()]
    );
}

#[tokio::test]
async fn find_available_tasks_disjoint_tags_no_match() {
    let (_c, backend) = setup().await;

    // Task tagged ["cpu"]
    let mut snap =
        WorkflowSnapshot::with_initial_input("wf-cpu", "h1".into(), Bytes::from(r#""1""#));
    snap.update_position(ExecutionPosition::AtTask {
        task_id: sayiir_core::TaskId::from("t1"),
    });
    snap.task_tags = vec!["cpu".into()];
    backend.save_snapshot(&mut snap).await.unwrap();

    // Worker with ["gpu"] should not see cpu tasks
    let tasks = backend
        .find_available_tasks("w1", 10, Duration::seconds(300), &["gpu".into()])
        .await
        .unwrap();
    assert!(tasks.is_empty());
}

// ─── TaskResultStore ─────────────────────────────────────────────────────────

#[tokio::test]
async fn load_task_result_in_progress() {
    let (_c, backend) = setup().await;
    let mut snapshot = WorkflowSnapshot::new("wf-1", "hash-1".into());
    snapshot.mark_task_completed(
        sayiir_core::TaskId::from("task-1"),
        Bytes::from(r#""out1""#),
    );
    backend.save_snapshot(&mut snapshot).await.unwrap();

    let result = backend
        .load_task_result("wf-1", &sayiir_core::TaskId::from("task-1"))
        .await
        .unwrap();
    assert_eq!(result, Some(Bytes::from(r#""out1""#)));
}

#[tokio::test]
async fn load_task_result_not_found() {
    let (_c, backend) = setup().await;
    let mut snapshot = WorkflowSnapshot::new("wf-1", "hash-1".into());
    backend.save_snapshot(&mut snapshot).await.unwrap();

    let result = backend
        .load_task_result("wf-1", &sayiir_core::TaskId::from("no-such-task"))
        .await
        .unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn load_task_result_nonexistent_instance() {
    let (_c, backend) = setup().await;
    let result = backend
        .load_task_result("no-such-wf", &sayiir_core::TaskId::from("task-1"))
        .await;
    assert!(matches!(result, Err(BackendError::NotFound(_))));
}

#[tokio::test]
async fn load_task_result_after_completion() {
    let (_c, backend) = setup().await;

    // Create workflow with a completed task
    let mut snapshot = WorkflowSnapshot::new("wf-1", "hash-1".into());
    snapshot.mark_task_completed(
        sayiir_core::TaskId::from("task-1"),
        Bytes::from(r#""out1""#),
    );
    backend.save_snapshot(&mut snapshot).await.unwrap();

    // Complete the workflow
    snapshot.mark_completed(Bytes::from(r#""final""#));
    backend.save_snapshot(&mut snapshot).await.unwrap();

    // Task result should still be accessible from history
    let result = backend
        .load_task_result("wf-1", &sayiir_core::TaskId::from("task-1"))
        .await
        .unwrap();
    assert_eq!(result, Some(Bytes::from(r#""out1""#)));
}

// ─── Minimum Postgres version ────────────────────────────────────────────────

/// Verify that migrations and core operations work on the minimum supported
/// PostgreSQL version (currently 13).
#[tokio::test]
async fn works_on_minimum_pg_version() {
    let (_c, backend) = setup_with(MIN_PG_VERSION).await;

    // Snapshot CRUD
    let mut snapshot = WorkflowSnapshot::new("min-pg-1", "hash".into());
    backend.save_snapshot(&mut snapshot).await.unwrap();
    let loaded = backend.load_snapshot("min-pg-1").await.unwrap();
    assert_eq!(&*loaded.instance_id, "min-pg-1");
    backend.delete_snapshot("min-pg-1").await.unwrap();

    // Signals
    let mut snapshot = WorkflowSnapshot::new("min-pg-2", "hash".into());
    backend.save_snapshot(&mut snapshot).await.unwrap();
    backend
        .store_signal(
            "min-pg-2",
            SignalKind::Cancel,
            SignalRequest::new(Some("test".into()), None),
        )
        .await
        .unwrap();
    let cancelled = backend
        .check_and_cancel("min-pg-2", Some(sayiir_core::TaskId::from("task-1")))
        .await
        .unwrap();
    assert!(cancelled);

    // Task claims (seed snapshot at the target task first).
    seed_snapshot_at_task(&backend, "min-pg-3", &sayiir_core::TaskId::from("task-1")).await;
    let claim = backend
        .claim_task(
            "min-pg-3",
            &sayiir_core::TaskId::from("task-1"),
            "worker-1",
            Some(Duration::seconds(60)),
        )
        .await
        .unwrap();
    assert!(claim.is_some());
}

// ─── LISTEN/NOTIFY ──────────────────────────────────────────────────────────

/// `save_snapshot` must wake listening workers iff the new state is
/// `InProgress AtTask` — the only position `find_hinted_task` can
/// target. Terminal, paused, not-yet-started, or any non-AtTask
/// in-progress position must not fire (those rely on the worker's
/// timer-tick fallback poll).
#[tokio::test]
async fn save_snapshot_emits_notify_only_when_poll_eligible() {
    use sqlx::postgres::PgListener;
    use std::time::Duration;

    let container = Postgres::default()
        .with_tag(DEFAULT_PG_VERSION)
        .start()
        .await
        .unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgresql://postgres:postgres@127.0.0.1:{port}/postgres");
    let pool = PgPool::connect(&url).await.unwrap();
    let backend = PostgresBackend::<JsonCodec>::connect_with(pool)
        .await
        .unwrap();

    let mut listener = PgListener::connect(&url).await.unwrap();
    listener.listen("sayiir_task_ready").await.unwrap();

    // 1) Fresh NotStarted snapshot — must not notify.
    let mut snapshot = WorkflowSnapshot::new("wf-notify", "h".into());
    backend.save_snapshot(&mut snapshot).await.unwrap();
    let no_notify = tokio::time::timeout(Duration::from_millis(150), listener.recv()).await;
    assert!(
        no_notify.is_err(),
        "NotStarted save should not emit a NOTIFY; got {:?}",
        no_notify.ok()
    );

    // 2) Transition to AtTask — must notify with a base64-wrapped
    //    nanoserde-encoded TaskWakeupHint carrying instance_id + task_id +
    //    definition_hash.
    let mut snapshot = backend.load_snapshot("wf-notify").await.unwrap();
    snapshot.update_position(ExecutionPosition::AtTask {
        task_id: sayiir_core::TaskId::from("step-1"),
    });
    backend.save_snapshot(&mut snapshot).await.unwrap();
    let notif = tokio::time::timeout(Duration::from_secs(1), listener.recv())
        .await
        .expect("AtTask save should emit a NOTIFY within 1s")
        .expect("listener recv failed");
    assert_eq!(notif.channel(), "sayiir_task_ready");
    let hint = sayiir_persistence::TaskWakeupHint::decode(notif.payload())
        .expect("payload must decode to a TaskWakeupHint");
    assert_eq!(&*hint.instance_id, "wf-notify");
    assert_eq!(
        hint.task_id,
        *sayiir_core::TaskId::from("step-1").as_bytes()
    );
    assert_eq!(
        hint.definition_hash,
        *sayiir_core::DefinitionHash::from("h").as_bytes()
    );

    // 3) Terminal completion — must not notify.
    let mut snapshot = backend.load_snapshot("wf-notify").await.unwrap();
    snapshot.mark_completed(Bytes::from("done"));
    backend.save_snapshot(&mut snapshot).await.unwrap();
    let no_notify = tokio::time::timeout(Duration::from_millis(150), listener.recv()).await;
    assert!(
        no_notify.is_err(),
        "Completed save should not emit a NOTIFY; got {:?}",
        no_notify.ok()
    );
}

/// `wait_for_wakeup` must return promptly once a `NOTIFY` is emitted by a
/// concurrent `save_snapshot`. The timeout is a ceiling; under normal
/// operation the listener delivers in <100 ms.
#[tokio::test]
async fn wait_for_wakeup_returns_on_notify() {
    use std::time::{Duration, Instant};
    use tokio::time::sleep;

    let (_c, backend) = setup().await;
    let backend2 = backend.clone();

    // Give the LISTEN task a brief moment to actually subscribe before we
    // race a NOTIFY against it. The listener spawns in `init` but the
    // socket-level LISTEN registration is racy without this nudge.
    sleep(Duration::from_millis(100)).await;

    let producer = tokio::spawn(async move {
        sleep(Duration::from_millis(150)).await;
        let mut snapshot = WorkflowSnapshot::new("wf-wake", "h".into());
        snapshot.update_position(ExecutionPosition::AtTask {
            task_id: sayiir_core::TaskId::from("step-1"),
        });
        backend2.save_snapshot(&mut snapshot).await.unwrap();
    });

    let started = Instant::now();
    backend
        .wait_for_wakeup(Duration::from_secs(5))
        .await
        .unwrap();
    let elapsed = started.elapsed();

    producer.await.unwrap();

    assert!(
        elapsed < Duration::from_secs(2),
        "expected NOTIFY-driven wakeup within 2s, took {elapsed:?}",
    );
}

/// With no NOTIFY traffic, `wait_for_wakeup` must respect its timeout and
/// return at the deadline — this is the fallback poll guarantee.
#[tokio::test]
async fn wait_for_wakeup_respects_timeout() {
    use std::time::{Duration, Instant};

    let (_c, backend) = setup().await;
    // Let the listener subscribe so a real Notify is in place — the
    // timeout path must hold even when the channel is actively listened.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let started = Instant::now();
    backend
        .wait_for_wakeup(Duration::from_millis(300))
        .await
        .unwrap();
    let elapsed = started.elapsed();

    assert!(
        elapsed >= Duration::from_millis(280),
        "expected ~300ms timeout, returned in {elapsed:?}",
    );
    assert!(
        elapsed < Duration::from_secs(1),
        "expected timeout near 300ms, got {elapsed:?}",
    );
}

/// `wait_for_wakeup` must return the parsed `TaskWakeupHint` when a
/// NOTIFY arrives, not just signal that something happened.
#[tokio::test]
async fn wait_for_wakeup_delivers_parsed_hint() {
    use std::time::Duration;
    use tokio::time::sleep;

    let (_c, backend) = setup().await;
    let backend2 = backend.clone();
    sleep(Duration::from_millis(100)).await;

    let producer = tokio::spawn(async move {
        sleep(Duration::from_millis(100)).await;
        let mut snapshot = WorkflowSnapshot::new("wf-hinted", "h-xyz".into());
        snapshot.update_position(ExecutionPosition::AtTask {
            task_id: sayiir_core::TaskId::from("task-a"),
        });
        backend2.save_snapshot(&mut snapshot).await.unwrap();
    });

    let hint = backend
        .wait_for_wakeup(Duration::from_secs(2))
        .await
        .unwrap()
        .expect("expected a hint from the NOTIFY");
    producer.await.unwrap();

    assert_eq!(&*hint.instance_id, "wf-hinted");
    assert_eq!(
        hint.task_id,
        *sayiir_core::TaskId::from("task-a").as_bytes()
    );
    assert_eq!(
        hint.definition_hash,
        *sayiir_core::DefinitionHash::from("h-xyz").as_bytes()
    );
}

/// `find_hinted_task` must return an `AvailableTask` when the snapshot
/// is still at the hinted task with no claim or signal block.
#[tokio::test]
async fn find_hinted_task_returns_available_task() {
    use sayiir_persistence::TaskWakeupHint;

    let (_c, backend) = setup().await;
    let mut snapshot = WorkflowSnapshot::with_initial_input(
        "wf-hint-found",
        "h".into(),
        Bytes::from_static(b"\"input\""),
    );
    snapshot.update_position(ExecutionPosition::AtTask {
        task_id: sayiir_core::TaskId::from("first"),
    });
    backend.save_snapshot(&mut snapshot).await.unwrap();

    let hint = TaskWakeupHint {
        instance_id: "wf-hint-found".into(),
        task_id: *sayiir_core::TaskId::from("first").as_bytes(),
        definition_hash: *sayiir_core::DefinitionHash::from("h").as_bytes(),
        tags: vec![],
    };
    let task = backend
        .find_hinted_task(&hint)
        .await
        .unwrap()
        .expect("hinted task should still be eligible");
    assert_eq!(&*task.instance_id, "wf-hint-found");
    assert_eq!(task.task_id, sayiir_core::TaskId::from("first"));
    assert_eq!(
        task.workflow_definition_hash,
        sayiir_core::DefinitionHash::from("h")
    );
}

/// `find_hinted_task` must return `None` when the snapshot already
/// moved past the hinted task (stale hint) — caller falls back to
/// the full poll, no claim happens.
#[tokio::test]
async fn find_hinted_task_returns_none_when_stale() {
    use sayiir_persistence::TaskWakeupHint;

    let (_c, backend) = setup().await;
    let mut snapshot = WorkflowSnapshot::new("wf-stale", "h".into());
    snapshot.update_position(ExecutionPosition::AtTask {
        task_id: sayiir_core::TaskId::from("second"),
    });
    backend.save_snapshot(&mut snapshot).await.unwrap();

    // Hint targets a task the workflow is no longer at — should be skipped.
    let hint = TaskWakeupHint {
        instance_id: "wf-stale".into(),
        task_id: *sayiir_core::TaskId::from("first").as_bytes(),
        definition_hash: *sayiir_core::DefinitionHash::from("h").as_bytes(),
        tags: vec![],
    };
    assert!(backend.find_hinted_task(&hint).await.unwrap().is_none());
}

/// `find_hinted_task` must return `None` when an active claim already
/// holds the task — preserving the find_available_tasks invariant that
/// claimed work is never re-handed-out.
#[tokio::test]
async fn find_hinted_task_returns_none_when_claimed() {
    use sayiir_persistence::TaskWakeupHint;

    let (_c, backend) = setup().await;
    let mut snapshot = WorkflowSnapshot::new("wf-claimed", "h".into());
    snapshot.update_position(ExecutionPosition::AtTask {
        task_id: sayiir_core::TaskId::from("first"),
    });
    backend.save_snapshot(&mut snapshot).await.unwrap();

    backend
        .claim_task(
            "wf-claimed",
            &sayiir_core::TaskId::from("first"),
            "other-worker",
            None,
        )
        .await
        .unwrap()
        .expect("first claim should succeed");

    let hint = TaskWakeupHint {
        instance_id: "wf-claimed".into(),
        task_id: *sayiir_core::TaskId::from("first").as_bytes(),
        definition_hash: *sayiir_core::DefinitionHash::from("h").as_bytes(),
        tags: vec![],
    };
    assert!(backend.find_hinted_task(&hint).await.unwrap().is_none());
}

// ─── send_event auto-resume ──────────────────────────────────────────────────

/// Build an `InProgress` snapshot parked at `AtSignal { signal_name }`
/// and save it.
async fn seed_snapshot_at_signal(
    backend: &PostgresBackend<JsonCodec>,
    instance_id: &str,
    signal_id: sayiir_core::TaskId,
    signal_name: &str,
    next_task_id: Option<sayiir_core::TaskId>,
) {
    let mut snapshot = WorkflowSnapshot::new(instance_id, "test-hash".into());
    snapshot.update_position(ExecutionPosition::AtSignal {
        signal_id,
        signal_name: signal_name.to_string(),
        wake_at: None,
        next_task_id,
    });
    backend.save_snapshot(&mut snapshot).await.unwrap();
}

/// Park at AtSignal, send the matching signal, assert the workflow
/// auto-resumes at the next task — the headline path PooledWorker
/// dispatch depends on (no AwaitSignal advance logic in PooledWorker).
#[tokio::test]
async fn send_event_auto_resumes_at_signal() {
    let (_c, backend) = setup().await;
    let signal_id = sayiir_core::TaskId::from("sig-1");
    let next_task = sayiir_core::TaskId::from("after-signal");

    seed_snapshot_at_signal(&backend, "wf-resume", signal_id, "go", Some(next_task)).await;

    backend
        .send_event("wf-resume", "go", Bytes::from("payload"))
        .await
        .unwrap();

    let snap = backend.load_snapshot("wf-resume").await.unwrap();
    match &snap.state {
        sayiir_core::snapshot::WorkflowSnapshotState::InProgress {
            position: ExecutionPosition::AtTask { task_id },
            completed_tasks,
            ..
        } => {
            assert_eq!(*task_id, next_task, "position must advance to next_task_id");
            assert!(
                completed_tasks.contains_key(&signal_id),
                "signal_id must appear in completed_tasks"
            );
        }
        other => panic!("expected InProgress AtTask after auto-resume, got {other:?}"),
    }

    // workflow_tasks must carry the signal payload so dispatch can hand
    // it to the next task as input (without this row a later
    // build_join_input / load_task_result returns TaskNotFound).
    let stored = backend
        .load_task_result("wf-resume", &signal_id)
        .await
        .unwrap();
    assert_eq!(stored.as_deref(), Some(b"payload".as_ref()));
}

/// Park at AtSignal whose `next_task_id = None` — the signal IS the
/// terminal node. send_event must flip the workflow to Completed AND
/// stamp `completed_at` on the snapshot row, otherwise retention sweeps
/// / dashboards filtering on completed_at silently miss this instance.
#[tokio::test]
async fn send_event_terminal_signal_sets_completed_at() {
    let (_c, backend, pool) = setup_with_pool(DEFAULT_PG_VERSION).await;
    let signal_id = sayiir_core::TaskId::from("sig-terminal");

    seed_snapshot_at_signal(&backend, "wf-term", signal_id, "done", None).await;

    backend
        .send_event("wf-term", "done", Bytes::from("final"))
        .await
        .unwrap();

    let snap = backend.load_snapshot("wf-term").await.unwrap();
    match &snap.state {
        sayiir_core::snapshot::WorkflowSnapshotState::Completed { final_output } => {
            // The signal payload must survive mark_completed — the
            // pre-fix code re-read it from completed_tasks AFTER
            // mark_completed (which drops the InProgress variant and
            // with it the completed_tasks map), persisting empty bytes.
            assert_eq!(
                final_output.as_ref(),
                b"final",
                "terminal-signal auto-resume must use the signal payload as the final output"
            );
        }
        other => panic!("expected Completed after terminal-signal auto-resume, got {other:?}"),
    }

    // workflow_tasks must also carry the signal payload — `mark_completed`
    // clears completed_tasks so a fresh `get_task_result_bytes` after the
    // transition would return None and the task_output CTE would persist
    // an empty payload (the bug fixed alongside this test). Read the row
    // directly because `load_task_result`'s terminal-state fallback
    // hydrates from the LAST `InProgress` history snapshot whose
    // `completed_tasks` here is empty (the seed was at AtSignal); the
    // sidecar row is the authoritative source for that case.
    let stored: Option<Vec<u8>> = sqlx::query_scalar(
        "SELECT output FROM sayiir_workflow_tasks
         WHERE instance_id = $1 AND task_id = $2 AND status = 'completed'",
    )
    .bind("wf-term")
    .bind(signal_id.as_bytes().as_slice())
    .fetch_optional(&pool)
    .await
    .unwrap()
    .flatten();
    assert_eq!(
        stored.as_deref(),
        Some(b"final".as_ref()),
        "signal payload must reach sayiir_workflow_tasks even on the terminal path"
    );

    // completed_at must be populated on the snapshot row.
    let row =
        sqlx::query("SELECT completed_at FROM sayiir_workflow_snapshots WHERE instance_id = $1")
            .bind("wf-term")
            .fetch_one(&pool)
            .await
            .unwrap();
    let completed_at: Option<chrono::DateTime<chrono::Utc>> = row.get("completed_at");
    assert!(
        completed_at.is_some(),
        "terminal-signal auto-resume must set completed_at; got NULL"
    );
}

/// Signal arrives BEFORE the workflow reaches AtSignal (or with a name
/// that doesn't match the current AtSignal). The probe-first path must
/// buffer the event and leave the snapshot untouched.
#[tokio::test]
async fn send_event_buffers_when_not_parked_at_signal() {
    let (_c, backend) = setup().await;

    // (a) Snapshot parked at AtTask — signal arrives early, must buffer.
    let task_id = sayiir_core::TaskId::from("t1");
    seed_snapshot_at_task(&backend, "wf-buffer", &task_id).await;
    let before = backend.load_snapshot("wf-buffer").await.unwrap();

    backend
        .send_event("wf-buffer", "go", Bytes::from("early"))
        .await
        .unwrap();

    let after = backend.load_snapshot("wf-buffer").await.unwrap();
    // Snapshot must be unchanged on the probe-first short-circuit path.
    match (&before.state, &after.state) {
        (
            sayiir_core::snapshot::WorkflowSnapshotState::InProgress {
                position: ExecutionPosition::AtTask { task_id: a },
                ..
            },
            sayiir_core::snapshot::WorkflowSnapshotState::InProgress {
                position: ExecutionPosition::AtTask { task_id: b },
                ..
            },
        ) => assert_eq!(a, b),
        _ => panic!("snapshot state must not change when event is buffered"),
    }

    // Event must be retrievable via consume_event (FIFO buffered store).
    let consumed = backend.consume_event("wf-buffer", "go").await.unwrap();
    assert_eq!(consumed.as_deref(), Some(b"early".as_ref()));

    // (b) Snapshot parked at AtSignal but for a DIFFERENT signal_name —
    // must also buffer (signal_resume_target returns None).
    let signal_id = sayiir_core::TaskId::from("sig-x");
    seed_snapshot_at_signal(&backend, "wf-mismatch", signal_id, "expected", None).await;
    backend
        .send_event("wf-mismatch", "other", Bytes::from("payload"))
        .await
        .unwrap();
    let snap = backend.load_snapshot("wf-mismatch").await.unwrap();
    match &snap.state {
        sayiir_core::snapshot::WorkflowSnapshotState::InProgress {
            position: ExecutionPosition::AtSignal { signal_name, .. },
            ..
        } => assert_eq!(signal_name, "expected"),
        _ => panic!("snapshot must remain parked at AtSignal on signal_name mismatch"),
    }
    let consumed = backend.consume_event("wf-mismatch", "other").await.unwrap();
    assert_eq!(consumed.as_deref(), Some(b"payload".as_ref()));
}

/// After auto-resume, the snapshot must be runnable by `find_hinted_task`
/// (the NOTIFY hot path) — the end-to-end guarantee the auto-resume
/// CTE exists to provide.
#[tokio::test]
async fn send_event_resumed_workflow_is_dispatchable() {
    use sayiir_persistence::TaskWakeupHint;

    let (_c, backend) = setup().await;
    let signal_id = sayiir_core::TaskId::from("sig-dispatch");
    let next_task = sayiir_core::TaskId::from("downstream");

    seed_snapshot_at_signal(&backend, "wf-dispatch", signal_id, "go", Some(next_task)).await;

    backend
        .send_event("wf-dispatch", "go", Bytes::from("payload"))
        .await
        .unwrap();

    let hint = TaskWakeupHint {
        instance_id: "wf-dispatch".into(),
        task_id: *next_task.as_bytes(),
        definition_hash: *sayiir_core::DefinitionHash::from("test-hash").as_bytes(),
        tags: vec![],
    };
    let available = backend
        .find_hinted_task(&hint)
        .await
        .unwrap()
        .expect("resumed workflow must be dispatchable via find_hinted_task");
    assert_eq!(available.task_id, next_task);
    // The signal payload must hydrate as the input to the downstream task.
    assert_eq!(available.input.as_ref(), b"payload");
}

/// `connect_with` must reject PostgreSQL versions below the minimum (13).
#[tokio::test]
async fn rejects_unsupported_pg_version() {
    let container = Postgres::default()
        .with_tag("12-alpine")
        .start()
        .await
        .unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgresql://postgres:postgres@127.0.0.1:{port}/postgres");
    let pool = PgPool::connect(&url).await.unwrap();

    let result = PostgresBackend::<JsonCodec>::connect_with(pool).await;
    match result {
        Ok(_) => panic!("expected version rejection, but connect succeeded"),
        Err(err) => {
            let msg = err.to_string();
            assert!(
                msg.contains("not supported"),
                "expected version rejection, got: {msg}"
            );
        }
    }
}
