//! Python-exposed persistence backends.

use pyo3::prelude::*;
use sayiir_persistence::InMemoryBackend;
use sayiir_postgres::{PoolOptions, PostgresBackend};
use sayiir_runtime::serialization::JsonCodec;
use std::sync::Arc;
use std::time::Duration;

/// Python-exposed in-memory persistence backend.
///
/// Stores workflow snapshots in memory. Suitable for testing and development.
#[pyclass(from_py_object)]
#[derive(Clone)]
pub struct PyInMemoryBackend {
    pub(crate) inner: Arc<InMemoryBackend>,
}

#[pymethods]
impl PyInMemoryBackend {
    #[new]
    fn new() -> Self {
        Self {
            inner: Arc::new(InMemoryBackend::new()),
        }
    }

    fn __repr__(&self) -> String {
        "InMemoryBackend()".to_string()
    }
}

/// Python-exposed `PostgreSQL` persistence backend.
///
/// Connects to a `PostgreSQL` database and stores workflow snapshots durably.
/// Runs migrations automatically on first connect.
#[pyclass(from_py_object)]
#[derive(Clone)]
pub struct PyPostgresBackend {
    pub(crate) inner: Arc<PostgresBackend<JsonCodec>>,
    pub(crate) url: String,
    /// Pool options stashed so engine / worker / client can rebuild the pool
    /// on their own tokio runtimes without losing the operator's tuning.
    pub(crate) options: PoolOptions,
    /// Kept alive for connection pool background tasks.
    #[allow(dead_code)]
    pub(crate) runtime: Arc<tokio::runtime::Runtime>,
}

#[pymethods]
impl PyPostgresBackend {
    /// Create a new `PostgreSQL` backend.
    ///
    /// Args:
    ///     url: Connection URL (e.g. `postgresql://localhost/sayiir`).
    ///     `max_connections`: Maximum pool size (sqlx default: 10).
    ///     `min_connections`: Minimum warm connections (sqlx default: 0).
    ///     `acquire_timeout_secs`: Seconds to wait for a free connection
    ///         before erroring out.
    ///     `idle_timeout_secs`: Drop connections idle longer than this.
    ///     `max_lifetime_secs`: Recycle connections older than this.
    ///     `statement_timeout_secs`: PG `statement_timeout` applied to every
    ///         connection. Aborts queries exceeding this duration.
    ///     `idle_in_transaction_session_timeout_secs`: PG
    ///         `idle_in_transaction_session_timeout` applied to every
    ///         connection. Aborts transactions sitting idle. Named to match
    ///         the underlying Postgres GUC for discoverability.
    #[new]
    #[pyo3(signature = (
        url, *,
        max_connections=None,
        min_connections=None,
        acquire_timeout_secs=None,
        idle_timeout_secs=None,
        max_lifetime_secs=None,
        statement_timeout_secs=None,
        idle_in_transaction_session_timeout_secs=None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        url: &str,
        max_connections: Option<u32>,
        min_connections: Option<u32>,
        acquire_timeout_secs: Option<f64>,
        idle_timeout_secs: Option<f64>,
        max_lifetime_secs: Option<f64>,
        statement_timeout_secs: Option<f64>,
        idle_in_transaction_session_timeout_secs: Option<f64>,
    ) -> PyResult<Self> {
        tracing::info!("connecting to PostgreSQL backend");
        tracing::debug!(url, "PostgreSQL connection URL");

        let options = PoolOptions {
            max_connections,
            min_connections,
            acquire_timeout: acquire_timeout_secs.map(Duration::from_secs_f64),
            idle_timeout: idle_timeout_secs.map(Duration::from_secs_f64),
            max_lifetime: max_lifetime_secs.map(Duration::from_secs_f64),
            statement_timeout: statement_timeout_secs.map(Duration::from_secs_f64),
            idle_in_transaction_session_timeout: idle_in_transaction_session_timeout_secs
                .map(Duration::from_secs_f64),
        };

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

        let backend = runtime
            .block_on(PostgresBackend::<JsonCodec>::connect_with_options(
                url,
                options.clone(),
            ))
            .map_err(|e: sayiir_persistence::BackendError| {
                tracing::error!(error = %e, "failed to connect to PostgreSQL");
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())
            })?;

        tracing::info!("PostgreSQL backend connected");

        Ok(Self {
            inner: Arc::new(backend),
            url: url.to_owned(),
            options,
            runtime: Arc::new(runtime),
        })
    }

    fn __repr__(&self) -> String {
        "PostgresBackend(...)".to_string()
    }
}

/// Internal enum dispatching to either backend kind.
///
/// Since `PersistentBackend` uses RPITIT (not object-safe), we use enum
/// dispatch instead of `dyn`.
pub(crate) enum BackendKind {
    InMemory(Arc<InMemoryBackend>),
    Postgres(Arc<PostgresBackend<JsonCodec>>),
}

/// Dispatch a call to the backend regardless of variant.
///
/// The `$body` expression uses `$backend` which is monomorphised for each
/// concrete type — required because the runtime functions take
/// `&impl PersistentBackend`.
macro_rules! with_backend {
    ($self:expr, |$backend:ident| $body:expr) => {
        match &$self.backend {
            BackendKind::InMemory($backend) => $body,
            BackendKind::Postgres($backend) => $body,
        }
    };
}
pub(crate) use with_backend;

// ---------------------------------------------------------------------------
// Trait implementations for BackendKind — needed for PooledWorker
// ---------------------------------------------------------------------------

use sayiir_core::snapshot::{SignalKind, SignalRequest, WorkflowSnapshot};
use sayiir_core::task_claim::{AvailableTask, TaskClaim};
use sayiir_persistence::{
    BackendError, SignalStore, SnapshotStore, TaskClaimStore, TaskResultStore,
};

/// Dispatch macro for trait methods on `BackendKind`.
macro_rules! dispatch {
    ($self:expr, |$inner:ident| $body:expr) => {
        match $self {
            BackendKind::InMemory($inner) => $body,
            BackendKind::Postgres($inner) => $body,
        }
    };
}

impl SnapshotStore for BackendKind {
    async fn save_snapshot(&self, snapshot: &mut WorkflowSnapshot) -> Result<(), BackendError> {
        dispatch!(self, |b| b.save_snapshot(snapshot).await)
    }

    async fn save_task_result(
        &self,
        instance_id: &str,
        task_id: &sayiir_core::TaskId,
        output: bytes::Bytes,
    ) -> Result<(), BackendError> {
        dispatch!(self, |b| b
            .save_task_result(instance_id, task_id, output)
            .await)
    }

    async fn load_snapshot(&self, instance_id: &str) -> Result<WorkflowSnapshot, BackendError> {
        dispatch!(self, |b| b.load_snapshot(instance_id).await)
    }

    async fn delete_snapshot(&self, instance_id: &str) -> Result<(), BackendError> {
        dispatch!(self, |b| b.delete_snapshot(instance_id).await)
    }

    async fn list_snapshots(&self) -> Result<Vec<String>, BackendError> {
        dispatch!(self, |b| b.list_snapshots().await)
    }
}

impl SignalStore for BackendKind {
    async fn store_signal(
        &self,
        instance_id: &str,
        kind: SignalKind,
        request: SignalRequest,
    ) -> Result<(), BackendError> {
        dispatch!(self, |b| b.store_signal(instance_id, kind, request).await)
    }

    async fn get_signal(
        &self,
        instance_id: &str,
        kind: SignalKind,
    ) -> Result<Option<SignalRequest>, BackendError> {
        dispatch!(self, |b| b.get_signal(instance_id, kind).await)
    }

    async fn clear_signal(&self, instance_id: &str, kind: SignalKind) -> Result<(), BackendError> {
        dispatch!(self, |b| b.clear_signal(instance_id, kind).await)
    }

    async fn send_event(
        &self,
        instance_id: &str,
        signal_name: &str,
        payload: bytes::Bytes,
    ) -> Result<(), BackendError> {
        dispatch!(self, |b| b
            .send_event(instance_id, signal_name, payload)
            .await)
    }

    async fn consume_event(
        &self,
        instance_id: &str,
        signal_name: &str,
    ) -> Result<Option<bytes::Bytes>, BackendError> {
        dispatch!(self, |b| b.consume_event(instance_id, signal_name).await)
    }
}

impl TaskClaimStore for BackendKind {
    async fn claim_task(
        &self,
        instance_id: &str,
        task_id: &sayiir_core::TaskId,
        worker_id: &str,
        ttl: Option<chrono::Duration>,
    ) -> Result<Option<TaskClaim>, BackendError> {
        dispatch!(self, |b| b
            .claim_task(instance_id, task_id, worker_id, ttl)
            .await)
    }

    async fn release_task_claim(
        &self,
        instance_id: &str,
        task_id: &sayiir_core::TaskId,
        worker_id: &str,
    ) -> Result<(), BackendError> {
        dispatch!(self, |b| b
            .release_task_claim(instance_id, task_id, worker_id)
            .await)
    }

    async fn extend_task_claim(
        &self,
        instance_id: &str,
        task_id: &sayiir_core::TaskId,
        worker_id: &str,
        additional_duration: chrono::Duration,
    ) -> Result<(), BackendError> {
        dispatch!(self, |b| b
            .extend_task_claim(instance_id, task_id, worker_id, additional_duration)
            .await)
    }

    async fn find_available_tasks(
        &self,
        worker_id: &str,
        limit: usize,
        aging_interval: chrono::Duration,
        worker_tags: &[String],
    ) -> Result<Vec<AvailableTask>, BackendError> {
        dispatch!(self, |b| b
            .find_available_tasks(worker_id, limit, aging_interval, worker_tags)
            .await)
    }
}

impl TaskResultStore for BackendKind {
    async fn load_task_result(
        &self,
        instance_id: &str,
        task_id: &sayiir_core::TaskId,
    ) -> Result<Option<bytes::Bytes>, BackendError> {
        dispatch!(self, |b| b.load_task_result(instance_id, task_id).await)
    }
}
