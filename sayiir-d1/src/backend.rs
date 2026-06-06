//! `SQLiteBackend` struct, inline JSON codec, and constructors.

use bytes::Bytes;
use sayiir_core::codec::{self, CodecIdentity, Decoder, Encoder};
use sayiir_core::snapshot::WorkflowSnapshot;
use sayiir_core::snapshot_format::{self, CodecId};
use sayiir_persistence::BackendError;
use sqlx::{Database, Executor, IntoArguments, Row};

use crate::schema::MIGRATIONS;

// ---------------------------------------------------------------------------
// Inline JsonCodec (avoids depending on sayiir-runtime which pulls in tokio)
// ---------------------------------------------------------------------------

/// Minimal JSON codec for snapshot serialization.
#[derive(Debug, Clone, Default)]
pub struct JsonCodec;

impl Encoder for JsonCodec {}
impl Decoder for JsonCodec {}

impl CodecIdentity for JsonCodec {
    fn codec_id(&self) -> CodecId {
        CodecId::Json
    }
}

impl codec::sealed::EncodeValue<WorkflowSnapshot> for JsonCodec {
    fn encode_value(
        &self,
        value: &WorkflowSnapshot,
    ) -> Result<Bytes, Box<dyn std::error::Error + Send + Sync>> {
        serde_json::to_vec(value)
            .map(Bytes::from)
            .map_err(Into::into)
    }
}

impl codec::sealed::DecodeValue<WorkflowSnapshot> for JsonCodec {
    fn decode_value(
        &self,
        bytes: Bytes,
    ) -> Result<WorkflowSnapshot, Box<dyn std::error::Error + Send + Sync>> {
        serde_json::from_slice(&bytes).map_err(Into::into)
    }
}

// ---------------------------------------------------------------------------
// SQLiteBackend
// ---------------------------------------------------------------------------

#[cfg(all(feature = "sqlite", not(feature = "d1")))]
pub type BackendDB = sqlx::Sqlite;
#[cfg(feature = "d1")]
pub type BackendDB = sqlx_d1::D1;

/// Persistence backend for Sayiir workflows using `sqlx-sqlite` or `sqlx-d1`.
///
/// Uses JSON serialization for snapshot data stored as `BLOB` in `SQLite`.
///
/// # Single-writer assumption
///
/// This backend assumes **at most one concurrent writer per workflow instance**.
/// Several operations (e.g. `save_task_result`, `store_signal`) use
/// read-modify-write sequences that are **not** protected by row-level locks
/// (`SQLite` / D1 does not support `SELECT … FOR UPDATE`). If multiple workers
/// or isolates write to the same database concurrently, these sequences can
/// lose updates.
///
/// The assumption holds when each workflow instance is owned by a single
/// worker or process. For use cases that require concurrent writers, this
/// backend is not suitable.
///
/// D1 is a persistent `SQLite` database hosted by Cloudflare. The data survives across Worker invocations. But a single
/// D1 binding is accessed by one Worker instance at a time per request, so concurrent writes from multiple in-flight
/// requests to the same Worker are not possible (Workers are single-threaded per request).
///
/// # Example
///
/// ```rust,ignore
/// use sayiir_d1::SQLiteBackend;
///
/// // d1 feature (default):
/// let backend = SQLiteBackend::connect(d1).await?;
///
/// // sqlite feature:
/// // let backend = SQLiteBackend::connect("sqlite://sayiir.db?mode=rwc").await?;
/// ```
#[derive(Clone)]
pub struct SQLiteBackend<T> {
    pub(crate) connection: T,
}

impl<T, DB: Database> SQLiteBackend<T>
where
    for<'c> &'c T: Executor<'c, Database = DB>,
    for<'a> DB::Arguments<'a>: IntoArguments<'a, DB>,
    for<'r> i64: sqlx::Decode<'r, DB> + sqlx::Encode<'r, DB> + sqlx::Type<DB>,
    usize: sqlx::ColumnIndex<DB::Row>,
    T: Clone,
{
    /// Create a new `SQLiteBackend` and run schema migrations.
    ///
    /// # Errors
    ///
    /// Returns a `BackendError` if the migration fails.
    pub async fn new(connection: T) -> Result<Self, BackendError> {
        let backend = Self { connection };
        backend.run_migrations().await?;
        Ok(backend)
    }

    /// Run the schema migrations on the database.
    ///
    /// # Errors
    ///
    /// Returns a `BackendError` if the migration fails.
    pub async fn run_migrations(&self) -> Result<(), BackendError> {
        let conn = self.exec();

        // Bootstrap the version table itself — idempotent. D1 forbids
        // `PRAGMA user_version`, so we track the applied schema version
        // in a regular row. The CHECK clause pins us to a single row.
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS sayiir_schema_version (
                 id      INTEGER PRIMARY KEY CHECK (id = 1),
                 version INTEGER NOT NULL
             )",
        )
        .execute(&conn)
        .await
        .map_err(|e| BackendError::Backend(format!("create sayiir_schema_version: {e}")))?;
        sqlx::query("INSERT OR IGNORE INTO sayiir_schema_version (id, version) VALUES (1, 0)")
            .execute(&conn)
            .await
            .map_err(|e| BackendError::Backend(format!("seed sayiir_schema_version: {e}")))?;

        let row = sqlx::query("SELECT version FROM sayiir_schema_version WHERE id = 1")
            .fetch_one(&conn)
            .await
            .map_err(|e| BackendError::Backend(format!("read schema version: {e}")))?;
        let current: i64 = row
            .try_get::<i64, _>(0_usize)
            .map_err(|e| BackendError::Backend(format!("decode schema version: {e}")))?;

        for migration in MIGRATIONS {
            if i64::from(migration.version) <= current {
                continue;
            }
            match sqlx::query(migration.sql).execute(&conn).await {
                Ok(_) => {}
                // Tolerate columns that were added manually before the
                // migrator existed — the schema is already where we want
                // it. Other errors propagate.
                Err(e) if e.to_string().contains("duplicate column name") => {
                    // Schema is already where we want it.
                }
                Err(e) => {
                    return Err(BackendError::Backend(format!(
                        "migration {} failed: {e}",
                        migration.version
                    )));
                }
            }
            sqlx::query("UPDATE sayiir_schema_version SET version = ?1 WHERE id = 1")
                .bind(i64::from(migration.version))
                .execute(&conn)
                .await
                .map_err(|e| {
                    BackendError::Backend(format!(
                        "bump schema version to {}: {e}",
                        migration.version
                    ))
                })?;
        }
        Ok(())
    }
}

impl<T> SQLiteBackend<T>
where
    T: Clone,
{
    pub(crate) fn exec(&self) -> T {
        self.connection.clone()
    }
}

impl<T> SQLiteBackend<T>
where
    for<'c> &'c T: Executor<'c, Database = crate::backend::BackendDB>,
    T: Clone + Send + Sync,
{
    /// Find workflow instance ids that should be re-driven by a cron sweep.
    ///
    /// Returns instances in three categories, ordered by `updated_at` ascending
    /// (oldest first) and capped at `limit`:
    ///
    /// 1. **Ready** — parked at `AtDelay`, `AtFork`, or timed `AtSignal`, with
    ///    `delay_wake_at <= now()`.
    /// 2. **Signalled** — parked at `AtSignal` (with or without timeout) and
    ///    has at least one buffered event row. Covers fire-and-forget
    ///    `send_signal` deliveries.
    /// 3. **Stale** — actively executing positions (`AtTask`, `AtJoin`,
    ///    `InLoop`, `NotStarted`, or NULL) not updated for at least
    ///    `stale_after_seconds`. Recovers from a worker that was evicted
    ///    mid-execution. Parked positions are excluded so workflows correctly
    ///    awaiting an external signal aren't periodically re-resumed.
    ///
    /// New `ExecutionPosition` variants default to *excluded* from category 3
    /// — extend the allow-list here when adding active-execution states.
    ///
    /// # Errors
    /// Returns [`BackendError::Backend`] if the query fails.
    pub async fn find_resumable_instances(
        &self,
        stale_after_seconds: u32,
        limit: u32,
    ) -> Result<Vec<String>, BackendError> {
        let exec = self.exec();
        let rows = sqlx::query(
            "SELECT s.instance_id FROM sayiir_workflow_snapshots s
             WHERE s.status = 'in_progress'
               AND (
                 (s.delay_wake_at IS NOT NULL AND s.delay_wake_at <= datetime('now'))
                 OR
                 (s.position_kind = 'AtSignal'
                  AND s.awaited_signal_name IS NOT NULL
                  AND EXISTS (SELECT 1 FROM sayiir_workflow_events e
                              WHERE e.instance_id = s.instance_id
                                AND e.signal_name = s.awaited_signal_name))
                 OR
                 (s.delay_wake_at IS NULL
                  AND (s.position_kind IS NULL
                       OR s.position_kind IN ('AtTask', 'AtJoin', 'InLoop', 'NotStarted'))
                  AND s.updated_at <= datetime('now', '-' || ?1 || ' seconds'))
               )
             ORDER BY s.updated_at ASC
             LIMIT ?2",
        )
        .bind(i64::from(stale_after_seconds))
        .bind(i64::from(limit))
        .fetch_all(&exec)
        .await
        .map_err(|e| BackendError::Backend(e.to_string()))?;

        Ok(rows.into_iter().map(|r| r.get("instance_id")).collect())
    }
}

impl<T> SQLiteBackend<T> {
    /// Encode a snapshot to JSON bytes wrapped in the durable
    /// [`snapshot_format`](sayiir_core::snapshot_format) envelope.
    #[allow(clippy::unused_self)]
    pub(crate) fn encode(&self, snapshot: &WorkflowSnapshot) -> Result<Vec<u8>, BackendError> {
        snapshot_format::encode_framed(&JsonCodec, snapshot)
            .map_err(|e| BackendError::Serialization(e.to_string()))
    }

    /// Decode a snapshot from a durable blob: parse the envelope, validate the
    /// codec id, then decode the JSON payload.
    #[allow(clippy::unused_self)]
    pub(crate) fn decode(&self, data: &[u8]) -> Result<WorkflowSnapshot, BackendError> {
        snapshot_format::decode_framed(&JsonCodec, data)
            .map_err(|e| BackendError::Serialization(e.to_string()))
    }
}

#[cfg(feature = "sqlite")]
impl SQLiteBackend<sqlx::SqlitePool> {
    /// Create a new `SQLiteBackend` from a database URL.
    ///
    /// # Errors
    ///
    /// Returns a `BackendError` if the connection or migration fails.
    pub async fn connect(url: &str) -> Result<Self, BackendError> {
        let pool = sqlx::SqlitePool::connect(url)
            .await
            .map_err(|e| BackendError::Backend(e.to_string()))?;
        Self::new(pool).await
    }
}

#[cfg(feature = "d1")]
impl SQLiteBackend<sqlx_d1::D1Connection> {
    /// Create a new `SQLiteBackend` from a `worker::D1Database`.
    ///
    /// # Errors
    ///
    /// Returns a `BackendError` if the migration fails.
    pub async fn connect(d1: worker::D1Database) -> Result<Self, BackendError> {
        let connection = sqlx_d1::D1Connection::new(d1);
        Self::new(connection).await
    }
}
