//! `PostgresBackend` struct and constructors.

use std::sync::Arc;
use std::time::Duration;

use sayiir_core::codec::{self, CodecIdentity, Decoder, Encoder};
use sayiir_core::snapshot::WorkflowSnapshot;
use sayiir_core::snapshot_format;
use sayiir_persistence::BackendError;
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};

use crate::error::PgError;
use crate::wakeup::WakeupListener;

/// Minimum supported PostgreSQL major version.
const MIN_PG_MAJOR_VERSION: u32 = 13;

/// Connection-pool configuration for the Postgres backend.
///
/// All fields are optional; unset fields fall back to the defaults noted
/// per field. `max_connections` defaults to 20 and `min_connections` to 2
/// (warm connections avoid cold-start latency after idle periods); size
/// `max_connections` to at least `workers × concurrent tasks + a few` for
/// the signal/claim transactions.
///
/// `statement_timeout` and `idle_in_transaction_session_timeout` are applied
/// at the *session* level via `SET` on every newly-acquired connection, so
/// they affect every query the pool serves without requiring a server-side
/// configuration change.
///
/// Construct via `PoolOptions::default()` and field-assign, or pass directly
/// to [`PostgresBackend::connect_with_options`].
#[derive(Debug, Clone, Default)]
pub struct PoolOptions {
    /// Maximum number of connections held by the pool. Default: 20.
    pub max_connections: Option<u32>,
    /// Minimum number of connections kept warm. Default: 2.
    pub min_connections: Option<u32>,
    /// Time to wait for a connection from the pool before erroring out.
    pub acquire_timeout: Option<Duration>,
    /// Drop connections idle for longer than this.
    pub idle_timeout: Option<Duration>,
    /// Recycle connections older than this regardless of idle state.
    pub max_lifetime: Option<Duration>,
    /// `statement_timeout` value applied to every connection. Aborts queries
    /// that run longer than this duration.
    ///
    /// Note: sub-millisecond and zero durations are floored at 1 ms, because
    /// PG interprets a `0` value as "timeout disabled" — the opposite of
    /// what such a tight bound would suggest. Pass `None` to leave the
    /// server default in place (typically no timeout).
    pub statement_timeout: Option<Duration>,
    /// `idle_in_transaction_session_timeout` value applied to every
    /// connection. Aborts transactions that sit idle for longer than this
    /// duration, releasing the connection and unblocking VACUUM.
    ///
    /// Subject to the same sub-millisecond floor as [`statement_timeout`](
    /// Self::statement_timeout).
    pub idle_in_transaction_session_timeout: Option<Duration>,
}

/// PostgreSQL persistence backend for Sayiir workflows.
///
/// Generic over a [`Codec`](sayiir_core::codec::Codec) that determines how
/// snapshots are serialized into the `BYTEA` column. Use `JsonCodec` for
/// human-readable storage with Postgres-side queryability, or a binary codec
/// for faster (de)serialization.
///
/// # Example (with `sayiir-runtime` JSON codec)
///
/// ```rust,no_run
/// use sayiir_postgres::PostgresBackend;
/// use sayiir_runtime::serialization::JsonCodec;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let backend = PostgresBackend::<JsonCodec>::connect("postgresql://localhost/sayiir").await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct PostgresBackend<C> {
    pub(crate) pool: PgPool,
    pub(crate) codec: C,
    pub(crate) wakeup: Arc<WakeupListener>,
}

impl<C> PostgresBackend<C>
where
    C: Default,
{
    /// Connect to Postgres with sqlx pool defaults and run migrations.
    ///
    /// Equivalent to [`Self::connect_with_options`] called with
    /// [`PoolOptions::default()`]. Use that method instead when you need to
    /// tune pool size or session-level timeouts.
    ///
    /// # Errors
    ///
    /// Returns an error if the connection or migration fails.
    pub async fn connect(url: &str) -> Result<Self, BackendError> {
        Self::connect_with_options(url, PoolOptions::default()).await
    }

    /// Connect to Postgres with explicit pool options and run migrations.
    ///
    /// Field-level details on each option are documented on [`PoolOptions`].
    /// `statement_timeout` and `idle_in_transaction_session_timeout` are
    /// installed via an `after_connect` hook that runs `SET` on every
    /// freshly-acquired connection.
    ///
    /// # Errors
    ///
    /// Returns an error if the connection, the `SET` hooks, or the migration
    /// fail.
    pub async fn connect_with_options(
        url: &str,
        options: PoolOptions,
    ) -> Result<Self, BackendError> {
        let mut builder = PgPoolOptions::new()
            .max_connections(options.max_connections.unwrap_or(20))
            .min_connections(options.min_connections.unwrap_or(2));
        if let Some(d) = options.acquire_timeout {
            builder = builder.acquire_timeout(d);
        }
        if let Some(d) = options.idle_timeout {
            builder = builder.idle_timeout(d);
        }
        if let Some(d) = options.max_lifetime {
            builder = builder.max_lifetime(d);
        }

        // Session-level GUCs applied on every new connection so the limits
        // hold uniformly across the pool, including across recycles.
        //
        // PG's `SET` is a utility statement and does not accept bind
        // parameters, so we go through `set_config(name, value, is_local)`
        // — a regular function call that does — to keep the safe-by-default
        // pattern of never interpolating values into SQL text. `is_local =
        // false` makes the setting session-scoped (equivalent to `SET`).
        // The value is passed as text in milliseconds; both timeouts treat a
        // bare integer string as ms.
        let stmt_to = options.statement_timeout;
        let idle_tx_to = options.idle_in_transaction_session_timeout;
        if stmt_to.is_some() || idle_tx_to.is_some() {
            builder = builder.after_connect(move |conn, _meta| {
                Box::pin(async move {
                    if let Some(d) = stmt_to {
                        sqlx::query("SELECT set_config('statement_timeout', $1, false)")
                            .bind(duration_to_ms(d).to_string())
                            .execute(&mut *conn)
                            .await?;
                    }
                    if let Some(d) = idle_tx_to {
                        sqlx::query(
                            "SELECT set_config('idle_in_transaction_session_timeout', $1, false)",
                        )
                        .bind(duration_to_ms(d).to_string())
                        .execute(&mut *conn)
                        .await?;
                    }
                    Ok(())
                })
            });
        }

        let pool = builder.connect(url).await.map_err(PgError)?;
        // The listener gets its own connection (from the url) so it doesn't
        // permanently occupy a pool slot.
        Self::init(pool, Some(url.to_string())).await
    }

    /// Use an existing connection pool and run migrations.
    ///
    /// Prefer [`Self::connect_with_options`] when you only want to tune
    /// standard pool knobs — this method is meant for callers who need full
    /// control over the sqlx `PgPool` (custom TLS, listeners, etc.).
    ///
    /// Note: with no URL available, the LISTEN/NOTIFY listener holds one
    /// pooled connection for the backend's lifetime — size the pool
    /// accordingly.
    ///
    /// # Errors
    ///
    /// Returns an error if the migration fails.
    pub async fn connect_with(pool: PgPool) -> Result<Self, BackendError> {
        Self::init(pool, None).await
    }

    async fn init(pool: PgPool, url: Option<String>) -> Result<Self, BackendError> {
        check_pg_version(&pool).await?;

        tracing::info!("running postgres migrations");
        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .map_err(|e| BackendError::Backend(format!("migration failed: {e}")))?;

        let wakeup = WakeupListener::spawn(pool.clone(), url);

        tracing::info!("postgres backend ready");
        Ok(Self {
            pool,
            codec: C::default(),
            wakeup,
        })
    }
}

impl<C> PostgresBackend<C>
where
    C: Encoder + CodecIdentity + codec::sealed::EncodeValue<WorkflowSnapshot>,
{
    /// Encode a snapshot using the configured codec, wrapped in the durable
    /// [`snapshot_format`](sayiir_core::snapshot_format) envelope.
    pub(crate) fn encode(&self, snapshot: &WorkflowSnapshot) -> Result<Vec<u8>, BackendError> {
        snapshot_format::encode_framed(&self.codec, snapshot)
            .map_err(|e| BackendError::Serialization(e.to_string()))
    }

    /// Encode the outputs-stripped blob + its SHA-256, without cloning.
    ///
    /// Destructive: strips task outputs in place, no restore. For callers that
    /// own the snapshot, read only metadata after, and drop it on commit
    /// (`save_task_result`, the signal-store `check_and_*` paths). Callers that
    /// read outputs back after encoding use
    /// [`encode_blob_preserving`](Self::encode_blob_preserving).
    pub(crate) fn encode_blob(
        &self,
        snapshot: &mut WorkflowSnapshot,
    ) -> Result<(Vec<u8>, [u8; 32]), BackendError> {
        snapshot.strip_task_outputs();
        let data = self.encode(snapshot)?;
        let hash = crate::history::snapshot_hash(&data);
        Ok((data, hash))
    }

    /// Like [`encode_blob`] but restores the task outputs after encoding,
    /// so the snapshot is logically unchanged on return (including on the
    /// encode-error path). Costs an extra O(n) hydrate; only use it when
    /// the caller reads outputs back after encoding.
    pub(crate) fn encode_blob_preserving(
        &self,
        snapshot: &mut WorkflowSnapshot,
    ) -> Result<(Vec<u8>, [u8; 32]), BackendError> {
        let outputs = snapshot.take_task_outputs();
        let encoded = self.encode(snapshot);
        snapshot.hydrate_task_outputs(outputs);
        let data = encoded?;
        let hash = crate::history::snapshot_hash(&data);
        Ok((data, hash))
    }
}

impl<C> PostgresBackend<C>
where
    C: Decoder + CodecIdentity + codec::sealed::DecodeValue<WorkflowSnapshot>,
{
    /// Decode a snapshot from a durable blob: parse the
    /// [`snapshot_format`](sayiir_core::snapshot_format) envelope, validate the
    /// codec id matches the configured codec, then decode the payload.
    pub(crate) fn decode(&self, data: &[u8]) -> Result<WorkflowSnapshot, BackendError> {
        snapshot_format::decode_framed(&self.codec, data)
            .map_err(|e| BackendError::Serialization(e.to_string()))
    }
}

/// Query `SHOW server_version_num` and reject versions below [`MIN_PG_MAJOR_VERSION`].
///
/// PostgreSQL encodes its version as a single integer: major * 10000 + minor.
/// For example 130005 = 13.5, 170001 = 17.1.
async fn check_pg_version(pool: &PgPool) -> Result<(), BackendError> {
    let row = sqlx::query("SHOW server_version_num")
        .fetch_one(pool)
        .await
        .map_err(PgError)?;

    let version_str: &str = row.get("server_version_num");
    let version_num: u32 = version_str.parse().map_err(|e| {
        BackendError::Backend(format!(
            "failed to parse server_version_num '{version_str}': {e}"
        ))
    })?;

    let major = version_num / 10000;
    tracing::info!(pg_version = major, "connected to PostgreSQL {major}");

    if major < MIN_PG_MAJOR_VERSION {
        return Err(BackendError::Backend(format!(
            "PostgreSQL {major} is not supported (minimum: {MIN_PG_MAJOR_VERSION})"
        )));
    }

    Ok(())
}

/// Convert a [`Duration`] to a millisecond count usable as a PG GUC value.
///
/// PG accepts these timeouts as signed 4-byte integers (ms). Two edge cases:
///
/// * Any `Duration` smaller than 1 ms (including [`Duration::ZERO`]) would
///   quantize to `0`, which Postgres interprets as **disabling** the timeout
///   — the opposite of a caller asking for "as tight as possible." We floor
///   at 1 ms so sub-millisecond inputs still produce an active timeout.
/// * Durations longer than `i32::MAX` ms (~24.8 days) saturate at the
///   max representable value. No realistic configuration hits this in
///   practice, but it's an explicit choice over a panic or wraparound.
fn duration_to_ms(d: Duration) -> i32 {
    let ms = d.as_millis();
    if ms == 0 {
        1
    } else {
        i32::try_from(ms).unwrap_or(i32::MAX)
    }
}
