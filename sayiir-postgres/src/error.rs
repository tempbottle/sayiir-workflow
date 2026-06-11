//! Error mapping from sqlx to `BackendError`, plus transient-error retry.

use sayiir_persistence::BackendError;

/// Map a [`sqlx::Error`] into a [`BackendError`].
impl From<sqlx::Error> for PgError {
    fn from(e: sqlx::Error) -> Self {
        Self(e)
    }
}

/// Newtype wrapper so we can implement `Into<BackendError>` without orphan rules.
pub(crate) struct PgError(pub sqlx::Error);

impl From<PgError> for BackendError {
    fn from(e: PgError) -> Self {
        match e.0 {
            sqlx::Error::RowNotFound => BackendError::NotFound(e.0.to_string()),
            other => BackendError::Backend(other.to_string()),
        }
    }
}

impl PgError {
    /// True for errors that a fresh transaction is likely to clear:
    /// serialization failures (40001), deadlocks (40P01), connection-class
    /// SQLSTATEs (08xxx), I/O drops, and pool-acquire timeouts.
    pub(crate) fn is_transient(&self) -> bool {
        match &self.0 {
            sqlx::Error::Io(_) | sqlx::Error::PoolTimedOut => true,
            sqlx::Error::Database(db) => db
                .code()
                .is_some_and(|c| c == "40001" || c == "40P01" || c.starts_with("08")),
            _ => false,
        }
    }
}

/// Error type for retryable transaction bodies: keeps the sqlx error
/// classifiable until the retry decision is made, then collapses into
/// [`BackendError`].
pub(crate) enum TxError {
    Pg(PgError),
    Other(BackendError),
}

impl From<PgError> for TxError {
    fn from(e: PgError) -> Self {
        Self::Pg(e)
    }
}

impl From<BackendError> for TxError {
    fn from(e: BackendError) -> Self {
        Self::Other(e)
    }
}

impl From<TxError> for BackendError {
    fn from(e: TxError) -> Self {
        match e {
            TxError::Pg(p) => p.into(),
            TxError::Other(b) => b,
        }
    }
}

/// Run `op` (a whole transaction) with up to two exponential-backoff
/// retries on transient Postgres errors. Transactions roll back on
/// failure, so re-running is safe.
pub(crate) async fn with_transient_retry<T, F, Fut>(op_name: &str, op: F) -> Result<T, BackendError>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<T, TxError>>,
{
    use backon::{BackoffBuilder, Retryable};
    op.retry(
        backon::ExponentialBuilder::default()
            .with_min_delay(std::time::Duration::from_millis(10))
            .with_factor(4.0)
            .with_max_times(2)
            .build(),
    )
    .when(|e| matches!(e, TxError::Pg(p) if p.is_transient()))
    .notify(|e, delay| {
        if let TxError::Pg(p) = e {
            tracing::warn!(op = op_name, ?delay, error = %p.0, "transient pg error, retrying");
        }
    })
    .await
    .map_err(Into::into)
}
