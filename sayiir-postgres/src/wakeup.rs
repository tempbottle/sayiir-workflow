//! LISTEN/NOTIFY plumbing for the polling wakeup path.
//!
//! Producers pipeline `pg_notify` into the same statement that writes
//! the snapshot via [`build_task_ready_payload`] — PG defers delivery
//! to commit time, so readers never see a wakeup without an underlying
//! row change. The payload is a nanoserde-encoded [`TaskWakeupHint`]
//! wrapped in base64 (PG `NOTIFY` payloads must be valid text). The
//! hint lets the worker filter at receive time and target the named
//! task with a one-row eligibility check, skipping the full
//! `find_available_tasks` scan.
//!
//! The consumer is one long-lived tokio task spawned in `init`. It runs
//! `LISTEN sayiir_task_ready` over a `PgListener` (sqlx auto-reconnects
//! internally; the outer loop here handles the case where reconnect
//! eventually gives up) and `try_send`s each parsed hint into a `flume`
//! mpmc channel.

use std::ops::ControlFlow;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use backon::{BackoffBuilder, ExponentialBuilder};
use sayiir_persistence::TaskWakeupHint;
use sqlx::PgPool;
use sqlx::postgres::PgListener;
use tokio_util::sync::CancellationToken;

/// Process-global counter of wakeup hints dropped because the in-memory
/// mpmc channel was full. Surfaced to the runtime via
/// [`wakeup_drops_total`] so the benchmark harness can include "fell back
/// to poll" rate in its report without taking a metrics-crate dep.
static WAKEUP_DROPS: AtomicU64 = AtomicU64::new(0);

/// Snapshot of the wakeup-drop counter. Monotonic; a benchmark records
/// the value at scenario start and again at scenario end and reports
/// the delta.
#[must_use]
pub fn wakeup_drops_total() -> u64 {
    WAKEUP_DROPS.load(Ordering::Relaxed)
}

/// PG channel name for "a task is ready" wakeups. Listener and producers
/// share this single source of truth.
pub(crate) const TASK_READY_CHANNEL: &str = "sayiir_task_ready";

/// When full, the listener drops the incoming wake messages (fallback to poll).
/// Local-only tuning above main's 1024 to absorb submission bursts on this
/// hardware (PG NOTIFY payload cap is 8 KB → worst-case buffer ~128 MB).
const WAKEUP_CHANNEL_CAPACITY: usize = 16384;

/// First reconnect delay after a listener failure. The exponential policy
/// grows from here so a flapping PG doesn't get hammered, and the jitter
/// spreads reconnect attempts across a fleet to avoid a thundering herd
/// when (e.g.) PG restarts and every backend wakes up at the same instant.
const RECONNECT_MIN_DELAY: Duration = Duration::from_secs(5);

/// Ceiling on the reconnect delay — even after many failures the listener
/// retries at least this often, so a recovered PG is picked up promptly.
const RECONNECT_MAX_DELAY: Duration = Duration::from_mins(1);

/// Build the wakeup-hint payload for a snapshot, if it warrants a wake.
/// Only `InProgress AtTask` qualifies — `find_hinted_task` targets a
/// concrete task, and every other in-progress position relies on the
/// timer-tick fallback poll. Returns the base64-encoded hint bytes
/// ready for `pg_notify($channel, $payload)`.
///
/// Pipelined save paths bind the payload into the same statement that
/// writes the snapshot and gate `pg_notify` on `payload IS NOT NULL`,
/// so NOTIFY is deferred to the transaction's commit (a rollback
/// correctly drops the wake) without a separate round-trip.
pub(crate) fn build_task_ready_payload(
    snapshot: &sayiir_core::snapshot::WorkflowSnapshot,
) -> Option<String> {
    let task_id = snapshot.current_task_id()?;
    Some(
        TaskWakeupHint {
            instance_id: snapshot.instance_id.to_string(),
            task_id: *task_id.as_bytes(),
            definition_hash: *snapshot.definition_hash.as_bytes(),
            tags: snapshot.current_task_tags().to_vec(),
        }
        .encode(),
    )
}

pub(crate) struct WakeupListener {
    rx: flume::Receiver<TaskWakeupHint>,
    shutdown: CancellationToken,
}

impl WakeupListener {
    /// Spawn the listener task and return a shared handle. Must be called
    /// from a tokio runtime.
    pub(crate) fn spawn(pool: PgPool) -> std::sync::Arc<Self> {
        let (tx, rx) = flume::bounded(WAKEUP_CHANNEL_CAPACITY);
        let shutdown = CancellationToken::new();
        tokio::spawn(run_listener(pool, tx, shutdown.clone()));
        std::sync::Arc::new(Self { rx, shutdown })
    }

    /// Wait for the next wakeup or until `timeout` elapses.
    ///
    /// `Some(hint)` on delivery; `None` on timeout or after the listener
    /// task exited (e.g. process shutdown). `flume::Receiver::recv_async`
    /// takes `&self`, so multiple `wait` callers share this single
    /// receiver without any mutex — fan-in is mpmc.
    pub(crate) async fn wait(&self, timeout: std::time::Duration) -> Option<TaskWakeupHint> {
        match tokio::time::timeout(timeout, self.rx.recv_async()).await {
            Ok(Ok(hint)) => Some(hint),
            Ok(Err(flume::RecvError::Disconnected)) | Err(_) => None,
        }
    }
}

impl Drop for WakeupListener {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

async fn run_listener(
    pool: PgPool,
    tx: flume::Sender<TaskWakeupHint>,
    shutdown: CancellationToken,
) {
    loop {
        let listener = match connect_and_listen(&pool, &shutdown).await {
            ControlFlow::Break(()) => break,
            ControlFlow::Continue(l) => l,
        };
        match recv_until_error(listener, &tx, &shutdown).await {
            ControlFlow::Break(()) => break,
            ControlFlow::Continue(()) => {}
        }
    }
    tracing::debug!("wakeup listener: exiting");
}

/// Open a `PgListener` and subscribe to [`TASK_READY_CHANNEL`].
///
/// `Break(())` on shutdown / `PoolClosed`; `Continue(listener)` with a
/// live subscription. The `backon` iterator is local to each call so
/// exponential growth resets across reconnect cycles — a recovered PG
/// isn't penalized for an earlier outage.
async fn connect_and_listen(
    pool: &PgPool,
    shutdown: &CancellationToken,
) -> ControlFlow<(), PgListener> {
    let mut backoff = ExponentialBuilder::default()
        .with_min_delay(RECONNECT_MIN_DELAY)
        .with_max_delay(RECONNECT_MAX_DELAY)
        .with_factor(2.0)
        .with_jitter()
        .without_max_times()
        .build();

    loop {
        if shutdown.is_cancelled() || pool.is_closed() {
            return ControlFlow::Break(());
        }

        let mut listener = match shutdown
            .run_until_cancelled(PgListener::connect_with(pool))
            .await
        {
            None | Some(Err(sqlx::Error::PoolClosed)) => return ControlFlow::Break(()),
            Some(Ok(l)) => l,
            Some(Err(e)) => {
                tracing::warn!(error = %e, "wakeup listener: connect failed, retrying");
                sleep_or_break(shutdown, &mut backoff).await?;
                continue;
            }
        };

        match shutdown
            .run_until_cancelled(listener.listen(TASK_READY_CHANNEL))
            .await
        {
            None => return ControlFlow::Break(()),
            Some(Ok(())) => {
                tracing::debug!(channel = TASK_READY_CHANNEL, "wakeup listener subscribed");
                return ControlFlow::Continue(listener);
            }
            Some(Err(e)) => {
                tracing::warn!(error = %e, "wakeup listener: LISTEN failed, retrying");
                sleep_or_break(shutdown, &mut backoff).await?;
            }
        }
    }
}

/// Drain `recv()` into the mpmc wakeup queue until cancellation or
/// non-recoverable error. sqlx auto-reconnects internally on transient
/// socket failures and replays the LISTEN; `Err` here means recovery is
/// exhausted. `Break` = exit; `Continue` = caller should reconnect.
///
/// Malformed payloads (base64 decode failure, nanoserde decode failure)
/// are logged at WARN and skipped — a producer with a mismatched wire
/// format shouldn't crash the listener.
async fn recv_until_error(
    mut listener: PgListener,
    tx: &flume::Sender<TaskWakeupHint>,
    shutdown: &CancellationToken,
) -> ControlFlow<(), ()> {
    loop {
        match shutdown.run_until_cancelled(listener.recv()).await {
            None | Some(Err(sqlx::Error::PoolClosed)) => return ControlFlow::Break(()),
            Some(Ok(notification)) => match TaskWakeupHint::decode(notification.payload()) {
                Ok(hint) => match tx.try_send(hint) {
                    // `Disconnected` only happens if every Receiver was
                    // dropped — i.e. every PostgresBackend clone is
                    // gone. At that point the shutdown CancellationToken
                    // has also fired, so the outer loop will exit on
                    // the next `run_until_cancelled` poll. Drop the
                    // hint silently in either case.
                    Ok(()) | Err(flume::TrySendError::Disconnected(_)) => {}
                    Err(flume::TrySendError::Full(_)) => {
                        WAKEUP_DROPS.fetch_add(1, Ordering::Relaxed);
                        tracing::warn!(
                            "wakeup channel full, dropping hint; worker falls back to poll",
                        );
                    }
                },
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        payload = notification.payload(),
                        "wakeup listener: dropping unparseable hint",
                    );
                }
            },
            Some(Err(e)) => {
                tracing::warn!(error = %e, "wakeup listener: recv failed, reconnecting");
                return ControlFlow::Continue(());
            }
        }
    }
}

/// Sleep the next delay from `backoff`, or bail with `Break` on shutdown.
/// Returns `ControlFlow` so callers can `?`-propagate the break.
async fn sleep_or_break<B: Iterator<Item = Duration>>(
    shutdown: &CancellationToken,
    backoff: &mut B,
) -> ControlFlow<()> {
    let delay = backoff.next().unwrap_or(RECONNECT_MAX_DELAY);
    match shutdown
        .run_until_cancelled(tokio::time::sleep(delay))
        .await
    {
        None => ControlFlow::Break(()),
        Some(()) => ControlFlow::Continue(()),
    }
}
