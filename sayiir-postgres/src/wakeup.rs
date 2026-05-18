//! LISTEN/NOTIFY plumbing for the polling wakeup path.
//!
//! Producers ([`emit_task_ready`]) fire `pg_notify` inside whatever
//! transaction wrote the snapshot — PG defers delivery to commit time, so
//! readers never see a wakeup without an underlying row change. The
//! payload is a nanoserde-encoded [`TaskWakeupHint`] wrapped in base64
//! (PG `NOTIFY` payloads must be valid text). The hint lets the worker
//! filter at receive time and target the named task with a one-row
//! eligibility check, skipping the full `find_available_tasks` scan.
//!
//! The consumer is one long-lived tokio task spawned in `init`. It runs
//! `LISTEN sayiir_task_ready` over a `PgListener` (sqlx auto-reconnects
//! internally; the outer loop here handles the case where reconnect
//! eventually gives up) and `try_send`s each parsed hint into a `flume`
//! mpmc channel.

use std::ops::ControlFlow;
use std::time::Duration;

use backon::{BackoffBuilder, ExponentialBuilder};
use sayiir_persistence::TaskWakeupHint;
use sqlx::PgPool;
use sqlx::postgres::PgListener;
use tokio_util::sync::CancellationToken;

/// PG channel name for "a task is ready" wakeups. Listener and producers
/// share this single source of truth.
pub(crate) const TASK_READY_CHANNEL: &str = "sayiir_task_ready";

/// When full, the listener drops the incoming wake messages (fallback to poll)
const WAKEUP_CHANNEL_CAPACITY: usize = 1024;

/// First reconnect delay after a listener failure. The exponential policy
/// grows from here so a flapping PG doesn't get hammered, and the jitter
/// spreads reconnect attempts across a fleet to avoid a thundering herd
/// when (e.g.) PG restarts and every backend wakes up at the same instant.
const RECONNECT_MIN_DELAY: Duration = Duration::from_secs(5);

/// Ceiling on the reconnect delay — even after many failures the listener
/// retries at least this often, so a recovered PG is picked up promptly.
const RECONNECT_MAX_DELAY: Duration = Duration::from_mins(1);

/// Fire `pg_notify(sayiir_task_ready, <hint bytes>)` on the given connection.
///
/// Call unconditionally from any save site — the eligibility predicate is
/// evaluated here. Only `InProgress AtTask` produces a wake; everything
/// else (terminal, paused, AtDelay, AtJoin, …) returns without touching
/// the connection.
///
/// Call from inside the transaction that wrote the snapshot (pass
/// `&mut *tx`) — NOTIFY is deferred to commit, so a rollback correctly
/// drops the wake. Uses the function form of NOTIFY because it accepts
/// bind parameters, keeping the channel name and payload out of SQL
/// string interpolation.
pub(crate) async fn emit_task_ready(
    conn: &mut sqlx::PgConnection,
    snapshot: &sayiir_core::snapshot::WorkflowSnapshot,
) -> Result<(), sqlx::Error> {
    let Some(hint) = build_hint(snapshot) else {
        return Ok(());
    };
    sqlx::query("SELECT pg_notify($1, $2)")
        .bind(TASK_READY_CHANNEL)
        .bind(hint.encode())
        .execute(conn)
        .await?;
    Ok(())
}

/// Build a wakeup hint from a snapshot if a save should wake workers.
/// Only `InProgress AtTask` qualifies — `find_hinted_task` targets a
/// concrete task, and every other in-progress position relies on the
/// timer-tick fallback poll.
fn build_hint(snapshot: &sayiir_core::snapshot::WorkflowSnapshot) -> Option<TaskWakeupHint> {
    let task_id = snapshot.current_task_id()?;
    Some(TaskWakeupHint {
        instance_id: snapshot.instance_id.to_string(),
        task_id: *task_id.as_bytes(),
        definition_hash: *snapshot.definition_hash.as_bytes(),
        tags: snapshot.current_task_tags().to_vec(),
    })
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
