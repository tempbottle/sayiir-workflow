//! Bounded-concurrency workflow submitter.
//!
//! Spawns `concurrency` long-lived workers that pull workflow indices from a
//! bounded mpsc channel and invoke a user-supplied submission future. The
//! caller's producer loop pushes indices (applying an optional token-bucket
//! `target_rate` drip), and the channel itself provides backpressure: once
//! all workers are busy, the next `send` awaits until a worker finishes.
//!
//! The submitter does not own the work — it just orchestrates fan-out so the
//! caller can construct the right submission future per index. Memory stays
//! bounded by `concurrency` (worker handles + in-flight channel slots)
//! regardless of `total`, which matters for the larger scenarios
//! (sleeping-giants defaults to 500k workflows).

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio::task::JoinSet;

/// Run `total` submissions with at most `concurrency` in flight at once.
///
/// `submit` is called once per index `i` in `0..total`. Errors from individual
/// submissions are logged and counted but do not abort the burst.
pub async fn submit_bounded<F, Fut>(
    total: usize,
    concurrency: usize,
    target_rate: Option<u64>,
    submit: F,
) -> Result<()>
where
    F: Fn(u64) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<()>> + Send + 'static,
{
    let submit = Arc::new(submit);

    // Bounded MPMC channel of capacity `concurrency`: the producer
    // blocks on send once every worker is busy, which is the
    // rate-limit-free backpressure path. Workers exit when `tx` is
    // dropped at the end of the producer loop, so no explicit shutdown
    // signal is needed.
    let (tx, rx) = flume::bounded::<u64>(concurrency.max(1));
    let mut workers: JoinSet<()> = JoinSet::new();

    for _ in 0..concurrency {
        let rx = rx.clone();
        let submit = Arc::clone(&submit);
        workers.spawn(async move {
            while let Ok(i) = rx.recv_async().await {
                if let Err(e) = submit(i).await {
                    tracing::warn!(index = i, error = %e, "workflow submission failed");
                }
            }
        });
    }
    // Drop the local consumer handle so the channel closes once the
    // producer's sender goes out of scope at the end of this function.
    drop(rx);

    let drip = target_rate.map(|r| Duration::from_secs_f64(1.0 / r as f64));
    let mut next_release = tokio::time::Instant::now();

    for i in 0..total {
        if let Some(d) = drip {
            next_release += d;
            tokio::time::sleep_until(next_release).await;
        }
        // `send_async` only errors when every receiver has dropped,
        // which can't happen here while `workers` is still owned by us.
        tx.send_async(i as u64)
            .await
            .expect("worker channel closed");
    }
    drop(tx);

    while workers.join_next().await.is_some() {}
    Ok(())
}
