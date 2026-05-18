//! Bounded-concurrency workflow submitter.
//!
//! Spawns `concurrency` workers that pull workflow indices from a channel and
//! invoke a user-supplied submission future. Optionally rate-limits to
//! `target_rate` submissions/second using a token-bucket-style drip.
//!
//! The submitter does not own the work — it just orchestrates fan-out so the
//! caller can construct the right submission future per index.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio::sync::Semaphore;

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
    let semaphore = Arc::new(Semaphore::new(concurrency));
    let submit = Arc::new(submit);

    let drip = target_rate.map(|r| Duration::from_secs_f64(1.0 / r as f64));
    let mut handles = Vec::with_capacity(total);
    let mut next_release = tokio::time::Instant::now();

    for i in 0..total {
        if let Some(d) = drip {
            next_release += d;
            tokio::time::sleep_until(next_release).await;
        }
        let permit = Arc::clone(&semaphore)
            .acquire_owned()
            .await
            .expect("semaphore closed");
        let submit = Arc::clone(&submit);
        handles.push(tokio::spawn(async move {
            let _permit = permit;
            if let Err(e) = submit(i as u64).await {
                tracing::warn!(index = i, error = %e, "workflow submission failed");
            }
        }));
    }

    for h in handles {
        let _ = h.await;
    }
    Ok(())
}
