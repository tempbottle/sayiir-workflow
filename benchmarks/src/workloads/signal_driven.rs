//! Signal-driven workflow.
//!
//! Workflow shape:
//! ```text
//!  pickup → wait_for_signal("kick") → final_emit
//! ```
//!
//! The driver submits N workflows, waits for each to reach the pickup
//! point (= the workflow has parked at the signal), then sends a `kick`
//! signal carrying the workflow's `State` payload. The final task fires
//! after the signal is consumed and records completion.
//!
//! Latency blocks:
//! * `e2e`: submit → completion (the headline number).
//! * `pickup`: submit → pickup task start (scheduler-pickup latency).
//! * `signal_resume`: signal-sent → completion (how fast the resumed
//!   workflow gets re-dispatched). Temporal and Inngest both publish
//!   this independently because the signal-driven path is a different
//!   hot path from the polling-driven dispatch path.

use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use bytes::Bytes;
use hdrhistogram::Histogram;
use sayiir_core::codec::Encoder;
use sayiir_core::context::WorkflowContext;
use sayiir_core::workflow::WorkflowBuilder;
use sayiir_postgres::{PostgresBackend, wakeup_drops_total};
use sayiir_runtime::serialization::JsonCodec;
use sayiir_runtime::{PooledWorker, WorkflowClient};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use tokio::sync::Semaphore;

use crate::SignalDrivenArgs;
use crate::driver::submit_bounded;
use crate::metrics::{COMPLETION_TX, PICKUP_TX, record_completion, record_pickup};
use crate::report::LatencyBlock;

/// In-flight task cap per bench worker.
const WORKER_PARALLELISM: std::num::NonZeroUsize = std::num::NonZeroUsize::new(2).unwrap();

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
struct State {
    id: u64,
    counter: u32,
}

const HISTOGRAM_HIGH_NS: u64 = 600_000_000_000;
const SIGNAL_NAME: &str = "kick";

pub async fn run(ctx: crate::CommonContext, args: SignalDrivenArgs) -> Result<()> {
    tracing::info!(
        workflows = args.workflows,
        concurrency = args.concurrency,
        workers = args.workers,
        "starting signal-driven scenario"
    );

    let pool = build_pool(&ctx.postgres_url, args.concurrency, args.workers).await?;
    let backend = PostgresBackend::<JsonCodec>::connect_with(pool.clone())
        .await
        .context("connecting client backend")?;
    if ctx.reset_db {
        crate::reset_sayiir_tables(&pool)
            .await
            .context("truncating sayiir tables")?;
        tracing::info!("sayiir tables truncated");
    }

    let workflow = Arc::new(build_workflow(args.signal_timeout_secs));
    let def_hash = workflow.definition_hash().clone();
    let backend_arc = Arc::new(backend);
    let client = Arc::new(WorkflowClient::from_shared(Arc::clone(&backend_arc)));

    let (completion_tx, mut completion_rx) = tokio::sync::mpsc::unbounded_channel();
    COMPLETION_TX
        .set(completion_tx)
        .map_err(|_| anyhow::anyhow!("completion channel already initialised"))?;
    let (pickup_tx, mut pickup_rx) = tokio::sync::mpsc::unbounded_channel();
    PICKUP_TX
        .set(pickup_tx)
        .map_err(|_| anyhow::anyhow!("pickup channel already initialised"))?;

    let workers = spawn_workers(
        backend_arc.as_ref(),
        args.workers,
        Duration::from_millis(args.poll_ms),
        NonZeroUsize::new(args.batch_size.max(1)).unwrap(),
        def_hash.clone(),
        Arc::clone(&workflow),
    );

    let wakeup_drops_baseline = wakeup_drops_total();
    let bench_start = Instant::now();

    let submit_times: Vec<AtomicU64> = (0..args.workflows).map(|_| AtomicU64::new(0)).collect();
    let pickup_times: Vec<AtomicU64> = (0..args.workflows).map(|_| AtomicU64::new(0)).collect();
    let signal_sent_times: Vec<AtomicU64> =
        (0..args.workflows).map(|_| AtomicU64::new(0)).collect();
    let submit_times = Arc::new(submit_times);
    let pickup_times = Arc::new(pickup_times);
    let signal_sent_times = Arc::new(signal_sent_times);

    // Submission burst.
    let submitter = {
        let client = Arc::clone(&client);
        let workflow = Arc::clone(&workflow);
        let submit_times = Arc::clone(&submit_times);
        let total = args.workflows;
        let concurrency = args.concurrency;
        tokio::spawn(async move {
            submit_bounded(total, concurrency, None, move |i| {
                let client = Arc::clone(&client);
                let workflow = Arc::clone(&workflow);
                let submit_times = Arc::clone(&submit_times);
                async move {
                    let now_ns = bench_start.elapsed().as_nanos() as u64;
                    submit_times[i as usize].store(now_ns, Ordering::Relaxed);
                    client
                        .submit(
                            workflow.as_ref(),
                            format!("sd-{i}"),
                            State { id: i, counter: 0 },
                        )
                        .await
                        .map(|_| ())
                        .map_err(anyhow::Error::from)
                }
            })
            .await
        })
    };

    // Signal sender: triggered by pickup events. We can't send the
    // signal *before* pickup completes because the workflow needs to
    // have reached the `wait_for_signal` node — sending earlier than
    // that races against the snapshot insert and the event would be
    // buffered but require a poll to land. Keying off pickup gives us
    // a clean "park + signal" measurement.
    let codec = Arc::new(JsonCodec);
    let signal_sender = {
        let client = Arc::clone(&client);
        let signal_sent_times = Arc::clone(&signal_sent_times);
        let codec = Arc::clone(&codec);
        let semaphore = Arc::new(Semaphore::new(args.concurrency));
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<u64>();

        // Pickup-watcher task: drains pickup_rx, records the pickup
        // timestamp, forwards the index to the sender task. Decoupling
        // pickup-record from signal-send keeps signal latency from
        // queuing up behind the bounded semaphore.
        let pickup_times_w = Arc::clone(&pickup_times);
        let submit_times_w = Arc::clone(&submit_times);
        // Move pickup_rx into watcher so the main collection loop
        // receives only completions (and timer ticks).
        let pickup_drain = tokio::spawn(async move {
            while let Some(p) = pickup_rx.recv().await {
                let idx = p.workflow_index as usize;
                if let Some(slot) = pickup_times_w.get(idx) {
                    let submit_ns = submit_times_w[idx].load(Ordering::Relaxed);
                    if submit_ns != 0 {
                        let pickup_ns = p.at.duration_since(bench_start).as_nanos() as u64;
                        slot.store(pickup_ns, Ordering::Relaxed);
                        let _ = tx.send(p.workflow_index);
                    }
                }
            }
        });

        let send_task = tokio::spawn(async move {
            while let Some(idx) = rx.recv().await {
                let Ok(permit) = Arc::clone(&semaphore).acquire_owned().await else {
                    break;
                };
                let client = Arc::clone(&client);
                let signal_sent_times = Arc::clone(&signal_sent_times);
                let codec = Arc::clone(&codec);
                tokio::spawn(async move {
                    let _permit = permit;
                    let payload_state = State {
                        id: idx,
                        counter: 99,
                    };
                    let bytes: Bytes = codec
                        .encode(&payload_state)
                        .ok()
                        .map_or_else(Bytes::new, std::convert::Into::into);
                    let sent_ns = bench_start.elapsed().as_nanos() as u64;
                    signal_sent_times[idx as usize].store(sent_ns, Ordering::Relaxed);
                    if let Err(e) = client
                        .send_event(&format!("sd-{idx}"), SIGNAL_NAME, bytes)
                        .await
                    {
                        tracing::warn!(idx, error = %e, "signal send failed");
                    }
                });
            }
        });

        (pickup_drain, send_task)
    };

    let mut e2e_hist = Histogram::<u64>::new_with_bounds(1_000, HISTOGRAM_HIGH_NS, 3)?;
    let mut pickup_hist = Histogram::<u64>::new_with_bounds(1_000, HISTOGRAM_HIGH_NS, 3)?;
    let mut signal_resume_hist = Histogram::<u64>::new_with_bounds(1_000, HISTOGRAM_HIGH_NS, 3)?;
    let mut completed = 0usize;
    let mut samples: Vec<(Duration, usize)> = Vec::new();
    let collect_deadline = Instant::now() + Duration::from_secs(120 + args.workflows as u64 / 100);
    let mut sample_tick = tokio::time::interval_at(
        tokio::time::Instant::now() + Duration::from_millis(100),
        Duration::from_millis(100),
    );
    sample_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    while completed < args.workflows {
        let remaining = collect_deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            tracing::warn!(completed, expected = args.workflows, "signal: deadline hit");
            break;
        }
        tokio::select! {
            biased;
            comp = completion_rx.recv() => {
                let Some(c) = comp else { break };
                let idx = c.workflow_index as usize;
                if idx >= args.workflows { continue }
                let submit_ns = submit_times[idx].load(Ordering::Relaxed);
                let pickup_ns = pickup_times[idx].load(Ordering::Relaxed);
                let sent_ns = signal_sent_times[idx].load(Ordering::Relaxed);
                if submit_ns == 0 || pickup_ns == 0 || sent_ns == 0 {
                    continue;
                }
                let complete_ns = c.at.duration_since(bench_start).as_nanos() as u64;
                e2e_hist.record(complete_ns.saturating_sub(submit_ns)).ok();
                pickup_hist.record(pickup_ns.saturating_sub(submit_ns)).ok();
                signal_resume_hist.record(complete_ns.saturating_sub(sent_ns)).ok();
                completed += 1;
            }
            _ = sample_tick.tick() => {
                samples.push((bench_start.elapsed(), completed));
            }
            () = tokio::time::sleep(remaining) => {
                tracing::warn!(completed, "signal: completion recv timed out");
                break;
            }
        }
    }

    let _ = submitter.await;
    // Drop pickup/signal sender tasks — they hold the channels but the
    // bench is done; aborting is fine, the underlying client is shared
    // and the main loop is the source of truth for completion.
    signal_sender.0.abort();
    signal_sender.1.abort();
    for w in workers {
        w.shutdown();
        let _ = w.join().await;
    }

    let total_elapsed = bench_start.elapsed();
    let sustained = crate::report::best_window_rate(&samples, Duration::from_mins(1));
    let wakeup_drops = wakeup_drops_total().saturating_sub(wakeup_drops_baseline);

    let mut latency = BTreeMap::new();
    latency.insert(
        "e2e".to_string(),
        LatencyBlock::from_histogram_ns(&e2e_hist),
    );
    latency.insert(
        "pickup".to_string(),
        LatencyBlock::from_histogram_ns(&pickup_hist),
    );
    latency.insert(
        "signal_resume".to_string(),
        LatencyBlock::from_histogram_ns(&signal_resume_hist),
    );

    tracing::info!(
        completed,
        expected = args.workflows,
        sustained,
        e2e_p50_ms = latency.get("e2e").map(|l| l.p50).unwrap_or(0.0),
        e2e_p99_ms = latency.get("e2e").map(|l| l.p99).unwrap_or(0.0),
        signal_resume_p99_ms = latency.get("signal_resume").map(|l| l.p99).unwrap_or(0.0),
        "signal-driven summary"
    );

    let pg_info = crate::report::collect_postgres_info(&pool).await;
    let prom = crate::report::prometheus_snapshot(&ctx.prometheus_url).await;
    let samples_json = samples
        .iter()
        .map(|(d, n)| crate::report::Sample {
            t_ms: u64::try_from(d.as_millis()).unwrap_or(u64::MAX),
            completed: *n,
        })
        .collect();
    let params_json = serde_json::json!({
        "workflows": args.workflows,
        "concurrency": args.concurrency,
        "workers": args.workers,
        "poll_ms": args.poll_ms,
        "batch_size": args.batch_size,
        "signal_timeout_secs": args.signal_timeout_secs,
    });
    let report = crate::report::build_report(
        "signal-driven",
        params_json,
        completed,
        args.workflows,
        0,
        0,
        total_elapsed,
        sustained,
        3, // pickup + signal-resume node + final_emit
        latency,
        samples_json,
        Some(wakeup_drops),
        pg_info,
        prom,
    );
    if let Err(e) = crate::report::write_report(&report, &ctx.results_dir) {
        tracing::warn!(error = %e, "failed to write report");
    }

    if completed < args.workflows {
        anyhow::bail!(
            "incomplete run: {} of {} completed",
            completed,
            args.workflows
        );
    }
    Ok(())
}

async fn build_pool(url: &str, concurrency: usize, workers: usize) -> Result<sqlx::PgPool> {
    let target = (concurrency + workers * 4 + 32) as u32;
    PgPoolOptions::new()
        .max_connections(target)
        .acquire_timeout(Duration::from_mins(1))
        .connect(url)
        .await
        .with_context(|| format!("connecting to postgres at {url}"))
}

fn build_workflow(timeout_secs: u64) -> sayiir_core::workflow::Workflow<JsonCodec, State, ()> {
    let ctx = WorkflowContext::new("bench-signal", Arc::new(JsonCodec), Arc::new(()));
    WorkflowBuilder::new(ctx)
        .then("pickup", |s: State| async move {
            record_pickup(s.id);
            Ok(s)
        })
        .wait_for_signal(
            "await_kick",
            SIGNAL_NAME,
            Some(Duration::from_secs(timeout_secs)),
        )
        .then("final_emit", |s: State| async move {
            record_completion(s.id);
            Ok(s)
        })
        .build()
        .expect("workflow build")
}

fn spawn_workers(
    backend: &PostgresBackend<JsonCodec>,
    n: usize,
    poll: Duration,
    batch_size: NonZeroUsize,
    def_hash: sayiir_core::DefinitionHash,
    workflow: Arc<sayiir_core::workflow::Workflow<JsonCodec, State, ()>>,
) -> Vec<sayiir_runtime::WorkerHandle<PostgresBackend<JsonCodec>>> {
    let mut handles = Vec::with_capacity(n);
    for i in 0..n {
        let worker_backend = backend.clone();
        let registry = sayiir_core::registry::TaskRegistry::new();
        let worker = PooledWorker::new(format!("sd-worker-{i}"), worker_backend, registry)
            .with_claim_ttl(Some(Duration::from_mins(2)))
            .with_batch_size(batch_size)
            .with_max_concurrent_tasks(WORKER_PARALLELISM);
        let entries = vec![(def_hash.clone(), Arc::clone(&workflow))];
        handles.push(worker.spawn(poll, entries));
    }
    handles
}

