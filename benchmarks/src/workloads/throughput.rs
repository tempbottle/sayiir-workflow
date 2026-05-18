//! Throughput burst — short transactional workflows.
//!
//! Workflow: three increment steps followed by a final-emit step that signals
//! completion back to the driver via the global completion channel.
//!
//! Headline numbers: sustained workflows/sec, end-to-end p50/p95/p99/p99.9.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use hdrhistogram::Histogram;
use sayiir_core::context::WorkflowContext;
use sayiir_core::workflow::WorkflowBuilder;
use sayiir_postgres::PostgresBackend;
use sayiir_runtime::serialization::JsonCodec;
use sayiir_runtime::{PooledWorker, WorkflowClient};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;

use crate::ThroughputArgs;
use crate::driver::submit_bounded;
use crate::metrics::{COMPLETION_TX, record_completion};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
struct State {
    id: u64,
    counter: u32,
}

pub async fn run(ctx: crate::CommonContext, args: ThroughputArgs) -> Result<()> {
    tracing::info!(
        workflows = args.workflows,
        concurrency = args.concurrency,
        workers = args.workers,
        "starting throughput scenario"
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

    let workflow = Arc::new(build_workflow());
    let def_hash = workflow.definition_hash().clone();
    // Share ONE backend across the client and every worker. Each
    // `PostgresBackend` clone shares the same `WakeupListener`, so all
    // workers in this process subscribe to NOTIFY through ONE 256-cap
    // flume MPMC channel — `flume::Receiver` is mpmc, so each hint goes
    // to exactly one worker. Without this, each `connect_with` would
    // spawn its own listener and we'd amplify NOTIFY traffic N-fold.
    let backend_arc = Arc::new(backend);
    let client = Arc::new(WorkflowClient::from_shared(Arc::clone(&backend_arc)));

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    COMPLETION_TX
        .set(tx)
        .map_err(|_| anyhow::anyhow!("completion channel already initialised"))?;

    let submit_times = Arc::new(
        (0..args.workflows)
            .map(|_| AtomicU64::new(0))
            .collect::<Vec<_>>(),
    );

    let workers = spawn_workers(
        backend_arc.as_ref(),
        args.workers,
        Duration::from_millis(args.poll_ms),
        NonZeroUsize::new(args.batch_size.max(1)).unwrap(),
        def_hash.clone(),
        Arc::clone(&workflow),
    );

    let bench_start = Instant::now();
    let warmup = Duration::from_secs(5);

    let submitter = {
        let client = Arc::clone(&client);
        let workflow = Arc::clone(&workflow);
        let submit_times = Arc::clone(&submit_times);
        let total = args.workflows;
        let concurrency = args.concurrency;
        let target_rate = args.target_rate;
        tokio::spawn(async move {
            submit_bounded(total, concurrency, target_rate, move |i| {
                let client = Arc::clone(&client);
                let workflow = Arc::clone(&workflow);
                let submit_times = Arc::clone(&submit_times);
                async move {
                    let now_ns = bench_start.elapsed().as_nanos() as u64;
                    submit_times[i as usize].store(now_ns, Ordering::Relaxed);
                    let res = client
                        .submit(
                            workflow.as_ref(),
                            format!("wf-{i}"),
                            State { id: i, counter: 0 },
                        )
                        .await
                        .map(|_| ())
                        .map_err(anyhow::Error::from);
                    if res.is_ok() {
                        metrics::counter!("sayiir_bench_workflows_submitted_total").increment(1);
                    }
                    res
                }
            })
            .await
        })
    };

    let mut histogram = Histogram::<u64>::new_with_bounds(1_000, 600_000_000_000, 3)
        .context("building histogram")?;
    let mut completed = 0usize;
    let mut excluded_warmup = 0usize;
    let mut stale_completions = 0usize;
    // Deadline scales with workflow count: 60s base + ~1s per 100 workflows.
    // Generous enough that a healthy run finishes well inside it; tight enough
    // that a stuck run fails fast.
    let collect_deadline =
        Instant::now() + Duration::from_secs(60 + (args.workflows as u64 / 100));

    let mut completion_rate_samples: Vec<(Duration, usize)> = Vec::new();

    while completed < args.workflows {
        let remaining = collect_deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            tracing::warn!(
                completed,
                expected = args.workflows,
                "completion collection deadline reached"
            );
            break;
        }
        match tokio::time::timeout(remaining, rx.recv()).await {
            Ok(Some(c)) => {
                let idx = c.workflow_index as usize;
                if idx >= submit_times.len() {
                    stale_completions += 1;
                    continue;
                }
                let submit_ns = submit_times[idx].load(Ordering::Relaxed);
                if submit_ns == 0 {
                    stale_completions += 1;
                    continue;
                }
                let complete_ns = c.at.duration_since(bench_start).as_nanos() as u64;
                let latency_ns = complete_ns.saturating_sub(submit_ns);
                let latency_ms = latency_ns as f64 / 1_000_000.0;
                metrics::counter!("sayiir_bench_workflows_completed_total").increment(1);
                metrics::histogram!("sayiir_bench_workflow_latency_ms").record(latency_ms);
                if c.at.duration_since(bench_start) >= warmup {
                    histogram.record(latency_ns).ok();
                } else {
                    excluded_warmup += 1;
                }
                completed += 1;
                if completed.is_multiple_of(1000) {
                    completion_rate_samples.push((bench_start.elapsed(), completed));
                }
            }
            Ok(None) => break,
            Err(_) => {
                tracing::warn!(completed, "completion recv timed out");
                break;
            }
        }
    }

    let _ = submitter.await;
    for w in workers {
        w.shutdown();
        let _ = w.join().await;
    }

    let total_elapsed = bench_start.elapsed();
    let sustained = best_window(&completion_rate_samples, Duration::from_secs(60));
    print_summary(
        args.workflows,
        completed,
        excluded_warmup,
        stale_completions,
        total_elapsed,
        sustained,
        &histogram,
    );

    let pg_info = crate::report::collect_postgres_info(&pool).await;
    let prom = crate::report::prometheus_snapshot(&ctx.prometheus_url).await;
    let samples_json = completion_rate_samples
        .iter()
        .map(|(d, n)| crate::report::Sample {
            t_ms: u64::try_from(d.as_millis()).unwrap_or(u64::MAX),
            completed: *n,
        })
        .collect();
    let params_json = serde_json::json!({
        "workflows": args.workflows,
        "concurrency": args.concurrency,
        "target_rate": args.target_rate,
        "workers": args.workers,
        "poll_ms": args.poll_ms,
        "batch_size": args.batch_size,
    });
    let report = crate::report::build_report(
        "throughput",
        params_json,
        completed,
        args.workflows,
        stale_completions,
        excluded_warmup,
        total_elapsed,
        sustained,
        &histogram,
        samples_json,
        pg_info,
        prom,
    );
    if let Err(e) = crate::report::write_report(&report, &ctx.results_dir) {
        tracing::warn!(error = %e, "failed to write report");
    }

    Ok(())
}

async fn build_pool(url: &str, concurrency: usize, workers: usize) -> Result<sqlx::PgPool> {
    let target = (concurrency + workers * 4 + 16) as u32;
    PgPoolOptions::new()
        .max_connections(target)
        .acquire_timeout(Duration::from_secs(30))
        .connect(url)
        .await
        .with_context(|| format!("connecting to postgres at {url}"))
}

fn build_workflow() -> sayiir_core::workflow::Workflow<JsonCodec, State, ()> {
    let ctx = WorkflowContext::new("bench-throughput", Arc::new(JsonCodec), Arc::new(()));
    WorkflowBuilder::new(ctx)
        .then("inc_a", |s: State| async move {
            Ok(State {
                id: s.id,
                counter: s.counter + 1,
            })
        })
        .then("inc_b", |s: State| async move {
            Ok(State {
                id: s.id,
                counter: s.counter + 1,
            })
        })
        .then("inc_c", |s: State| async move {
            Ok(State {
                id: s.id,
                counter: s.counter + 1,
            })
        })
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
        // Clone the SHARED backend: Arc-internal, so all workers in this
        // process pull NOTIFY hints from the same flume MPMC receiver.
        let worker_backend = backend.clone();
        let registry = sayiir_core::registry::TaskRegistry::new();
        let worker = PooledWorker::new(format!("bench-worker-{i}"), worker_backend, registry)
            .with_claim_ttl(Some(Duration::from_secs(60)))
            .with_batch_size(batch_size);
        let entries = vec![(def_hash.clone(), Arc::clone(&workflow))];
        handles.push(worker.spawn(poll, entries));
    }
    handles
}

/// Best sliding-window completions/sec from the (time, cumulative) samples.
///
/// Prefers a `target` window (e.g. 60s), but if the run is shorter than `target`,
/// falls back to the full elapsed range so we still produce a meaningful number
/// on short smoke tests.
fn best_window(samples: &[(Duration, usize)], target: Duration) -> f64 {
    if samples.len() < 2 {
        return match samples.first() {
            Some((dt, n)) if !dt.is_zero() => *n as f64 / dt.as_secs_f64(),
            _ => 0.0,
        };
    }
    let total = samples.last().unwrap().0.saturating_sub(samples[0].0);
    let window = if total < target { total } else { target };
    if window.is_zero() {
        return 0.0;
    }
    let mut best = 0.0f64;
    let mut left = 0usize;
    for right in 0..samples.len() {
        while samples[right].0.saturating_sub(samples[left].0) > window {
            left += 1;
        }
        let dt = samples[right].0.saturating_sub(samples[left].0).as_secs_f64();
        if dt > 0.0 {
            let dn = (samples[right].1 - samples[left].1) as f64;
            let rate = dn / dt;
            if rate > best {
                best = rate;
            }
        }
    }
    best
}

fn print_summary(
    expected: usize,
    completed: usize,
    excluded_warmup: usize,
    stale_completions: usize,
    elapsed: Duration,
    sustained_window: f64,
    h: &Histogram<u64>,
) {
    let avg = completed as f64 / elapsed.as_secs_f64();
    let p = |q: f64| Duration::from_nanos(h.value_at_quantile(q)).as_secs_f64() * 1000.0;
    tracing::info!(
        completed,
        expected,
        excluded_warmup,
        stale_completions,
        elapsed_s = elapsed.as_secs_f64(),
        throughput_avg = avg,
        throughput_sustained_window = sustained_window,
        p50_ms = p(0.50),
        p95_ms = p(0.95),
        p99_ms = p(0.99),
        p99_9_ms = p(0.999),
        "throughput summary"
    );
}
