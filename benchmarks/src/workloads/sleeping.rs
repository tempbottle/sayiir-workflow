//! Sleeping giants — long-timer durable parking + wake storm.
//!
//! Workflow shape:
//! ```text
//!  AcceptId → delay(sleep_secs) → WakeTask → FinalEmit
//! ```
//!
//! Three phases:
//! 1. **Submission** — fan out N workflows. Each runs `AcceptId` then parks
//!    at the durable delay. PG storage grows linearly with N; runtime RSS
//!    should stay flat (that's the headline number).
//! 2. **Steady state** — for the configured sleep duration, the driver
//!    samples (in-flight, completed) at 1 Hz. RSS / DB-size enrichment
//!    comes from Prometheus / postgres_exporter via the report snapshot.
//! 3. **Wake storm** — timers fire; workers wake every workflow, run the
//!    two trailing tasks, and emit completion. We measure the wake-up
//!    throughput and per-workflow wake-to-complete latency.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};

use std::collections::BTreeMap;

use anyhow::{Context, Result};
use hdrhistogram::Histogram;
use sayiir_core::context::WorkflowContext;
use sayiir_core::workflow::WorkflowBuilder;
use sayiir_postgres::{PostgresBackend, wakeup_drops_total};
use sayiir_runtime::serialization::JsonCodec;
use sayiir_runtime::{PooledWorker, WorkflowClient};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;

use crate::SleepingGiantsArgs;
use crate::driver::submit_bounded;
use crate::metrics::{COMPLETION_TX, record_completion};
use crate::report::LatencyBlock;

/// In-flight task cap per bench worker.
const WORKER_PARALLELISM: std::num::NonZeroUsize = std::num::NonZeroUsize::new(2).unwrap();

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
struct State {
    id: u64,
}

pub async fn run(ctx: crate::CommonContext, args: SleepingGiantsArgs) -> Result<()> {
    tracing::info!(
        workflows = args.workflows,
        concurrency = args.concurrency,
        workers = args.workers,
        sleep_secs = args.sleep_secs,
        demo_restart = args.demo_restart,
        "starting sleeping-giants scenario"
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

    let wakeup_drops_baseline = wakeup_drops_total();
    let workflow = Arc::new(build_workflow(args.sleep_secs));
    let def_hash = workflow.definition_hash().clone();
    let backend_arc = Arc::new(backend);
    let client = Arc::new(WorkflowClient::from_shared(Arc::clone(&backend_arc)));

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    COMPLETION_TX
        .set(tx)
        .map_err(|_| anyhow::anyhow!("completion channel already initialised"))?;

    let workers = spawn_workers(
        backend_arc.as_ref(),
        args.workers,
        Duration::from_millis(args.poll_ms),
        NonZeroUsize::new(args.batch_size.max(1)).unwrap(),
        def_hash.clone(),
        Arc::clone(&workflow),
    );

    // ── Phase 1: submission burst ───────────────────────────────────────
    let bench_start = Instant::now();
    let submit_start = Instant::now();
    let submitter = {
        let client = Arc::clone(&client);
        let workflow = Arc::clone(&workflow);
        let total = args.workflows;
        let concurrency = args.concurrency;
        tokio::spawn(async move {
            submit_bounded(total, concurrency, None, move |i| {
                let client = Arc::clone(&client);
                let workflow = Arc::clone(&workflow);
                async move {
                    let res = client
                        .submit(workflow.as_ref(), format!("sg-{i}"), State { id: i })
                        .await
                        .map(|_| ())
                        .map_err(anyhow::Error::from);
                    if res.is_ok() {
                        metrics::counter!("sayiir_bench_sleeping_submitted_total").increment(1);
                    }
                    res
                }
            })
            .await
        })
    };
    let _ = submitter.await;
    let submit_duration = submit_start.elapsed();
    tracing::info!(
        submit_duration_s = submit_duration.as_secs_f64(),
        submission_rate_wf_per_sec = args.workflows as f64 / submit_duration.as_secs_f64(),
        "submission burst done"
    );

    // ── Phase 2: steady-state sampling ──────────────────────────────────
    // Most workflows are now parked at AtDelay. Sample every second until
    // the configured sleep elapses + a safety margin so we catch the storm.
    let sleep_total = Duration::from_secs(args.sleep_secs);
    let storm_deadline =
        bench_start + sleep_total + Duration::from_secs(120 + args.workflows as u64 / 100);

    let mut histogram = Histogram::<u64>::new_with_bounds(1_000, 600_000_000_000, 3)
        .context("building histogram")?;
    let mut completed = 0usize;
    let mut samples: Vec<(Duration, usize)> = Vec::new();
    let mut next_sample = bench_start + Duration::from_secs(1);

    // Wake storm starts approximately when the last submitted workflow's
    // sleep elapses. We treat that as `submit_start + sleep_total` —
    // i.e. measure latency relative to "the earliest a workflow could
    // first become wakeable". Per-workflow precision isn't needed for the
    // wake-storm headline number.
    let wake_origin = submit_start + sleep_total;

    while completed < args.workflows {
        let now = Instant::now();
        if now > storm_deadline {
            tracing::warn!(completed, expected = args.workflows, "storm deadline hit");
            break;
        }
        let recv_timeout = next_sample
            .saturating_duration_since(now)
            .max(Duration::from_millis(50));
        match tokio::time::timeout(recv_timeout, rx.recv()).await {
            Ok(Some(c)) => {
                completed += 1;
                metrics::counter!("sayiir_bench_sleeping_completed_total").increment(1);
                if c.at >= wake_origin {
                    let latency_ns = c.at.duration_since(wake_origin).as_nanos() as u64;
                    histogram.record(latency_ns).ok();
                    metrics::histogram!("sayiir_bench_sleeping_wake_latency_ms")
                        .record(latency_ns as f64 / 1_000_000.0);
                }
            }
            Ok(None) => break,
            Err(_) => {} // timer tick, fall through to sample
        }
        // Sample once per second.
        let now = Instant::now();
        if now >= next_sample {
            let elapsed = now.duration_since(bench_start);
            let in_flight = args.workflows.saturating_sub(completed);
            samples.push((elapsed, completed));
            metrics::gauge!("sayiir_bench_sleeping_in_flight").set(in_flight as f64);
            tracing::info!(
                t_s = elapsed.as_secs(),
                completed,
                in_flight,
                "steady-state sample"
            );
            next_sample = now + Duration::from_secs(1);
        }
    }

    let total_elapsed = bench_start.elapsed();
    // Wake-phase throughput: completions divided by the span from when the
    // first wake landed (≈ `wake_origin`) to the last completion observed.
    let wake_sustained = if total_elapsed >= sleep_total && completed > 0 {
        let wake_span = total_elapsed.saturating_sub(sleep_total).as_secs_f64();
        if wake_span > 0.0 {
            completed as f64 / wake_span
        } else {
            0.0
        }
    } else {
        0.0
    };

    print_summary(
        args.workflows,
        completed,
        submit_duration,
        total_elapsed,
        wake_sustained,
        &histogram,
    );

    // Shutdown workers cleanly.
    for w in workers {
        w.shutdown();
        let _ = w.join().await;
    }

    let wakeup_drops = wakeup_drops_total().saturating_sub(wakeup_drops_baseline);

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
        "sleep_secs": args.sleep_secs,
        "demo_restart": args.demo_restart,
    });
    let mut latency = BTreeMap::new();
    latency.insert(
        "wake".to_string(),
        LatencyBlock::from_histogram_ns(&histogram),
    );
    let report = crate::report::build_report(
        "sleeping-giants",
        params_json,
        completed,
        args.workflows,
        0,
        0,
        total_elapsed,
        wake_sustained,
        // accept_id → delay-wake → wake_task → final_emit = 4
        4,
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
            "incomplete run: woke {} of {} workflows before storm deadline",
            completed,
            args.workflows
        );
    }
    Ok(())
}

fn build_workflow(sleep_secs: u64) -> sayiir_core::workflow::Workflow<JsonCodec, State, ()> {
    let ctx = WorkflowContext::new("bench-sleeping", Arc::new(JsonCodec), Arc::new(()));
    WorkflowBuilder::new(ctx)
        .then("accept_id", |s: State| async move { Ok(s) })
        .delay("sleep", Duration::from_secs(sleep_secs))
        .then("wake_task", |s: State| async move { Ok(s) })
        .then("final_emit", |s: State| async move {
            record_completion(s.id);
            Ok(s)
        })
        .build()
        .expect("workflow build")
}

async fn build_pool(url: &str, concurrency: usize, workers: usize) -> Result<sqlx::PgPool> {
    let target = (concurrency + workers * 4 + 16) as u32;
    PgPoolOptions::new()
        .max_connections(target)
        .acquire_timeout(Duration::from_mins(1))
        .connect(url)
        .await
        .with_context(|| format!("connecting to postgres at {url}"))
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
        let worker = PooledWorker::new(format!("sg-worker-{i}"), worker_backend, registry)
            .with_claim_ttl(Some(Duration::from_mins(2)))
            .with_batch_size(batch_size)
            .with_max_concurrent_tasks(WORKER_PARALLELISM);
        let entries = vec![(def_hash.clone(), Arc::clone(&workflow))];
        handles.push(worker.spawn(poll, entries));
    }
    handles
}

fn print_summary(
    expected: usize,
    completed: usize,
    submit_duration: Duration,
    total_elapsed: Duration,
    wake_sustained: f64,
    h: &Histogram<u64>,
) {
    let p = |q: f64| Duration::from_nanos(h.value_at_quantile(q)).as_secs_f64() * 1000.0;
    tracing::info!(
        completed,
        expected,
        submission_rate_wf_per_sec = expected as f64 / submit_duration.as_secs_f64(),
        total_elapsed_s = total_elapsed.as_secs_f64(),
        wake_sustained_wf_per_sec = wake_sustained,
        wake_p50_ms = p(0.50),
        wake_p95_ms = p(0.95),
        wake_p99_ms = p(0.99),
        wake_p99_9_ms = p(0.999),
        "sleeping-giants summary"
    );
}
