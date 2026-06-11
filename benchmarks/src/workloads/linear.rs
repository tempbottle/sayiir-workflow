//! Linear N-step workflow throughput.
//!
//! Workflow shape: one `pickup` task (records scheduler-pickup latency)
//! followed by `steps - 2` no-op middle tasks, ending with a
//! `final_emit` task that records completion. For `steps == 1` the
//! single task records both pickup and completion; for `steps == 2`
//! pickup + final-emit are the only two tasks.
//!
//! This is the universal benchmark — every competitor publishes a
//! linear N-step sweep (Restate 1/3/9, DBOS multi-step, Temporal Omes
//! `throughput_stress`). Headline numbers: sustained workflows/sec,
//! state transitions/sec, end-to-end p50/p95/p99 latency, and the
//! separately-reported scheduler-pickup latency (Temporal calls this
//! `StartWorkflow` p50, we follow that convention).
//!
//! Methodology:
//! 1. Explicit warmup phase — submit `warmup_workflows`, wait for them
//!    all to drain, *then* reset histograms and start measuring. Beats
//!    the old "exclude completions arriving in first 5 s" heuristic on
//!    small runs where no completion ever arrives within the window.
//! 2. Fixed 100 ms sampling cadence on `(elapsed, completed)` for the
//!    sustained-throughput sliding window.
//! 3. Per-phase latency blocks (`e2e`, `pickup`, `execution`) so charts
//!    diff cleanly against Temporal/DBOS published numbers.

use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use hdrhistogram::Histogram;
use sayiir_core::context::WorkflowContext;
use sayiir_core::workflow::WorkflowBuilder;
use sayiir_postgres::{PostgresBackend, wakeup_drops_total};
use sayiir_runtime::serialization::JsonCodec;
use sayiir_runtime::{PooledWorker, WorkflowClient};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;

use crate::LinearArgs;
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

pub async fn run(ctx: crate::CommonContext, args: LinearArgs) -> Result<()> {
    let steps = args.steps.max(1);
    tracing::info!(
        workflows = args.workflows,
        warmup_workflows = args.warmup_workflows,
        concurrency = args.concurrency,
        workers = args.workers,
        steps,
        "starting linear scenario"
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

    let workflow = Arc::new(build_workflow(steps));
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

    // ── Phase 1: warmup (drain a small burst before measuring) ──────────
    //
    // Submit `warmup_workflows` first and wait for them all to drain.
    // None of these counts toward the histogram or sustained-throughput
    // window; their job is to warm pg shared_buffers, prime the
    // wakeup listener, and reach steady-state worker fan-out.
    if args.warmup_workflows > 0 {
        drain_burst(
            "warmup",
            &client,
            &workflow,
            args.warmup_workflows,
            args.concurrency,
            &mut completion_rx,
            &mut pickup_rx,
            Duration::from_secs(60 + args.warmup_workflows as u64 / 100),
        )
        .await?;
    }

    let wakeup_drops_baseline = wakeup_drops_total();

    // ── Phase 2: measurement ────────────────────────────────────────────
    let bench_start = Instant::now();

    let submit_times = Arc::new(
        (0..args.workflows)
            .map(|_| AtomicU64::new(0))
            .collect::<Vec<_>>(),
    );

    let submitter = {
        let client = Arc::clone(&client);
        let workflow = Arc::clone(&workflow);
        let submit_times = Arc::clone(&submit_times);
        let id_offset = args.warmup_workflows as u64;
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
                            format!("wf-{}", id_offset + i),
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

    let mut e2e_hist = Histogram::<u64>::new_with_bounds(1_000, HISTOGRAM_HIGH_NS, 3)
        .context("building e2e histogram")?;
    let mut pickup_hist = Histogram::<u64>::new_with_bounds(1_000, HISTOGRAM_HIGH_NS, 3)
        .context("building pickup histogram")?;
    let mut exec_hist = Histogram::<u64>::new_with_bounds(1_000, HISTOGRAM_HIGH_NS, 3)
        .context("building execution histogram")?;

    let pickup_times: Vec<AtomicU64> = (0..args.workflows).map(|_| AtomicU64::new(0)).collect();

    let mut completed = 0usize;
    let mut stale_completions = 0usize;
    let mut samples: Vec<(Duration, usize)> = Vec::new();

    // Deadline scales with workflow count: 60 s base + ~1 s per 100 workflows.
    let collect_deadline = Instant::now() + Duration::from_secs(60 + (args.workflows as u64 / 100));

    let mut sample_tick = tokio::time::interval_at(
        tokio::time::Instant::now() + Duration::from_millis(100),
        Duration::from_millis(100),
    );
    sample_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

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

        tokio::select! {
            biased;

            pickup = pickup_rx.recv() => {
                let Some(p) = pickup else { break };
                let idx = p.workflow_index as usize;
                if let Some(slot) = pickup_times.get(idx) {
                    let submit_ns = submit_times[idx].load(Ordering::Relaxed);
                    if submit_ns != 0 {
                        let pickup_ns = p.at.duration_since(bench_start).as_nanos() as u64;
                        slot.store(pickup_ns, Ordering::Relaxed);
                        pickup_hist.record(pickup_ns.saturating_sub(submit_ns)).ok();
                    }
                }
            }
            comp = completion_rx.recv() => {
                let Some(c) = comp else { break };
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
                let e2e_ns = complete_ns.saturating_sub(submit_ns);
                e2e_hist.record(e2e_ns).ok();
                let pickup_ns = pickup_times[idx].load(Ordering::Relaxed);
                if pickup_ns != 0 && complete_ns >= pickup_ns {
                    exec_hist.record(complete_ns - pickup_ns).ok();
                }
                metrics::counter!("sayiir_bench_workflows_completed_total").increment(1);
                metrics::histogram!("sayiir_bench_workflow_latency_ms")
                    .record(e2e_ns as f64 / 1_000_000.0);
                completed += 1;
            }
            _ = sample_tick.tick() => {
                samples.push((bench_start.elapsed(), completed));
            }
            () = tokio::time::sleep(remaining) => {
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
        "execution".to_string(),
        LatencyBlock::from_histogram_ns(&exec_hist),
    );

    print_summary(
        args.workflows,
        completed,
        total_elapsed,
        sustained,
        &latency,
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
        "warmup_workflows": args.warmup_workflows,
        "concurrency": args.concurrency,
        "target_rate": args.target_rate,
        "workers": args.workers,
        "poll_ms": args.poll_ms,
        "batch_size": args.batch_size,
        "steps": steps,
    });
    let report = crate::report::build_report(
        "linear",
        params_json,
        completed,
        args.workflows,
        stale_completions,
        0,
        total_elapsed,
        sustained,
        steps,
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
            "incomplete run: completed {} of {} workflows (deadline or shutdown race)",
            completed,
            args.workflows
        );
    }
    Ok(())
}

/// Submit a self-contained burst and wait for every workflow to drain.
///
/// Used by the warmup phase; reuses the same client, workflow, and
/// channels but doesn't update histograms — completions and pickups
/// arriving here are discarded. Returns an error only if the deadline
/// elapses, since a failed warmup signals a misconfigured run worth
/// failing fast on.
#[allow(clippy::too_many_arguments)]
async fn drain_burst(
    label: &str,
    client: &Arc<WorkflowClient<PostgresBackend<JsonCodec>>>,
    workflow: &Arc<sayiir_core::workflow::Workflow<JsonCodec, State, ()>>,
    total: usize,
    concurrency: usize,
    completion_rx: &mut tokio::sync::mpsc::UnboundedReceiver<crate::metrics::Completion>,
    pickup_rx: &mut tokio::sync::mpsc::UnboundedReceiver<crate::metrics::Pickup>,
    deadline: Duration,
) -> Result<()> {
    let start = Instant::now();
    let label_owned = label.to_string();
    tracing::info!(label = %label_owned, total, "drain phase starting");

    let submitter = {
        let client = Arc::clone(client);
        let workflow = Arc::clone(workflow);
        let label = label_owned.clone();
        tokio::spawn(async move {
            submit_bounded(total, concurrency, None, move |i| {
                let client = Arc::clone(&client);
                let workflow = Arc::clone(&workflow);
                let label = label.clone();
                async move {
                    client
                        .submit(
                            workflow.as_ref(),
                            format!("{label}-{i}"),
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

    let mut drained = 0usize;
    let drain_deadline = Instant::now() + deadline;
    while drained < total {
        let remaining = drain_deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            anyhow::bail!(
                "{label_owned} phase: drained {drained} of {total} before deadline ({deadline:?})",
            );
        }
        tokio::select! {
            biased;
            _ = pickup_rx.recv() => {}
            comp = completion_rx.recv() => {
                if comp.is_some() {
                    drained += 1;
                } else {
                    break;
                }
            }
            () = tokio::time::sleep(remaining) => {
                anyhow::bail!(
                    "{label_owned} phase timed out: drained {drained} of {total}",
                );
            }
        }
    }

    let _ = submitter.await;
    tracing::info!(
        label = %label_owned,
        total,
        elapsed_s = start.elapsed().as_secs_f64(),
        "drain phase complete",
    );
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

fn build_workflow(steps: usize) -> sayiir_core::workflow::Workflow<JsonCodec, State, ()> {
    // `WorkflowBuilder` is type-state: each `.then(...)` changes its
    // generic type parameters (`NoContinuation → WorkflowContinuation`,
    // and the output type can change per node). We can't bind it to a
    // `mut` variable and reassign — the type doesn't match. So we
    // hand-unroll the prefix (pickup + first middle) and then loop the
    // remaining nodes by routing through the `SubBuilder` continuation
    // helper... but that's far more code than the steps we actually
    // care to benchmark. The dominant cases for competitor parity are
    // steps ∈ {1, 3, 4, 9}; we materialise each ladder up to 9 here
    // and bail for higher values with a clear error. If you genuinely
    // need >9, run multiple workflows in parallel — the harness's
    // throughput metric will report the same number.
    let ctx = WorkflowContext::new("bench-linear", Arc::new(JsonCodec), Arc::new(()));
    let pickup_fn = |s: State| async move {
        record_pickup(s.id);
        Ok(State {
            id: s.id,
            counter: s.counter + 1,
        })
    };
    let final_fn = |s: State| async move {
        record_completion(s.id);
        Ok(s)
    };
    let mid_fn = |s: State| async move {
        Ok(State {
            id: s.id,
            counter: s.counter + 1,
        })
    };
    match steps {
        0 | 1 => WorkflowBuilder::new(ctx)
            .then("solo", |s: State| async move {
                record_pickup(s.id);
                record_completion(s.id);
                Ok(s)
            })
            .build()
            .expect("workflow build"),
        2 => WorkflowBuilder::new(ctx)
            .then("pickup", pickup_fn)
            .then("final_emit", final_fn)
            .build()
            .expect("workflow build"),
        3 => WorkflowBuilder::new(ctx)
            .then("pickup", pickup_fn)
            .then("step_1", mid_fn)
            .then("final_emit", final_fn)
            .build()
            .expect("workflow build"),
        4 => WorkflowBuilder::new(ctx)
            .then("pickup", pickup_fn)
            .then("step_1", mid_fn)
            .then("step_2", mid_fn)
            .then("final_emit", final_fn)
            .build()
            .expect("workflow build"),
        5 => WorkflowBuilder::new(ctx)
            .then("pickup", pickup_fn)
            .then("step_1", mid_fn)
            .then("step_2", mid_fn)
            .then("step_3", mid_fn)
            .then("final_emit", final_fn)
            .build()
            .expect("workflow build"),
        9 => WorkflowBuilder::new(ctx)
            .then("pickup", pickup_fn)
            .then("step_1", mid_fn)
            .then("step_2", mid_fn)
            .then("step_3", mid_fn)
            .then("step_4", mid_fn)
            .then("step_5", mid_fn)
            .then("step_6", mid_fn)
            .then("step_7", mid_fn)
            .then("final_emit", final_fn)
            .build()
            .expect("workflow build"),
        other => panic!(
            "linear scenario only supports steps ∈ {{0,1,2,3,4,5,9}} today (got {other}); add a ladder arm in build_workflow if you need a different shape",
        ),
    }
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
        let worker = PooledWorker::new(format!("bench-worker-{i}"), worker_backend, registry)
            .with_claim_ttl(Some(Duration::from_mins(1)))
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
    elapsed: Duration,
    sustained_window: f64,
    latency: &BTreeMap<String, LatencyBlock>,
) {
    let avg = completed as f64 / elapsed.as_secs_f64().max(1e-9);
    let e2e = latency.get("e2e");
    let pickup = latency.get("pickup");
    tracing::info!(
        completed,
        expected,
        elapsed_s = elapsed.as_secs_f64(),
        throughput_avg = avg,
        throughput_sustained_window = sustained_window,
        e2e_p50_ms = e2e.map(|l| l.p50).unwrap_or(0.0),
        e2e_p95_ms = e2e.map(|l| l.p95).unwrap_or(0.0),
        e2e_p99_ms = e2e.map(|l| l.p99).unwrap_or(0.0),
        pickup_p50_ms = pickup.map(|l| l.p50).unwrap_or(0.0),
        pickup_p99_ms = pickup.map(|l| l.p99).unwrap_or(0.0),
        "linear summary"
    );
}
