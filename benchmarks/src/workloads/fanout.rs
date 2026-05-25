//! Fan-out / parallel-children workflow.
//!
//! Workflow shape:
//! ```text
//!  pickup → fork { branch_0, branch_1, …, branch_{K-1} } → join (final_emit)
//! ```
//!
//! Headline metrics:
//! * **Makespan p50/p95/p99** — pickup → join completion latency, the
//!   number that matters for a "spawn K children, wait for all" pattern.
//! * **State transitions/sec** — `1 (pickup) + K (branches) + 1 (join)`
//!   per workflow. With `K=100` and 50 WF/s sustained that's 5K
//!   transitions/sec, the unit Temporal/Restate publish.
//!
//! Used by Temporal Omes scatter-gather scenarios and implicitly by
//! every queue-depth study (Hatchet, Trigger.dev). The most-asked-about
//! shape in real workloads.

use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use hdrhistogram::Histogram;
use sayiir_core::context::WorkflowContext;
use sayiir_core::task::BranchOutputs;
use sayiir_core::workflow::WorkflowBuilder;
use sayiir_postgres::{PostgresBackend, wakeup_drops_total};
use sayiir_runtime::serialization::JsonCodec;
use sayiir_runtime::{PooledWorker, WorkflowClient};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;

use crate::FanoutArgs;
use crate::driver::submit_bounded;
use crate::metrics::{COMPLETION_TX, PICKUP_TX, record_completion, record_pickup};
use crate::report::LatencyBlock;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
struct State {
    id: u64,
}

const HISTOGRAM_HIGH_NS: u64 = 600_000_000_000;

pub async fn run(ctx: crate::CommonContext, args: FanoutArgs) -> Result<()> {
    let children = args.children.max(1);
    tracing::info!(
        workflows = args.workflows,
        children,
        concurrency = args.concurrency,
        workers = args.workers,
        "starting fanout scenario"
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

    let workflow = Arc::new(build_workflow(children));
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

    if args.warmup_workflows > 0 {
        drain_burst(
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
        tokio::spawn(async move {
            submit_bounded(total, concurrency, None, move |i| {
                let client = Arc::clone(&client);
                let workflow = Arc::clone(&workflow);
                let submit_times = Arc::clone(&submit_times);
                async move {
                    let now_ns = bench_start.elapsed().as_nanos() as u64;
                    submit_times[i as usize].store(now_ns, Ordering::Relaxed);
                    let res = client
                        .submit(
                            workflow.as_ref(),
                            format!("fo-{}", id_offset + i),
                            State { id: i },
                        )
                        .await
                        .map(|_| ())
                        .map_err(anyhow::Error::from);
                    if res.is_ok() {
                        metrics::counter!("sayiir_bench_fanout_submitted_total").increment(1);
                    }
                    res
                }
            })
            .await
        })
    };

    let mut e2e_hist = Histogram::<u64>::new_with_bounds(1_000, HISTOGRAM_HIGH_NS, 3)?;
    let mut pickup_hist = Histogram::<u64>::new_with_bounds(1_000, HISTOGRAM_HIGH_NS, 3)?;
    let mut makespan_hist = Histogram::<u64>::new_with_bounds(1_000, HISTOGRAM_HIGH_NS, 3)?;
    let pickup_times: Vec<AtomicU64> = (0..args.workflows).map(|_| AtomicU64::new(0)).collect();
    let mut completed = 0usize;
    let mut stale_completions = 0usize;
    let mut samples: Vec<(Duration, usize)> = Vec::new();

    let collect_deadline = Instant::now()
        + Duration::from_secs(120 + (args.workflows as u64 * children as u64 / 200));
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
                "fanout: collection deadline reached"
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
                e2e_hist.record(complete_ns.saturating_sub(submit_ns)).ok();
                let pickup_ns = pickup_times[idx].load(Ordering::Relaxed);
                if pickup_ns != 0 && complete_ns >= pickup_ns {
                    makespan_hist.record(complete_ns - pickup_ns).ok();
                }
                metrics::counter!("sayiir_bench_fanout_completed_total").increment(1);
                completed += 1;
            }
            _ = sample_tick.tick() => {
                samples.push((bench_start.elapsed(), completed));
            }
            () = tokio::time::sleep(remaining) => {
                tracing::warn!(completed, "fanout: completion recv timed out");
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
    let sustained = best_window(&samples, Duration::from_secs(60));
    let wakeup_drops = wakeup_drops_total().saturating_sub(wakeup_drops_baseline);

    let steps_per_workflow = 1 + children + 1; // pickup + branches + join
    let mut latency = BTreeMap::new();
    latency.insert("e2e".to_string(), LatencyBlock::from_histogram_ns(&e2e_hist));
    latency.insert(
        "pickup".to_string(),
        LatencyBlock::from_histogram_ns(&pickup_hist),
    );
    latency.insert(
        "makespan".to_string(),
        LatencyBlock::from_histogram_ns(&makespan_hist),
    );

    tracing::info!(
        completed,
        expected = args.workflows,
        elapsed_s = total_elapsed.as_secs_f64(),
        sustained,
        children,
        steps_per_workflow,
        "fanout summary"
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
        "children": children,
        "concurrency": args.concurrency,
        "workers": args.workers,
        "poll_ms": args.poll_ms,
        "batch_size": args.batch_size,
    });
    let report = crate::report::build_report(
        "fanout",
        params_json,
        completed,
        args.workflows,
        stale_completions,
        0,
        total_elapsed,
        sustained,
        steps_per_workflow,
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
            "incomplete run: completed {} of {} workflows",
            completed,
            args.workflows
        );
    }
    Ok(())
}

async fn drain_burst(
    client: &Arc<WorkflowClient<PostgresBackend<JsonCodec>>>,
    workflow: &Arc<sayiir_core::workflow::Workflow<JsonCodec, State, ()>>,
    total: usize,
    concurrency: usize,
    completion_rx: &mut tokio::sync::mpsc::UnboundedReceiver<crate::metrics::Completion>,
    pickup_rx: &mut tokio::sync::mpsc::UnboundedReceiver<crate::metrics::Pickup>,
    deadline: Duration,
) -> Result<()> {
    let submitter = {
        let client = Arc::clone(client);
        let workflow = Arc::clone(workflow);
        tokio::spawn(async move {
            submit_bounded(total, concurrency, None, move |i| {
                let client = Arc::clone(&client);
                let workflow = Arc::clone(&workflow);
                async move {
                    client
                        .submit(
                            workflow.as_ref(),
                            format!("fo-warmup-{i}"),
                            State { id: i },
                        )
                        .await
                        .map(|_| ())
                        .map_err(anyhow::Error::from)
                }
            })
            .await
        })
    };
    let drain_deadline = Instant::now() + deadline;
    let mut drained = 0usize;
    while drained < total {
        let remaining = drain_deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            anyhow::bail!("fanout warmup: drained {drained} of {total} before deadline");
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
                anyhow::bail!("fanout warmup: timed out, drained {drained} of {total}");
            }
        }
    }
    let _ = submitter.await;
    Ok(())
}

async fn build_pool(url: &str, concurrency: usize, workers: usize) -> Result<sqlx::PgPool> {
    let target = (concurrency + workers * 4 + 16) as u32;
    PgPoolOptions::new()
        .max_connections(target)
        .acquire_timeout(Duration::from_secs(60))
        .connect(url)
        .await
        .with_context(|| format!("connecting to postgres at {url}"))
}

fn build_workflow(children: usize) -> sayiir_core::workflow::Workflow<JsonCodec, State, ()> {
    let ctx = WorkflowContext::new("bench-fanout", Arc::new(JsonCodec), Arc::new(()));
    let mut fork = WorkflowBuilder::new(ctx)
        .then("pickup", |s: State| async move {
            record_pickup(s.id);
            Ok(s)
        })
        .fork();
    for i in 0..children {
        let id = format!("branch_{i}");
        fork = fork.branch(&id, |s: State| async move { Ok(s) });
    }
    fork.join(
        "final_emit",
        |outputs: BranchOutputs<JsonCodec>| async move {
            // Pull the id off branch_0 — all branches share it. Avoids a
            // hard dependency on iteration order for the join hash.
            let s: State = outputs.get_by_id("branch_0").unwrap_or(State { id: 0 });
            record_completion(s.id);
            Ok(s)
        },
    )
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
        let worker = PooledWorker::new(format!("fo-worker-{i}"), worker_backend, registry)
            .with_claim_ttl(Some(Duration::from_secs(120)))
            .with_batch_size(batch_size);
        let entries = vec![(def_hash.clone(), Arc::clone(&workflow))];
        handles.push(worker.spawn(poll, entries));
    }
    handles
}

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
