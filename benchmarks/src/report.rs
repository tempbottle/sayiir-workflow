//! JSON results writer + (best-effort) Prometheus end-of-run snapshot.
//!
//! Every scenario run emits one JSON file under `results/` with everything
//! needed to reproduce the numbers and chart them later. The driver-side
//! hdrhistogram is the authoritative latency source; Prometheus queries
//! are best-effort enrichment (DB size, postgres tps).
//!
//! Latency blocks are *named*: scenarios surface one or more histograms
//! (e.g. `e2e`, `pickup`, `wake`) so the report can split scheduler
//! latency from end-to-end without ambiguity. Competitors publish these
//! separately — Temporal headlines `StartWorkflow` independently of
//! `WorkflowEndToEnd`, and DBOS reports per-step latency. We follow that
//! convention so numbers are directly diffable.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use hdrhistogram::Histogram;
use serde::{Deserialize, Serialize};
use sqlx::Row;

#[derive(Serialize, Deserialize)]
pub struct Report {
    pub scenario: String,
    pub timestamp_utc: String,
    pub git_sha: Option<String>,
    pub sayiir_version: Option<String>,
    pub hardware: HardwareInfo,
    pub postgres: PostgresInfo,
    pub params: serde_json::Value,
    pub results: ResultsBlock,
    pub samples: Vec<Sample>,
    pub prometheus: Option<PrometheusSnapshot>,
}

#[derive(Serialize, Deserialize)]
pub struct HardwareInfo {
    pub os: String,
    pub arch: String,
    pub cores: usize,
}

#[derive(Serialize, Deserialize)]
pub struct PostgresInfo {
    pub version: Option<String>,
    pub synchronous_commit: Option<String>,
    pub shared_buffers: Option<String>,
    pub max_connections: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub struct ResultsBlock {
    pub completed: usize,
    pub expected: usize,
    pub stale_completions: usize,
    pub excluded_warmup: usize,
    pub elapsed_s: f64,
    pub throughput_wf_per_sec_average: f64,
    pub throughput_wf_per_sec_sustained: f64,
    /// `wf_per_sec_sustained × steps_per_workflow`. Competitors (Temporal
    /// state-transitions/sec, Restate actions/sec) report this number;
    /// surface it so our charts diff cleanly against theirs.
    pub state_transitions_per_sec: f64,
    /// Number of state transitions counted in one workflow execution.
    /// Set by the scenario — `linear --steps N` uses `N`, fanout uses
    /// `1 (parent) + K (branches) + 1 (join)`, etc.
    pub steps_per_workflow: usize,
    /// Named latency blocks. Keys are scenario-defined (e.g. `"e2e"`,
    /// `"pickup"`, `"wake"`). A `BTreeMap` for stable JSON ordering.
    pub latency_ms: BTreeMap<String, LatencyBlock>,
    /// Number of wakeup hints dropped by sayiir-postgres during the run
    /// because its in-memory channel was full. Non-zero means the
    /// fallback poll loop was carrying load — a useful signal that the
    /// channel cap or worker count needs tuning.
    pub wakeup_drops: Option<u64>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct LatencyBlock {
    pub p50: f64,
    pub p95: f64,
    pub p99: f64,
    pub p99_9: f64,
    pub max: f64,
    /// Number of samples in the underlying histogram. Useful for
    /// downstream tools that want to weight averages.
    pub count: u64,
}

impl LatencyBlock {
    /// Convert an hdrhistogram of nanoseconds into a millisecond-bucketed
    /// summary. Empty histograms collapse to all-zero blocks rather than
    /// failing — a clean smoke run with no samples in a given block
    /// shouldn't make the whole report unparseable.
    #[must_use]
    pub fn from_histogram_ns(h: &Histogram<u64>) -> Self {
        let to_ms = |v: u64| Duration::from_nanos(v).as_secs_f64() * 1000.0;
        Self {
            p50: to_ms(h.value_at_quantile(0.50)),
            p95: to_ms(h.value_at_quantile(0.95)),
            p99: to_ms(h.value_at_quantile(0.99)),
            p99_9: to_ms(h.value_at_quantile(0.999)),
            max: to_ms(h.max()),
            count: h.len(),
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct Sample {
    pub t_ms: u64,
    pub completed: usize,
}

#[derive(Serialize, Deserialize)]
pub struct PrometheusSnapshot {
    pub pg_db_size_mb: Option<f64>,
    pub pg_xact_commit_total: Option<f64>,
    pub pg_xact_rollback_total: Option<f64>,
    pub pg_numbackends_peak: Option<f64>,
    pub container_pg_rss_peak_mb: Option<f64>,
}

#[allow(clippy::too_many_arguments)]
pub fn build_report(
    scenario: &str,
    params: serde_json::Value,
    completed: usize,
    expected: usize,
    stale_completions: usize,
    excluded_warmup: usize,
    elapsed: Duration,
    sustained: f64,
    steps_per_workflow: usize,
    latency_ms: BTreeMap<String, LatencyBlock>,
    samples: Vec<Sample>,
    wakeup_drops: Option<u64>,
    postgres: PostgresInfo,
    prometheus: Option<PrometheusSnapshot>,
) -> Report {
    let throughput_avg = completed as f64 / elapsed.as_secs_f64().max(1e-9);
    let state_transitions_per_sec = sustained * steps_per_workflow as f64;
    Report {
        scenario: scenario.to_string(),
        timestamp_utc: Utc::now().to_rfc3339(),
        git_sha: std::env::var("SAYIIR_BENCH_GIT_SHA").ok(),
        sayiir_version: std::env::var("SAYIIR_BENCH_SAYIIR_VERSION").ok(),
        hardware: HardwareInfo {
            os: std::env::consts::OS.to_string(),
            arch: std::env::consts::ARCH.to_string(),
            cores: std::thread::available_parallelism()
                .map(std::num::NonZeroUsize::get)
                .unwrap_or(0),
        },
        postgres,
        params,
        results: ResultsBlock {
            completed,
            expected,
            stale_completions,
            excluded_warmup,
            elapsed_s: elapsed.as_secs_f64(),
            throughput_wf_per_sec_average: throughput_avg,
            throughput_wf_per_sec_sustained: sustained,
            state_transitions_per_sec,
            steps_per_workflow,
            latency_ms,
            wakeup_drops,
        },
        samples,
        prometheus,
    }
}

pub fn write_report(report: &Report, results_dir: &str) -> Result<PathBuf> {
    let ts: DateTime<Utc> = report.timestamp_utc.parse().unwrap_or_else(|_| Utc::now());
    let fname = format!("{}-{}.json", report.scenario, ts.format("%Y%m%dT%H%M%SZ"));
    let path = PathBuf::from(results_dir).join(fname);
    let json = serde_json::to_string_pretty(report).context("serialising report")?;
    std::fs::write(&path, json).with_context(|| format!("writing {}", path.display()))?;
    tracing::info!(file = %path.display(), "wrote benchmark results");
    Ok(path)
}

/// Best-effort: query a few PG-state SHOWs + a version() for the report.
pub async fn collect_postgres_info(pool: &sqlx::PgPool) -> PostgresInfo {
    async fn show(pool: &sqlx::PgPool, name: &str) -> Option<String> {
        sqlx::query(&format!("SHOW {name}"))
            .fetch_one(pool)
            .await
            .ok()
            .and_then(|r| r.try_get::<String, _>(0).ok())
    }
    let version = sqlx::query("SELECT version()")
        .fetch_one(pool)
        .await
        .ok()
        .and_then(|r| r.try_get::<String, _>(0).ok());

    PostgresInfo {
        version,
        synchronous_commit: show(pool, "synchronous_commit").await,
        shared_buffers: show(pool, "shared_buffers").await,
        max_connections: show(pool, "max_connections").await,
    }
}

/// Best-effort: query Prometheus for a handful of named series at end-of-run.
///
/// Returns `None` if Prometheus is unreachable — the report is still written
/// without this enrichment.
pub async fn prometheus_snapshot(base_url: &str) -> Option<PrometheusSnapshot> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .ok()?;

    async fn scalar(client: &reqwest::Client, base: &str, query: &str) -> Option<f64> {
        let url = format!("{base}/api/v1/query");
        let resp = client
            .get(&url)
            .query(&[("query", query)])
            .send()
            .await
            .ok()?
            .json::<serde_json::Value>()
            .await
            .ok()?;
        let arr = resp
            .get("data")?
            .get("result")?
            .as_array()?
            .first()?
            .get("value")?
            .as_array()?;
        arr.get(1)?.as_str()?.parse::<f64>().ok()
    }

    Some(PrometheusSnapshot {
        pg_db_size_mb: scalar(
            &client,
            base_url,
            "pg_database_size_bytes{datname=\"sayiir_bench\"} / (1024*1024)",
        )
        .await,
        pg_xact_commit_total: scalar(
            &client,
            base_url,
            "pg_stat_database_xact_commit{datname=\"sayiir_bench\"}",
        )
        .await,
        pg_xact_rollback_total: scalar(
            &client,
            base_url,
            "pg_stat_database_xact_rollback{datname=\"sayiir_bench\"}",
        )
        .await,
        pg_numbackends_peak: scalar(
            &client,
            base_url,
            "max_over_time(pg_stat_database_numbackends{datname=\"sayiir_bench\"}[10m])",
        )
        .await,
        container_pg_rss_peak_mb: scalar(
            &client,
            base_url,
            "max_over_time(container_memory_working_set_bytes{name=\"sayiir-bench-postgres\"}[10m]) / (1024*1024)",
        )
        .await,
    })
}
