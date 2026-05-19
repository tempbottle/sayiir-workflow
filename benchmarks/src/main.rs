//! Sayiir performance benchmark driver.
//!
//! See `docs/plans/2026-05-17-performance-benchmark-design.md` for the full design.

#![deny(clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]

mod driver;
mod metrics;
mod report;
mod workloads;

use std::time::Duration;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};

const DEFAULT_POSTGRES_URL: &str = "postgresql://postgres:postgres@127.0.0.1:5432/sayiir_bench";
const DEFAULT_PROMETHEUS_URL: &str = "http://127.0.0.1:9090";

#[derive(Parser, Debug)]
#[command(name = "sayiir-bench", version, about)]
struct Cli {
    /// Postgres connection URL.
    #[arg(long, env = "SAYIIR_BENCH_POSTGRES_URL", default_value = DEFAULT_POSTGRES_URL)]
    postgres_url: String,

    /// Prometheus base URL used for the end-of-run snapshot.
    #[arg(long, env = "SAYIIR_BENCH_PROMETHEUS_URL", default_value = DEFAULT_PROMETHEUS_URL)]
    prometheus_url: String,

    /// Bind address for the driver's Prometheus metrics scrape endpoint.
    #[arg(long, env = "SAYIIR_BENCH_METRICS_ADDR", default_value = "0.0.0.0:9464")]
    metrics_addr: String,

    /// Directory where JSON result files are written.
    #[arg(long, default_value = "results")]
    results_dir: String,

    /// Skip truncating Sayiir tables before the run. Default: truncate.
    #[arg(long)]
    no_reset_db: bool,

    #[command(subcommand)]
    scenario: Scenario,
}

#[derive(Subcommand, Debug)]
enum Scenario {
    /// Short-workflow throughput burst.
    Throughput(ThroughputArgs),
    /// Long-timer durable parking + wake storm.
    SleepingGiants(SleepingGiantsArgs),
}

#[derive(Parser, Debug)]
struct ThroughputArgs {
    /// Total number of workflows to submit.
    #[arg(long, default_value_t = 100_000)]
    workflows: usize,

    /// Number of concurrent submission tasks.
    #[arg(long, default_value_t = 256)]
    concurrency: usize,

    /// Optional submission rate cap (workflows/sec). Default: unbounded.
    #[arg(long)]
    target_rate: Option<u64>,

    /// Number of in-process Sayiir worker actors.
    #[arg(long, default_value_t = 8)]
    workers: usize,

    /// Worker poll interval (milliseconds).
    #[arg(long, default_value_t = 5)]
    poll_ms: u64,

    /// Tasks fetched per worker poll (batch size).
    #[arg(long, default_value_t = 64)]
    batch_size: usize,
}

#[derive(Parser, Debug)]
struct SleepingGiantsArgs {
    /// Total number of workflows to submit.
    #[arg(long, default_value_t = 500_000)]
    workflows: usize,

    /// Submission concurrency.
    #[arg(long, default_value_t = 512)]
    concurrency: usize,

    /// Sleep duration per workflow (seconds).
    #[arg(long, default_value_t = 60)]
    sleep_secs: u64,

    /// Number of in-process Sayiir worker actors.
    #[arg(long, default_value_t = 16)]
    workers: usize,

    /// Worker poll interval (milliseconds).
    #[arg(long, default_value_t = 50)]
    poll_ms: u64,

    /// Tasks fetched per worker poll (batch size).
    #[arg(long, default_value_t = 128)]
    batch_size: usize,

    /// SIGKILL the runtime mid-sleep then restart it (durability demo).
    #[arg(long)]
    demo_restart: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();

    let cli = Cli::parse();
    std::fs::create_dir_all(&cli.results_dir)
        .with_context(|| format!("creating results directory {}", cli.results_dir))?;

    metrics::install_prometheus_exporter(&cli.metrics_addr)
        .context("installing prometheus exporter")?;

    let common = CommonContext {
        postgres_url: cli.postgres_url,
        prometheus_url: cli.prometheus_url,
        results_dir: cli.results_dir,
        reset_db: !cli.no_reset_db,
    };

    let result = match cli.scenario {
        Scenario::Throughput(args) => workloads::throughput::run(common, args).await,
        Scenario::SleepingGiants(args) => workloads::sleeping::run(common, args).await,
    };

    sayiir_runtime::trace_context::shutdown_tracing();
    result
}

#[derive(Clone)]
pub struct CommonContext {
    pub postgres_url: String,
    pub prometheus_url: String,
    pub results_dir: String,
    pub reset_db: bool,
}

impl CommonContext {
    pub fn poll_interval(ms: u64) -> Duration {
        Duration::from_millis(ms)
    }
}

/// Truncate Sayiir tables so each run starts with a clean slate.
pub async fn reset_sayiir_tables(pool: &sqlx::PgPool) -> anyhow::Result<()> {
    let tables = sayiir_postgres::WORKFLOW_CHILD_TABLES
        .iter()
        .copied()
        .chain(std::iter::once("sayiir_workflow_snapshots"))
        .collect::<Vec<_>>()
        .join(", ");
    sqlx::query(&format!("TRUNCATE TABLE {tables} RESTART IDENTITY"))
        .execute(pool)
        .await
        .map(|_| ())
        .map_err(anyhow::Error::from)
}

/// If `OTEL_EXPORTER_OTLP_ENDPOINT` is set, ship runtime spans there via Sayiir's
/// OTLP-aware init. Otherwise fall back to a plain stderr fmt subscriber.
fn init_tracing() {
    sayiir_runtime::trace_context::init_tracing("sayiir-bench");
}
