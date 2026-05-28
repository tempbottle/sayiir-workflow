//! Sweep driver: run a scenario N times varying one axis.
//!
//! Competitors publish *charts*, not point measurements. A sweep loops
//! the same scenario over a range of one parameter (workers,
//! concurrency, steps, …) and emits a per-point JSON file plus a
//! summary table the user can paste into a PR or README. Default axis
//! is `workers` because scaling efficiency is the most-asked question
//! ("does Sayiir actually use all my cores?").
//!
//! This module deliberately shells out to the bench binary itself for
//! each point rather than running the scenario in-process. Reasons:
//!
//! 1. Each point gets a fresh process — no leaked tokio runtimes, no
//!    drifting metrics, no `OnceLock`-already-set panics across runs.
//! 2. The bench binary IS the contract. If a user can run a single
//!    point manually, they can reproduce a sweep point identically.
//! 3. Parallelism is bounded by the OS, not by our internal scheduler
//!    — easier to reason about resource isolation.

use std::ffi::OsString;
use std::fs;
use std::path::PathBuf;
use std::process::Stdio;

use anyhow::{Context, Result};
use serde::Deserialize;
use tokio::process::Command;

use crate::SweepArgs;

#[derive(Debug, Deserialize)]
struct SweepRow {
    axis_value: String,
    sustained: f64,
    state_transitions_per_sec: f64,
    e2e_p50_ms: f64,
    e2e_p99_ms: f64,
    pickup_p50_ms: f64,
    pickup_p99_ms: f64,
    wakeup_drops: u64,
    completed: usize,
    expected: usize,
    report_path: String,
}

pub async fn run(common: crate::CommonContext, args: SweepArgs) -> Result<()> {
    let values: Vec<String> = args
        .values
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .collect();
    if values.is_empty() {
        anyhow::bail!("--values must list at least one comma-separated value");
    }
    tracing::info!(scenario = %args.scenario, axis = %args.axis, values = ?values, "starting sweep");

    let exe = std::env::current_exe().context("locating bench binary path")?;
    let results_dir = PathBuf::from(&common.results_dir);
    fs::create_dir_all(&results_dir)
        .with_context(|| format!("creating results dir {}", results_dir.display()))?;

    let mut rows: Vec<SweepRow> = Vec::with_capacity(values.len());

    for value in &values {
        let extra_args = sweep_overrides(&args.axis, value, &args.extra_args)?;
        let mut cmd = Command::new(&exe);
        cmd.arg("--postgres-url")
            .arg(&common.postgres_url)
            .arg("--prometheus-url")
            .arg(&common.prometheus_url)
            .arg("--results-dir")
            .arg(&common.results_dir);
        if !common.reset_db {
            cmd.arg("--no-reset-db");
        }
        cmd.arg(&args.scenario);
        for a in &extra_args {
            cmd.arg(a);
        }
        cmd.stdout(Stdio::inherit()).stderr(Stdio::inherit());

        tracing::info!(value, "sweep point: starting");
        let status = cmd
            .status()
            .await
            .with_context(|| format!("invoking bench for value={value}"))?;
        if !status.success() {
            tracing::warn!(value, ?status, "sweep point failed; continuing");
            continue;
        }

        // Match by mtime: the most recently written JSON in results_dir
        // is the one we just produced. `prefix` matches `linear`,
        // `fanout`, etc. — sleeping-giants is the only one with a dash,
        // and clap maps the subcommand to the scenario name in the
        // report. We accept any prefix match to stay forward-compat.
        let latest = latest_report(&results_dir, &args.scenario)?;
        let parsed = parse_summary(&latest, value)?;
        rows.push(parsed);
    }

    print_table(&args, &rows);
    let md = render_markdown(&args, &rows);
    let summary_path = results_dir.join(format!(
        "sweep-{}-{}.md",
        args.scenario,
        chrono::Utc::now().format("%Y%m%dT%H%M%SZ"),
    ));
    fs::write(&summary_path, &md).with_context(|| format!("writing {}", summary_path.display()))?;
    tracing::info!(file = %summary_path.display(), "sweep summary written");
    println!("\n{md}");
    Ok(())
}

/// Build the CLI overrides for one sweep point.
///
/// Each axis maps to a known flag. `extra_args` is the user-supplied
/// pass-through so they can pin e.g. `--workflows 10000` and only sweep
/// over workers.
fn sweep_overrides(axis: &str, value: &str, extra: &[String]) -> Result<Vec<OsString>> {
    let flag = match axis {
        "workers" => "--workers",
        "concurrency" => "--concurrency",
        "batch_size" | "batch-size" => "--batch-size",
        "poll_ms" | "poll-ms" => "--poll-ms",
        "steps" => "--steps",
        "children" => "--children",
        other => anyhow::bail!("unknown sweep axis: {other}"),
    };
    let mut out: Vec<OsString> = Vec::with_capacity(extra.len() + 2);
    // Strip any pre-existing copy of the axis flag from extra_args so
    // the sweep value wins when the user accidentally double-specifies.
    let mut skip_next = false;
    for a in extra {
        if skip_next {
            skip_next = false;
            continue;
        }
        if a == flag {
            skip_next = true;
            continue;
        }
        if a.starts_with(&format!("{flag}=")) {
            continue;
        }
        out.push(a.into());
    }
    out.push(flag.into());
    out.push(value.into());
    Ok(out)
}

fn latest_report(dir: &std::path::Path, scenario: &str) -> Result<PathBuf> {
    let prefix = scenario;
    let entries = fs::read_dir(dir).with_context(|| format!("reading {}", dir.display()))?;
    let mut newest: Option<(std::time::SystemTime, PathBuf)> = None;
    for entry in entries.flatten() {
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if !name.ends_with(".json") {
            continue;
        }
        if !name.starts_with(prefix) {
            continue;
        }
        let mtime = entry.metadata().and_then(|m| m.modified()).ok();
        let mtime = mtime.unwrap_or(std::time::UNIX_EPOCH);
        match &newest {
            Some((t, _)) if *t >= mtime => {}
            _ => newest = Some((mtime, path.clone())),
        }
    }
    newest
        .map(|(_, p)| p)
        .ok_or_else(|| anyhow::anyhow!("no report file found with prefix {prefix}"))
}

fn parse_summary(path: &std::path::Path, axis_value: &str) -> Result<SweepRow> {
    let raw = fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let report: crate::report::Report =
        serde_json::from_str(&raw).with_context(|| format!("parsing {}", path.display()))?;
    let e2e = report.results.latency_ms.get("e2e").cloned();
    let pickup = report.results.latency_ms.get("pickup").cloned();
    Ok(SweepRow {
        axis_value: axis_value.to_string(),
        sustained: report.results.throughput_wf_per_sec_sustained,
        state_transitions_per_sec: report.results.state_transitions_per_sec,
        e2e_p50_ms: e2e.as_ref().map(|l| l.p50).unwrap_or(0.0),
        e2e_p99_ms: e2e.as_ref().map(|l| l.p99).unwrap_or(0.0),
        pickup_p50_ms: pickup.as_ref().map(|l| l.p50).unwrap_or(0.0),
        pickup_p99_ms: pickup.as_ref().map(|l| l.p99).unwrap_or(0.0),
        wakeup_drops: report.results.wakeup_drops.unwrap_or(0),
        completed: report.results.completed,
        expected: report.results.expected,
        report_path: path.display().to_string(),
    })
}

fn print_table(args: &SweepArgs, rows: &[SweepRow]) {
    tracing::info!(
        scenario = %args.scenario,
        axis = %args.axis,
        points = rows.len(),
        "sweep complete"
    );
}

fn render_markdown(args: &SweepArgs, rows: &[SweepRow]) -> String {
    let mut s = String::new();
    s.push_str(&format!(
        "# Sweep: {} (axis: {})\n\nGenerated {} UTC.\n\n",
        args.scenario,
        args.axis,
        chrono::Utc::now().format("%Y-%m-%d %H:%M:%S"),
    ));
    s.push_str(&format!(
        "| {} | wf/s sustained | transitions/s | e2e p50 ms | e2e p99 ms | pickup p50 ms | pickup p99 ms | wakeup drops | completed |\n",
        args.axis,
    ));
    s.push_str("|---|---|---|---|---|---|---|---|---|\n");
    for r in rows {
        s.push_str(&format!(
            "| {} | {:.1} | {:.1} | {:.1} | {:.1} | {:.2} | {:.2} | {} | {}/{} |\n",
            r.axis_value,
            r.sustained,
            r.state_transitions_per_sec,
            r.e2e_p50_ms,
            r.e2e_p99_ms,
            r.pickup_p50_ms,
            r.pickup_p99_ms,
            r.wakeup_drops,
            r.completed,
            r.expected,
        ));
    }
    s.push_str("\n## Reports\n\n");
    for r in rows {
        s.push_str(&format!("- {} → `{}`\n", r.axis_value, r.report_path));
    }
    s
}
