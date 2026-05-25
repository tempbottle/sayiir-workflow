//! Compare a fresh result against a committed baseline.
//!
//! Baselines live at `benchmarks/baselines/<scenario>.json` — a
//! committed copy of a known-good run that downstream consumers (CI,
//! release reviews) gate on. `compare` checks the candidate's
//! sustained throughput and e2e p99 against the baseline and exits
//! non-zero if either regresses by more than `--throughput-pct` /
//! `--latency-pct` (defaults: 10% throughput drop, 25% latency
//! increase). Both numbers are intentionally permissive — single-run
//! noise on a developer laptop is ±5–10% even at steady state, and we
//! want the gate to catch real regressions, not flag every CI build.
//!
//! The candidate is auto-selected as the most-recent `results/`
//! file matching `<scenario>-*.json`, or specified via `--candidate
//! path`. The baseline is `benchmarks/baselines/<scenario>.json` or
//! `--baseline path`.

use std::path::PathBuf;

use anyhow::{Context, Result};
use serde::Serialize;

use crate::CompareArgs;
use crate::report::Report;

#[derive(Debug, Serialize)]
struct ComparisonRow {
    metric: &'static str,
    baseline: f64,
    candidate: f64,
    delta_pct: f64,
    /// `Some(true)` if this row crossed its regression threshold.
    /// `Some(false)` if the row is OK. `None` for informational rows
    /// where there's no threshold (e.g. `completed`).
    regressed: Option<bool>,
}

pub fn run(args: CompareArgs) -> Result<()> {
    let baseline_path = args
        .baseline
        .map(PathBuf::from)
        .unwrap_or_else(|| default_baseline_path(&args.scenario));
    let candidate_path = match args.candidate.as_ref() {
        Some(p) => PathBuf::from(p),
        None => latest_in_dir(&PathBuf::from(&args.results_dir), &args.scenario)?,
    };

    let baseline: Report = read_report(&baseline_path)?;
    let candidate: Report = read_report(&candidate_path)?;

    if baseline.scenario != candidate.scenario {
        anyhow::bail!(
            "scenario mismatch: baseline={} candidate={}",
            baseline.scenario,
            candidate.scenario,
        );
    }

    let mut rows: Vec<ComparisonRow> = Vec::new();
    let throughput_thresh_pct = args.throughput_pct;
    let latency_thresh_pct = args.latency_pct;

    let b_sustained = baseline.results.throughput_wf_per_sec_sustained;
    let c_sustained = candidate.results.throughput_wf_per_sec_sustained;
    let throughput_delta_pct = pct_change(b_sustained, c_sustained);
    rows.push(ComparisonRow {
        metric: "sustained_wf_per_sec",
        baseline: b_sustained,
        candidate: c_sustained,
        delta_pct: throughput_delta_pct,
        // A *negative* delta means candidate is slower — regression.
        regressed: Some(throughput_delta_pct < -throughput_thresh_pct),
    });

    for name in ["e2e", "pickup", "execution", "makespan", "signal_resume", "wake"] {
        let Some(b) = baseline.results.latency_ms.get(name) else {
            continue;
        };
        let Some(c) = candidate.results.latency_ms.get(name) else {
            continue;
        };
        let metric_p50 = Box::leak(format!("{name}_p50_ms").into_boxed_str());
        let metric_p99 = Box::leak(format!("{name}_p99_ms").into_boxed_str());
        let p50_delta = pct_change(b.p50, c.p50);
        let p99_delta = pct_change(b.p99, c.p99);
        rows.push(ComparisonRow {
            metric: metric_p50,
            baseline: b.p50,
            candidate: c.p50,
            delta_pct: p50_delta,
            regressed: Some(p50_delta > latency_thresh_pct),
        });
        rows.push(ComparisonRow {
            metric: metric_p99,
            baseline: b.p99,
            candidate: c.p99,
            delta_pct: p99_delta,
            regressed: Some(p99_delta > latency_thresh_pct),
        });
    }

    rows.push(ComparisonRow {
        metric: "state_transitions_per_sec",
        baseline: baseline.results.state_transitions_per_sec,
        candidate: candidate.results.state_transitions_per_sec,
        delta_pct: pct_change(
            baseline.results.state_transitions_per_sec,
            candidate.results.state_transitions_per_sec,
        ),
        regressed: None,
    });
    rows.push(ComparisonRow {
        metric: "wakeup_drops",
        baseline: baseline.results.wakeup_drops.unwrap_or(0) as f64,
        candidate: candidate.results.wakeup_drops.unwrap_or(0) as f64,
        delta_pct: 0.0,
        regressed: None,
    });

    print_table(&baseline, &candidate, &rows);

    let any_regressed = rows.iter().any(|r| r.regressed == Some(true));
    if any_regressed && !args.report_only {
        anyhow::bail!(
            "regression detected vs baseline {} (threshold: throughput {:.0}%, latency {:.0}%)",
            baseline_path.display(),
            throughput_thresh_pct,
            latency_thresh_pct,
        );
    }
    if any_regressed {
        tracing::warn!(
            "regression detected, but --report-only set: exiting 0 with diagnostics above"
        );
    }
    Ok(())
}

/// `(candidate - baseline) / baseline * 100`. Zero baselines collapse
/// to 0% so we don't blow up the JSON output on noisy/empty buckets.
fn pct_change(baseline: f64, candidate: f64) -> f64 {
    if baseline.abs() < f64::EPSILON {
        return 0.0;
    }
    (candidate - baseline) / baseline * 100.0
}

fn default_baseline_path(scenario: &str) -> PathBuf {
    PathBuf::from("benchmarks/baselines").join(format!("{scenario}.json"))
}

fn read_report(path: &std::path::Path) -> Result<Report> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("reading report {}", path.display()))?;
    serde_json::from_str(&raw).with_context(|| format!("parsing report {}", path.display()))
}

fn latest_in_dir(dir: &std::path::Path, scenario: &str) -> Result<PathBuf> {
    let entries = std::fs::read_dir(dir).with_context(|| format!("reading {}", dir.display()))?;
    let mut newest: Option<(std::time::SystemTime, PathBuf)> = None;
    for entry in entries.flatten() {
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if !name.ends_with(".json") || !name.starts_with(scenario) {
            continue;
        }
        let mtime = entry
            .metadata()
            .and_then(|m| m.modified())
            .unwrap_or(std::time::UNIX_EPOCH);
        match &newest {
            Some((t, _)) if *t >= mtime => {}
            _ => newest = Some((mtime, path.clone())),
        }
    }
    newest
        .map(|(_, p)| p)
        .ok_or_else(|| anyhow::anyhow!("no candidate found for scenario {scenario} in {}", dir.display()))
}

fn print_table(baseline: &Report, candidate: &Report, rows: &[ComparisonRow]) {
    println!(
        "\nscenario: {}    baseline: {}    candidate: {}",
        baseline.scenario,
        baseline.timestamp_utc,
        candidate.timestamp_utc,
    );
    println!(
        "{:<32}  {:>14}  {:>14}  {:>10}  {}",
        "metric", "baseline", "candidate", "delta %", "status"
    );
    println!("{}", "-".repeat(90));
    for r in rows {
        let status = match r.regressed {
            Some(true) => "REGRESSED",
            Some(false) => "ok",
            None => "info",
        };
        println!(
            "{:<32}  {:>14.3}  {:>14.3}  {:>9.2}%  {}",
            r.metric, r.baseline, r.candidate, r.delta_pct, status,
        );
    }
}
