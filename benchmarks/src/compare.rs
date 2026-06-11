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

use std::fmt::Write as _;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use comfy_table::{Cell, CellAlignment, ContentArrangement, Table, presets};
use serde::Serialize;

use crate::report::Report;
use crate::{CompareArgs, CompareFormat};

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

    let b_sustained = sustained_rate(&baseline);
    let c_sustained = sustained_rate(&candidate);
    let throughput_delta_pct = pct_change(b_sustained, c_sustained);
    rows.push(ComparisonRow {
        metric: "sustained_wf_per_sec",
        baseline: b_sustained,
        candidate: c_sustained,
        delta_pct: throughput_delta_pct,
        // A *negative* delta means candidate is slower — regression.
        regressed: Some(throughput_delta_pct < -throughput_thresh_pct),
    });

    for name in [
        "e2e",
        "pickup",
        "execution",
        "makespan",
        "signal_resume",
        "wake",
    ] {
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

    let any_regressed = rows.iter().any(|r| r.regressed == Some(true));

    let rendered = match args.format {
        CompareFormat::Text => render_text(&baseline, &candidate, &rows),
        CompareFormat::Markdown => render_markdown(
            &baseline,
            &candidate,
            &rows,
            any_regressed,
            throughput_thresh_pct,
            latency_thresh_pct,
        ),
    };
    print!("{rendered}");
    if let Some(path) = args.output.as_ref() {
        std::fs::write(path, &rendered).with_context(|| format!("writing {path}"))?;
    }

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

/// Window width of the workloads' sustained-throughput metric; the
/// recomputation below must match what `best_window_rate` callers use.
const SUSTAINED_WINDOW: Duration = Duration::from_mins(1);

/// Sustained throughput under the current full-window definition.
///
/// Recomputed from the report's completion samples rather than read from
/// `results`: the stored value reflects whatever definition the
/// *producing* harness had, and the `bench-baseline-main` artifact is
/// built by main's harness — an older main can carry the any-span
/// burst-peak variant, inflated 50–100% vs the full-window rate, which
/// manufactures regressions against an honest candidate. Recomputing
/// both sides keeps the gate apples-to-apples across harness versions.
///
/// Falls back to the stored value when samples are missing, and for
/// sleeping-giants, whose sustained metric (completions per *awake*
/// second) is intentionally not derivable from the completion samples.
fn sustained_rate(report: &Report) -> f64 {
    if report.scenario == "sleeping-giants" || report.samples.len() < 2 {
        return report.results.throughput_wf_per_sec_sustained;
    }
    let points: Vec<(Duration, usize)> = report
        .samples
        .iter()
        .map(|s| (Duration::from_millis(s.t_ms), s.completed))
        .collect();
    crate::report::best_window_rate(&points, SUSTAINED_WINDOW)
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
    newest.map(|(_, p)| p).ok_or_else(|| {
        anyhow::anyhow!(
            "no candidate found for scenario {scenario} in {}",
            dir.display()
        )
    })
}

fn render_text(baseline: &Report, candidate: &Report, rows: &[ComparisonRow]) -> String {
    let mut s = String::new();
    let _ = writeln!(
        s,
        "\nscenario: {}    baseline: {}    candidate: {}",
        baseline.scenario, baseline.timestamp_utc, candidate.timestamp_utc,
    );
    let _ = writeln!(
        s,
        "{:<32}  {:>14}  {:>14}  {:>10}  status",
        "metric", "baseline", "candidate", "delta %",
    );
    let _ = writeln!(s, "{}", "-".repeat(90));
    for r in rows {
        let status = match r.regressed {
            Some(true) => "REGRESSED",
            Some(false) => "ok",
            None => "info",
        };
        let _ = writeln!(
            s,
            "{:<32}  {:>14.3}  {:>14.3}  {:>9.2}%  {}",
            r.metric, r.baseline, r.candidate, r.delta_pct, status,
        );
    }
    s
}

/// Render a GitHub-flavored-markdown block suitable for posting to a
/// PR via `marocchino/sticky-pull-request-comment`. The first line is
/// a status header (`### ✅` / `### ⚠️`) so the comment immediately
/// reads at-a-glance; the table mirrors the text format with a
/// signed-delta column. Hidden HTML comment at the top lets the CI
/// workflow scope this body to one scenario when stitching multi-
/// scenario comments together.
fn render_markdown(
    baseline: &Report,
    candidate: &Report,
    rows: &[ComparisonRow],
    any_regressed: bool,
    throughput_pct: f64,
    latency_pct: f64,
) -> String {
    let mut s = String::new();
    let _ = writeln!(s, "<!-- sayiir-bench-compare:{} -->", candidate.scenario);
    let header_icon = if any_regressed { "⚠️" } else { "✅" };
    let header_text = if any_regressed {
        "regression detected"
    } else {
        "within thresholds"
    };
    let _ = writeln!(
        s,
        "### {} `{}` — {}",
        header_icon, candidate.scenario, header_text,
    );
    let _ = writeln!(s);
    let _ = writeln!(
        s,
        "Thresholds: throughput drop ≤ {throughput_pct:.0}%, latency rise ≤ {latency_pct:.0}%.",
    );
    let _ = writeln!(
        s,
        "Baseline: `{}` · Candidate: `{}`",
        baseline.timestamp_utc, candidate.timestamp_utc,
    );
    let _ = writeln!(s);

    // Body table — comfy-table with `ASCII_MARKDOWN` preset renders
    // GFM-compatible pipe tables, including the separator row. We set
    // per-column alignment (metric left, numerics right, status
    // center) — alignment that hand-rolled `writeln!` chains can't
    // easily encode without manual `:---:` math per row.
    let mut t = Table::new();
    t.load_preset(presets::ASCII_MARKDOWN)
        .set_content_arrangement(ContentArrangement::Disabled);
    t.set_header(vec![
        Cell::new("Metric").set_alignment(CellAlignment::Left),
        Cell::new("Baseline").set_alignment(CellAlignment::Right),
        Cell::new("Candidate").set_alignment(CellAlignment::Right),
        Cell::new("Δ").set_alignment(CellAlignment::Right),
        Cell::new("Status").set_alignment(CellAlignment::Center),
    ]);
    for r in rows {
        let status = match r.regressed {
            Some(true) => "🔴 regressed",
            Some(false) => "🟢 ok",
            None => "ℹ️ info",
        };
        // Sign-prefix the delta so reviewers can scan for the worst rows.
        let delta = if r.delta_pct.abs() < 0.005 {
            "0.00%".to_string()
        } else if r.delta_pct >= 0.0 {
            format!("+{:.2}%", r.delta_pct)
        } else {
            format!("{:.2}%", r.delta_pct)
        };
        t.add_row(vec![
            Cell::new(format!("`{}`", r.metric)).set_alignment(CellAlignment::Left),
            Cell::new(format!("{:.3}", r.baseline)).set_alignment(CellAlignment::Right),
            Cell::new(format!("{:.3}", r.candidate)).set_alignment(CellAlignment::Right),
            Cell::new(delta).set_alignment(CellAlignment::Right),
            Cell::new(status).set_alignment(CellAlignment::Center),
        ]);
    }
    let _ = writeln!(s, "{t}");
    let _ = writeln!(s);

    // Hardware footer is what makes this comparable across PRs — the
    // CI runner is the same image, but expressing it inline avoids
    // "wait, was the baseline on the M2 mac?" confusion.
    let _ = writeln!(
        s,
        "<sub>Hardware: {}/{} · {} cores · pg `{}`</sub>",
        candidate.hardware.os,
        candidate.hardware.arch,
        candidate.hardware.cores,
        candidate
            .postgres
            .version
            .as_deref()
            .map_or("unknown", |v| v.split_whitespace().nth(1).unwrap_or(v)),
    );
    s
}
