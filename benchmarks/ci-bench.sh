#!/usr/bin/env bash
# Smoke-grade CI bench run: small workflow counts so the build pipeline
# finishes inside a few minutes, but enough volume per scenario to make
# the comparison against baselines/<scenario>.json meaningful.
#
# Designed to be called from .github/workflows or any other CI runner
# with a Postgres reachable at SAYIIR_BENCH_POSTGRES_URL (defaults to
# the docker-compose stack in benchmarks/).
#
# Exits non-zero if any `compare` call detects a regression vs baseline.
# Pass `--report-only` to surface diagnostics without gating the build.

set -euo pipefail

REPORT_ONLY=""
if [[ "${1:-}" == "--report-only" ]]; then
    REPORT_ONLY="--report-only"
fi

cd "$(dirname "$0")/.."
RESULTS=benchmarks/results
mkdir -p "$RESULTS"

BENCH="cargo run --release -q -p sayiir-bench --"

# Each scenario uses a small but non-trivial workflow count: enough to
# saturate the warmup + measurement phases without burning CI minutes.
echo "== linear (steps=4) =="
$BENCH --results-dir "$RESULTS" linear --workflows 2000 --warmup-workflows 200 --steps 4

echo "== fanout (children=10) =="
$BENCH --results-dir "$RESULTS" fanout --workflows 500 --warmup-workflows 50 --children 10

echo "== signal-driven =="
$BENCH --results-dir "$RESULTS" signal-driven --workflows 1000

# Compare against committed baselines. Each baseline is keyed by
# scenario name; non-default baselines (e.g. linear-9.json) are picked
# up by passing --baseline explicitly. CI doesn't gate scenarios that
# don't have a committed baseline — that's a one-time setup cost the
# first time a new scenario lands.
for scenario in linear fanout signal-driven; do
    baseline="benchmarks/baselines/${scenario}.json"
    if [[ ! -f "$baseline" ]]; then
        echo "skip compare: no baseline at $baseline"
        continue
    fi
    echo "== compare $scenario =="
    $BENCH compare --scenario "$scenario" --results-dir "$RESULTS" $REPORT_ONLY
done

echo "ci-bench: ok"
