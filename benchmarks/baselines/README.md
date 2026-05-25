# Benchmark baselines

Each `<scenario>.json` in this directory is a committed reference run that
`sayiir-bench compare --scenario <scenario>` gates against. CI runs the
same scenarios on every PR and compares the fresh result back to the
baseline; a >10% throughput drop or >25% latency increase fails the build.

## Creating a baseline

```bash
# 1. Bring up the bench Postgres + observability stack.
cd benchmarks
docker compose up -d

# 2. Run the scenario you want to baseline.
cargo run --release -p sayiir-bench -- linear \
    --workflows 10000 --warmup-workflows 500 --steps 4

# 3. Promote the freshly-written report to the baselines directory.
cp results/linear-*.json baselines/linear.json
git add baselines/linear.json
```

Repeat for each scenario you want gated:

| Scenario | Suggested command |
|---|---|
| `linear` (steps=1) | `cargo run --release -p sayiir-bench -- linear --workflows 10000 --steps 1` |
| `linear` (steps=4) | `cargo run --release -p sayiir-bench -- linear --workflows 10000 --steps 4` |
| `linear` (steps=9) | `cargo run --release -p sayiir-bench -- linear --workflows 5000  --steps 9` |
| `fanout` (K=10)    | `cargo run --release -p sayiir-bench -- fanout --workflows 2000  --children 10` |
| `fanout` (K=100)   | `cargo run --release -p sayiir-bench -- fanout --workflows 500   --children 100` |
| `signal-driven`    | `cargo run --release -p sayiir-bench -- signal-driven --workflows 5000` |

If you need multiple baselines per scenario (e.g. different step counts),
name them `linear-1.json`, `linear-4.json`, `linear-9.json` and pass
`--baseline benchmarks/baselines/linear-9.json` to `compare`.

## What's gated

`compare` checks `throughput_wf_per_sec_sustained`, `e2e p50/p99`,
`pickup p50/p99`, plus per-scenario blocks (`makespan` for fanout,
`signal_resume` for signal-driven, `wake` for sleeping-giants). Anything
else (`state_transitions_per_sec`, `wakeup_drops`) is informational —
shown in the report but doesn't fail the build.

## Refreshing baselines

Baselines drift when hardware changes or after a deliberate perf
improvement lands. After landing perf work, regenerate the affected
baseline in the *same PR* so the gate doesn't trip on the next
unrelated change.
