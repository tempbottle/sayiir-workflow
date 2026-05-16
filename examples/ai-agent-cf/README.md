# ai-agent-cf — Durable AI research agent on Cloudflare Workers

End-to-end example of an AI agent built as a Sayiir workflow on Cloudflare
Workers. Demonstrates:

- **Fork/join** — parallel searches across Hacker News + Wikipedia
- **Loop** — iterative refinement with Workers AI (Llama 3.1)
- **`waitForSignal`** — human-in-the-loop approval, durable for up to 24h
- **D1 persistence** — workflow snapshots *and* drafts/reports share one DB
- **Cron resumeAll** — automatic recovery from Worker eviction + delay expiry

```
parseQuery
  │
  ▼
fork ─── searchHackerNews
      └─ searchWikipedia
  │
  ▼
mergeSources
  │
  ▼
loop (≤3 iterations) — refineAnswer (Workers AI)
  │
  ▼
saveDraft  ──►  waitForSignal("approval", 24h)  ──►  publish
```

## Prerequisites

- Node ≥ 22 (required by Wrangler 4), [pnpm](https://pnpm.io)
- Cloudflare account (free tier works) + `wrangler` ≥ 4
- Rust + `wasm-pack` (only if you want to rebuild the WASM core; the
  workspace build does this for you)

## Setup

From the monorepo root:

```bash
pnpm install
pnpm --filter @sayiir/cloudflare run build   # builds WASM + TS
```

Then from this directory:

```bash
# Create a D1 database for snapshots + agent storage
wrangler d1 create sayiir-agent
```

Paste the printed `database_id` into `wrangler.jsonc`. Then apply the schema
(Sayiir creates its own snapshot tables on first `Engine.create`; this
migration only adds the example's `agent_drafts` / `agent_reports` tables):

```bash
# from examples/ai-agent-cf
pnpm db:migrate:local      # for `wrangler dev`
# or
pnpm db:migrate:remote     # for deployed
```

## Run

```bash
pnpm dev
```

`wrangler dev` exposes the Worker at `http://localhost:8787`. Workers AI calls
hit the real Cloudflare inference API (not local). Without an account, swap
the `refineAnswer` task for a stub.

### Walk through a run

```bash
# 1. Start a research run
curl -sX POST localhost:8787/agents \
  -H 'content-type: application/json' \
  -d '{ "topic": "WebAssembly garbage collection" }' | jq

# → { "instanceId": "agent-…", "status": { "status": "awaiting_signal", "signalName": "approval", … } }
#   The fork, merge, refinement loop and draft save all happened before the
#   response returned. The workflow is now parked waiting for approval.

# 2. Inspect the draft that was saved
wrangler d1 execute sayiir-agent --local --command \
  "SELECT topic, substr(body,1,200) as preview FROM agent_drafts;"

# 3. Approve (optionally with edits)
curl -sX POST localhost:8787/agents/agent-…/approve \
  -H 'content-type: application/json' \
  -d '{ "approvedBy": "yacine", "edits": "(final edited body)" }' | jq

# → { "status": { "status": "completed", "output": { "instanceId": "…", "topic": "…", "body": "…" } } }

# 4. Final report
wrangler d1 execute sayiir-agent --local --command \
  "SELECT topic, approved_by FROM agent_reports;"
```

### HTTP API

| Method | Path                        | Body                                       | Notes                                                  |
|-------:|-----------------------------|--------------------------------------------|--------------------------------------------------------|
| `POST` | `/agents`                   | `{ topic, maxResultsPerSource? }`          | Start a new run. Returns `instanceId` and initial status. |
| `GET`  | `/agents/:id`               | —                                          | Poll status (idempotent `resume`).                     |
| `POST` | `/agents/:id/approve`       | `{ approvedBy?, edits? }`                  | Deliver the `approval` signal and resume.              |
| `POST` | `/agents/:id/cancel`        | `{ reason?, cancelledBy? }`                | Cancel a workflow.                                     |

### Cron

The `triggers.crons` entry runs every minute and calls `engine.resumeAll()`,
which sweeps up:

- Workflows whose `delay_wake_at` has passed
- Workflows stuck `in_progress` (Worker evicted mid-task)

You don't need to do anything special on eviction — the next cron tick picks
up the workflow from its last checkpoint. The task that was in flight at
eviction re-runs once; everything before it stays skipped.

## Deploy

```bash
# from examples/ai-agent-cf
pnpm db:migrate:remote
pnpm deploy
```

## How it works

- `src/workflow.ts` defines the tasks and the flow. Tasks read the D1 + AI
  bindings (and the current instance id) from a module-scoped `agentContext`
  that the Worker handler sets per request via `withAgentContext()`. The
  workflow values themselves stay pure JSON — Sayiir serializes them into D1.
- `src/index.ts` wires the HTTP routes. Each handler creates the `Engine`,
  sets the context, and invokes `engine.run` / `engine.resume` /
  `engine.sendSignal` / `engine.cancel`.
- The draft is persisted to `agent_drafts` *before* `waitForSignal`, then
  re-read by `publish` after the approval comes in. Threading values past a
  signal would require carrying them in the workflow state, which works but
  costs a snapshot round-trip; persisting them in your own table is the
  idiomatic pattern for arbitrary-size payloads.

## Tweak it

- Drop `refineAnswer` and you have a one-shot synthesis agent.
- Add a third search branch (GitHub trending, arXiv) — just put another
  `branch("...", ...)` inside `.fork([...])`.
- Replace `waitForSignal` with a second `loop` that polls an external review
  system for an approval verdict.
- Swap `@cf/meta/llama-3.1-8b-instruct` for any model in the
  [Workers AI catalog](https://developers.cloudflare.com/workers-ai/models/).
