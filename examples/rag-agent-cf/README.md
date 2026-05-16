# rag-agent-cf — Durable RAG assistant on Cloudflare Workers

Agentic RAG with citation verification and human-in-the-loop fallback,
end-to-end durable on Cloudflare's AI stack: **Workers AI + Vectorize +
R2 + D1**.

```
ingest workflow (POST /docs):
  fetch ─► extract ─► chunk ─► saveDoc ─► embed ─► upsert
                                            │
                                            └── one Workers AI call per chunk batch
                                                (Vectorize.upsert + D1 inserts at end)

query workflow (POST /ask):
  parseQuery
    │
    ▼
  fork ─┬─ vectorBranch  (Vectorize topK=8)
        ├─ keywordBranch (D1 FTS5 topK=5)
        └─ recentBranch  (D1: last 24h)
    │
    ▼
  mergeContext  (Reciprocal Rank Fusion, top 10)
    │
    ▼
  loop refineAnswer  (≤3 iterations)
        │   draft ─► verify each [n] citation against R2 ─► done | refine
    │
    ▼
  saveDraftCheckpoint  ──►  waitForSignal('human_clarify', 5min)  ──►  incorporateAndSave
                            ┌── confidence ≥ 0.6: handler auto-sends {skip:true}
                            ├── confidence <  0.6: park; client POSTs clarify
                            └── 5-min timeout:    auto-resume, low_confidence flag
```

## Demonstrates

- **fork/join** — hybrid retrieval over Vectorize + D1 FTS5 + recent docs
  in parallel, merged with RRF.
- **loop** — citation verification: re-fetches each cited chunk from R2
  and asks the LLM whether the source actually supports the claim. Drops
  unsupported citations and refines.
- **waitForSignal with timeout** — low-confidence pause for human
  clarification, with a 5-minute timeout that falls back to "answer
  anyway, flagged low_confidence". The human is an optimization, not a
  hard dependency.
- **D1 checkpointing** — every task result is persisted before the next
  task starts. Worker eviction mid-ingest doesn't lose finished chunks.
- **cron resumeAll** — recovers stuck ingest workflows once per minute.
  (Ask workflows recover via HTTP polling instead — see "How it works
  → Cron" below.)

## Prerequisites

- Node ≥ 22 (Wrangler 4 requirement), [pnpm](https://pnpm.io)
- Cloudflare account (free tier works) + `wrangler` ≥ 4
- Rust + `wasm-pack` (only if you want to rebuild the WASM core; the
  workspace build does this for you)

## Setup (4 commands)

From the monorepo root, install + build the workspace:

```bash
pnpm install
pnpm --filter @sayiir/cloudflare run build   # builds WASM + TS
```

Then from this directory, provision the three Cloudflare resources:

```bash
# from examples/rag-agent-cf
wrangler d1 create rag-agent
wrangler vectorize create rag-chunks --dimensions=768 --metric=cosine
wrangler r2 bucket create rag-raw-docs
```

Paste the printed D1 `database_id` into `wrangler.jsonc`, then apply the
example schema (Sayiir's own snapshot tables are created automatically on
first `Engine.create()`):

```bash
# from examples/rag-agent-cf
pnpm db:migrate:local      # for `wrangler dev`
# or
pnpm db:migrate:remote     # for deployed
```

## Run

```bash
# from examples/rag-agent-cf
pnpm dev
```

`wrangler dev` exposes the Worker at `http://localhost:8787`. Workers AI
calls hit the real Cloudflare inference API even in local dev.

On the first `POST /ask` after a fresh setup, three short "about Sayiir"
docs are seeded so the agent has something to chat about. The seed is
gated on `SELECT COUNT(*) FROM docs == 0` so it runs exactly once.

## Walk through it

```bash
# 1. Ask a question the seed docs cover — high-confidence inline answer.
curl -sX POST localhost:8787/ask \
  -H 'content-type: application/json' \
  -d '{"question":"What is Sayiir?"}' | jq

# → 200 { instanceId: "ask-…", status: { status: "completed",
#                                       output: { answer: "…",
#                                                 citations: [...],
#                                                 confidence: 0.83 } } }

# 2. Ask something the index doesn't cover — parks for clarification.
curl -sX POST localhost:8787/ask \
  -H 'content-type: application/json' \
  -d '{"question":"What is the airspeed velocity of an unladen swallow?"}' | jq

# → 202 { instanceId: "ask-…", clarifyUrl: "/ask/ask-…/clarify",
#         status: { status: "awaiting_signal", signalName: "human_clarify", wakeAt: "+5min" } }

# 3a. Deliver a clarification before the 5-minute timeout.
curl -sX POST "localhost:8787${clarifyUrl}" \
  -H 'content-type: application/json' \
  -d '{"clarification":"I meant African swallow, see Monty Python"}' | jq

# 3b. Or do nothing — the cron tick or the next GET /ask/:id picks up the
#     timeout and returns the best-effort answer with low_confidence: true.
curl -s "localhost:8787/ask/ask-…" | jq

# 4. Ingest your own URL.
curl -sX POST localhost:8787/docs \
  -H 'content-type: application/json' \
  -d '{"url":"https://en.wikipedia.org/wiki/WebAssembly"}' | jq

# → 200 (small docs) or 202 (parked, large docs)
#   { instanceId: "ingest-…", status: { status: "completed", output: { docId, url, chunkCount } } }

# 5. Inspect what was retrieved.
curl -s "localhost:8787/docs/$(jq -r .status.output.docId <<<"$RESP")" | jq
```

## HTTP API

| Method  | Path                          | Body                            | Notes                                                            |
|--------:|-------------------------------|---------------------------------|------------------------------------------------------------------|
|  `POST` | `/docs`                       | `{ url }`                       | Start ingest. `200` if completed inline, `202` if parked.        |
|   `GET` | `/docs`                       | —                               | List ingested docs (most recent first, paginated to 100).        |
|   `GET` | `/docs/:id`                   | —                               | One doc + its chunks (no embeddings).                            |
|`DELETE` | `/docs/:id`                   | —                               | Cascade-delete D1 (FK ON DELETE CASCADE) + Vectorize + R2.       |
|  `POST` | `/ask`                        | `{ question, conversationId? }` | Start ask. Inline answer on happy path, `202` + clarifyUrl on low confidence. |
|   `GET` | `/ask/:instanceId`            | —                               | Poll (idempotent `engine.resume`). Picks up timeouts.            |
|  `POST` | `/ask/:instanceId/clarify`    | `{ clarification }`             | Deliver `human_clarify` signal + resume. Returns the final answer. |

## How it works

- **`src/workflow-ingest.ts`** — defines `ingestDoc` with linear tasks
  for fetch/extract/chunk/saveDoc/embed/upsert. The flow factory closes
  over a `RagContext`, so tasks read their D1 / AI / Vectorize / R2
  bindings directly without any module-scoped globals.
- **`src/workflow-ask.ts`** — defines `askQuestion` with `parseQuery →
  fork(3) → mergeContext → loop(refine + verify) → saveDraftCheckpoint →
  waitForSignal → incorporateAndSave`. Same closure-over-context pattern.
- **`src/index.ts`** — wires the HTTP routes. Each handler builds a fresh
  workflow with a per-request context (carrying the workflow's
  `instanceId`), then calls `engine.run` / `engine.resume` /
  `engine.sendSignal` as appropriate.

### Why always-on `waitForSignal`?

`route().branch()` only accepts a single task per branch, and
`waitForSignal` is a flow-level construct, not a task — so a conditional
"park or not" path can't be expressed inside a route. The flow always
includes the signal; the HTTP handler in `index.ts` reads the saved
draft's confidence after the workflow parks, and:

- **`confidence ≥ 0.6`** → handler immediately sends `{skip: true}` via
  `engine.sendSignal` and resumes. The high-confidence path still
  completes in a single HTTP round-trip.
- **`confidence < 0.6`** → handler returns `202` with a `clarifyUrl`. The
  client (or a 5-minute timeout) brings the workflow forward.

State across the signal is bridged via D1: the draft message is keyed by
`${instanceId}:assistant`. `incorporateAndSave` reads it back, applies
the clarification or skip, and writes the final message.

### Cron

The cron trigger runs `engine.resumeAll(ingestDoc)` every minute. It does
**not** sweep ask workflows. The reason: `resumeAll` reuses a single
workflow definition across all resumed instances, but the ask workflow's
tasks read `ctx.instanceId` for their D1 lookups — a shared context would
key them all to the same row. Ask workflows park only at `waitForSignal`,
which is reached by `GET /ask/:id` (idempotent resume that picks up
timeouts) or `POST /ask/:id/clarify` (delivers the signal). If a client
asks a low-confidence question and never polls, the workflow stays parked
indefinitely — that's the expected behavior for this example, since the
human-in-the-loop is the whole point of the pause.

Want true durable timeouts on ask workflows? See "Tweak it" below.

## Tweak it

- **Per-chunk fork on ingest** — split `embed` into N static fork branches
  for finer resume granularity. Trade-off: requires fixing the chunk
  count at build time, so good for batch ingest jobs with known sizes.
- **Browser Rendering** — for JS-heavy sites, swap `fetch(url, ...)` in
  `workflow-ingest.ts:fetch` for `env.BROWSER.fetch(url, ...)` and add
  the `browser` binding to `wrangler.jsonc`. Requires the paid plan.
- **Reranker** — drop a `@cf/baai/bge-reranker` call into
  `mergeContext` to post-rank the top-10 by cross-encoder score.
- **Cron-sweep ask workflows** — write a small manual loop in
  `scheduled` that queries `sayiir_workflow_snapshots` directly for ask
  instances with `position_kind = 'AtSignal'` and an expired `wake_at`,
  then builds a per-instance `RagContext` and calls `engine.resume` for
  each. Depends on the snapshot table layout (currently internal API).
- **Streaming responses** — the durable flow can't stream because it may
  park. Add a separate non-durable endpoint that runs `refineAnswer`
  inline and streams the LLM response via `ReadableStream`.
- **AI Gateway** — point `env.AI` at an AI Gateway endpoint to get
  caching, observability, and per-model rate limits with zero code
  changes.

## Storage map

- **D1** — Sayiir's snapshot tables (auto-created on first
  `Engine.create()`) plus this example's schema (`docs`, `chunks`,
  `chunks_fts` FTS5 virtual table, `conversations`, `messages`). All
  in `migrations/0001_rag_schema.sql`.
- **Vectorize** — `rag-chunks`, 768-dim cosine. Chunk ids are
  `${docId}:${ordinal}`. Metadata includes `doc_id` and `ordinal` for
  filtering.
- **R2** — `rag-raw-docs`. Keys are doc UUIDs; values are the raw fetched
  bytes (or UTF-8 text for seed docs). Range-GET serves citation
  verification.
