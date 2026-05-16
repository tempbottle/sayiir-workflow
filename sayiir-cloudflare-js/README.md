# @sayiir/cloudflare

**Durable workflows for Cloudflare Workers, powered by a Rust/WASM runtime and D1 persistence.**

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://github.com/sayiir/sayiir/blob/main/LICENSE)
[![Discord](https://img.shields.io/badge/Discord-Join-7289da)](https://discord.gg/A2jWBFZsNK)

Write plain TypeScript functions. Sayiir makes them durable — automatic checkpointing, crash recovery, and parallel execution inside Cloudflare Workers.

```typescript
import { task, flow, Engine } from "@sayiir/cloudflare";

const fetchUser = task("fetch-user", async (id: number) => {
  const res = await fetch(`https://api.example.com/users/${id}`);
  return res.json() as Promise<{ id: number; name: string }>;
});

const sendEmail = task("send-email", async (user: { id: number; name: string }) => {
  await fetch("https://api.example.com/email", {
    method: "POST",
    body: JSON.stringify({ to: user.name, subject: "Welcome!" }),
  });
  return `Sent welcome to ${user.name}`;
});

const onboarding = flow<number>("onboarding")
  .then(fetchUser)
  .then(sendEmail)
  .build();

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const engine = await Engine.create(env.DB);
    const status = await engine.run(onboarding, "onboard-42", 42);
    return Response.json(status);
  },
};
```

No DSL. No YAML. No determinism constraints. No external orchestrator.

## Why Sayiir on Workers?

- **Checkpoint-and-exit** — Workers are ephemeral. Sayiir checkpoints after every task to D1. On eviction, completed tasks are preserved; use `resumeAll()` in a cron handler to automatically recover interrupted instances.
- **No replay** — Unlike replay-based engines, Sayiir never re-executes completed tasks. Your tasks can call any API, use any library, generate random values. No restrictions.
- **WASM core** — All orchestration, checkpointing, and execution runs in compiled Rust/WASM. You write TypeScript; WASM handles the hard parts.
- **D1 persistence** — Snapshots are stored in Cloudflare D1 (SQLite at the edge). No external database needed.
- **Type-safe** — Generic `Flow<TInput, TLast>` builder tracks types through the entire chain.

## Installation

```bash
pnpm add @sayiir/cloudflare
```

## Setup

### D1 binding

Add a D1 database to your `wrangler.toml`:

```toml
[[d1_databases]]
binding = "DB"
database_name = "sayiir"
database_id = "<your-database-id>"
```

### Environment type

```typescript
interface Env {
  DB: D1Database;
}
```

## Quickstart

### Durable workflow with D1

```typescript
import { task, flow, Engine } from "@sayiir/cloudflare";

const chargeCard = task("charge-card", async (order: { amount: number }) => {
  const res = await fetch("https://payments.example.com/charge", {
    method: "POST",
    body: JSON.stringify(order),
  });
  return res.json() as Promise<{ transactionId: string }>;
}, { timeout: "30s" });

const sendReceipt = task("send-receipt", async (tx: { transactionId: string }) => {
  await fetch("https://email.example.com/send", {
    method: "POST",
    body: JSON.stringify({ template: "receipt", txId: tx.transactionId }),
  });
  return `Receipt sent for ${tx.transactionId}`;
});

const checkout = flow<{ amount: number }>("checkout")
  .then(chargeCard)
  .then(sendReceipt)
  .build();

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const engine = await Engine.create(env.DB);
    const status = await engine.run(checkout, "order-123", { amount: 99_99 });

    if (status.status === "completed") {
      return new Response(status.output);
    }
    return Response.json(status, { status: 202 });
  },
};
```

Sayiir checkpoints to D1 after each completed task. If the Worker is evicted (CPU limit, deployment, etc.) between checkpoints, the last checkpointed task is preserved. The task that was running at eviction time will re-execute on the next resume — but no completed task is ever re-executed.

Eviction recovery is **not automatic** — something must trigger `engine.resume()` again. Use `engine.resumeAll()` in a cron handler to sweep for interrupted instances (see [Recovering from eviction](#recovering-from-eviction) below).

### Handling parking (delays and signals)

Workflows can **park** at a `delay()` or `waitForSignal()` node. This is intentional — the engine saves the snapshot to D1 and returns a non-completed status with wake-up metadata. Your Worker returns a response, and a later trigger (cron, queue, or HTTP) resumes execution.

```typescript
import { task, flow, Engine } from "@sayiir/cloudflare";

const submitOrder = task("submit", async (orderId: string) => {
  return { orderId, submittedAt: new Date().toISOString() };
});

const notifyCustomer = task("notify", async (data: { orderId: string; approval: unknown }) => {
  return `Order ${data.orderId} approved`;
});

const orderApproval = flow<string>("order-approval")
  .then(submitOrder)
  .delay("cooling-period", "1h")
  .waitForSignal("approval", "manager_approval", { timeout: "48h" })
  .then(notifyCustomer)
  .build();

export default {
  // Start or resume a workflow
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    const engine = await Engine.create(env.DB);

    // POST /workflows/:id — start a new workflow
    if (request.method === "POST") {
      const instanceId = url.pathname.split("/").pop()!;
      const status = await engine.run(orderApproval, instanceId, instanceId);
      return Response.json(status, { status: statusCode(status) });
    }

    // PUT /workflows/:id/resume — resume after delay/signal
    if (request.method === "PUT" && url.pathname.endsWith("/resume")) {
      const instanceId = url.pathname.split("/").at(-2)!;
      const status = await engine.resume(orderApproval, instanceId);
      return Response.json(status, { status: statusCode(status) });
    }

    return new Response("Not found", { status: 404 });
  },

  // Cron: resume parked workflows + recover from eviction
  async scheduled(event: ScheduledEvent, env: Env): Promise<void> {
    const engine = await Engine.create(env.DB);
    await engine.resumeAll(orderApproval);
  },
};

function statusCode(status: { status: string }): number {
  return status.status === "completed" ? 200 : 202;
}
```

The execution model:

1. **First call** — runs `submitOrder`, parks at `delay("1h")`. Returns `{ status: "waiting", wakeAt: "..." }`.
2. **After 1 hour** — cron or queue triggers `engine.resume()`. Passes the delay, parks at `waitForSignal`. Returns `{ status: "awaiting_signal" }`.
3. **Signal arrives** — see next section.
4. **Final resume** — runs `notifyCustomer`. Returns `{ status: "completed", output: "..." }`.

### Signals — external events

Send a signal from any Worker (or even a different service) to unblock a waiting workflow:

```typescript
import { Engine } from "@sayiir/cloudflare";

// POST /signals — deliver an external signal
export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const { instanceId, signalName, payload } = await request.json<{
      instanceId: string;
      signalName: string;
      payload: unknown;
    }>();

    const engine = await Engine.create(env.DB);
    await engine.sendSignal(instanceId, signalName, payload);

    return new Response("Signal delivered", { status: 200 });
  },
};
```

After the signal is delivered, the next `engine.resume()` call picks it up and continues execution.

### Pause, unpause, and cancel

```typescript
const engine = await Engine.create(env.DB);

// Pause — workflow stops at the next task boundary
await engine.pause("order-123", { reason: "Fraud review", pausedBy: "admin" });

// Unpause — allows the next resume() to continue
await engine.unpause("order-123");
const status = await engine.resume(orderApproval, "order-123");

// Cancel — workflow stops permanently
await engine.cancel("order-123", { reason: "Customer request", cancelledBy: "user-7" });
```

### Conflict policy — `run()` vs `resume()`

`engine.run(workflow, instanceId, input)` starts a **new** workflow at
`instanceId`. It is *not* an idempotent alias for `resume()` — calling it
twice with the same id on a workflow that is parked (waiting on a delay or
signal) would otherwise overwrite the checkpoint with a fresh initial state
and silently discard all completed task results.

To prevent this, the engine defaults to `conflictPolicy: "fail"`. A duplicate
`run()` throws; the correct action for a known instance is `resume()`.

```ts
// Default — duplicate run() throws.
const engine = await Engine.create(env.DB);

// Idempotent retries (e.g. an "at-least-once" trigger that may fire twice).
const engine = await Engine.create(env.DB, { conflictPolicy: "use_existing" });

// Force a fresh start, discarding any prior snapshot for this id.
const engine = await Engine.create(env.DB, { conflictPolicy: "terminate_existing" });
```

| Policy                 | Existing snapshot for `instanceId` | Behaviour                                      |
|------------------------|------------------------------------|------------------------------------------------|
| `"fail"` (default)     | yes                                | Throws — caller should `resume()` instead.     |
| `"use_existing"`       | yes                                | Returns the current status; does not execute.  |
| `"terminate_existing"` | yes                                | Deletes snapshot + clears signals; starts new. |
| any                    | no                                 | Starts new.                                    |

In all cases a definition-hash mismatch against an existing snapshot is a
hard error — you can't restart a different workflow under the same id.

### Recovering from eviction

Workers can be evicted at any time (CPU limit, deployment, memory pressure). Sayiir checkpoints after each task, so completed work is never lost — but the interrupted instance sits in D1 as `in_progress` with no wake-up time. Nothing will automatically resume it.

`engine.resumeAll()` finds these instances and resumes them in a single pass:

```typescript
// In your scheduled handler — sweep for resumable instances
async scheduled(event: ScheduledEvent, env: Env): Promise<void> {
  const engine = await Engine.create(env.DB);

  // Resume both parked (expired delays/signals) and stuck (evicted) instances
  await engine.resumeAll(orderApproval);

  // Custom stale threshold (e.g. 60 seconds) and batch size
  await engine.resumeAll(orderApproval, { staleAfter: 60, limit: 20 });
}
```

`resumeAll` picks up three categories:

1. Parked instances whose `delay_wake_at` has passed — covers `AtDelay`, `AtSignal` with a timeout, and `AtFork` (when a branch parked at a delay).
2. Instances parked on `waitForSignal` (with or without a timeout) that have at least one buffered event waiting — covers fire-and-forget `sendSignal()`.
3. Actively-executing instances (positions `AtTask`, `AtJoin`, `InLoop`, `NotStarted`) with no update within `staleAfter` seconds (default: 300) — recovers from Worker eviction or CPU-limit kills.

The stale path explicitly **excludes** parked positions (`AtFork`, `AtSignal`, `AtDelay`). A workflow correctly waiting on an external signal won't get re-resumed every staleAfter window just because the snapshot hasn't been touched in a while; only buffered events (category 2) or an explicit `engine.resume()` will wake it.

The task that was running at eviction time re-executes; all previously checkpointed tasks are skipped.

**Signal latency.** `sendSignal()` writes the event but does not bump `updated_at`. The worst-case latency between `sendSignal()` and the workflow resuming is the cron interval, not `staleAfter`. If you cannot tolerate cron latency, call `engine.resume()` inline from your `sendSignal` handler — the cron path is then just a safety net for missed deliveries.

### Parallel execution (fork/join)

```typescript
import { task, flow, branch, Engine } from "@sayiir/cloudflare";

const checkInventory = task("check-inventory", async (order: { id: number }) => {
  return { stock: "available" };
});

const validatePayment = task("validate-payment", async (order: { id: number }) => {
  return { payment: "valid" };
});

const checkout = flow<{ id: number }>("checkout")
  .fork([
    branch("inventory", checkInventory),
    branch("payment", validatePayment),
  ])
  .join("finalize", ([inventory, payment]) => {
    return { ...inventory, ...payment };
  })
  .build();
```

Fork branches run sequentially in Workers (single-threaded), but each branch is checkpointed independently — if the Worker is evicted mid-fork, only uncompleted branches re-execute on resume.

### Loops

```typescript
import { task, flow, LoopResult, Engine } from "@sayiir/cloudflare";

const pollStatus = task("poll-status", async (jobId: string) => {
  const res = await fetch(`https://api.example.com/jobs/${jobId}`);
  const job = await res.json() as { status: string; result?: string };

  return job.status === "done"
    ? LoopResult.done(job.result!)
    : LoopResult.again(jobId);
});

const workflow = flow<string>("poll-until-done")
  .loop(pollStatus, { maxIterations: 20, onMax: "exit_with_last" })
  .build();
```

### Conditional branching

```typescript
import { task, flow, Engine } from "@sayiir/cloudflare";

const classify = task("classify", (ticket: { type: string }) => {
  return ticket.type === "billing" ? "billing" : "tech";
});

const handleBilling = task("handle-billing", (t: { type: string }) => "billing resolved");
const handleTech = task("handle-tech", (t: { type: string }) => "tech resolved");

const router = flow<{ type: string }>("support")
  .route((t) => t.type === "billing" ? "billing" : "tech", ["billing", "tech"] as const)
    .branch("billing", handleBilling)
    .branch("tech", handleTech)
  .done()
  .build();
```

## API Reference

### Engine

- **`Engine.create(db, opts?)`** — Create a durable engine backed by a D1 database. `opts.conflictPolicy` (default `"fail"`) controls what `run()` does when the instance id is reused — see [Conflict policy](#conflict-policy) below. Returns `Promise<Engine>`.
- **`engine.run(workflow, instanceId, input)`** — Run a workflow. Calling `run()` twice with the same `instanceId` is **not** the same as calling `resume()`. Under the default `"fail"` policy a duplicate `run()` throws; use `resume()` to continue a parked instance.
- **`engine.resume(workflow, instanceId)`** — Resume from the last checkpoint.
- **`engine.cancel(instanceId, opts?)`** — Cancel a running workflow.
- **`engine.pause(instanceId, opts?)`** — Pause at the next task boundary.
- **`engine.unpause(instanceId)`** — Unpause a paused workflow.
- **`engine.sendSignal(instanceId, signalName, payload)`** — Deliver an external signal.
- **`engine.resumeAll(workflow, opts?)`** — Resume instances whose delay/signal expired **and** instances stuck after Worker eviction (no update within `staleAfter` seconds, default: 300). Up to `limit` (default: 10, oldest first). Returns `Promise<WorkflowStatus<TOut>[]>`.

### Task Definition

- **`task(id, fn, opts?)`** — Create a named task. Optional: `timeout`, `retries`, `retry`, `tags`, `description`, `input`/`output` (Zod schemas).

### Flow Builder

- **`flow<TInput>(name)`** — Create a new type-safe flow builder.
- **`.then(fn)`** / **`.then(id, fn, opts?)`** — Append a task step.
- **`.loop(fn, opts?)`** — Add a loop. Body returns `LoopResult.again(value)` or `LoopResult.done(value)`.
- **`.fork(branches)`** — Start parallel branches.
- **`.join(id, fn)`** — Merge branches with a combining function.
- **`.delay(id, duration)`** — Durable delay (`"30s"`, `"5m"`, `"1h"`).
- **`.waitForSignal(id, signalName, opts?)`** — Wait for an external signal.
- **`.route(keyFn, keys)`** — Conditional branching.
- **`.build()`** — Compile to a `Workflow<TIn, TOut>`.

### Convenience

- **`runWorkflow(workflow, input)`** — Run without persistence (testing/prototyping). Returns `Promise<TOut>`.

### WorkflowStatus\<TOut\>

Discriminated union — use `status.status` with TypeScript narrowing:

| Status | Fields |
|--------|--------|
| `completed` | `output: TOut` |
| `in_progress` | — |
| `failed` | `error: string` |
| `cancelled` | `reason?: string`, `cancelledBy?: string` |
| `paused` | `reason?: string`, `pausedBy?: string` |
| `waiting` | `wakeAt: string`, `delayId: string` |
| `awaiting_signal` | `signalId: string`, `signalName: string`, `wakeAt?: string` |

### Loop Control

- **`LoopResult.again(value)`** — Continue iterating.
- **`LoopResult.done(value)`** — Exit the loop.

## Architecture

### Request lifecycle

A single HTTP request that drives a workflow either to completion or to a park point:

```
 HTTP request
      │
      ▼
┌──────────────────────────────────────────────────────────────────┐
│ Worker isolate                                                   │
│                                                                  │
│   ① cold-start only: wasm-init.ts → initSync(<bundled .wasm>)    │
│      (idempotent; folds into isolate spin-up, not first request) │
│                                                                  │
│   ② Engine.create(env.DB)                                        │
│      └─ D1Backend connects + runs MIGRATION_SQL (idempotent)     │
│                                                                  │
│   ③ engine.run(workflow, instanceId, input)                      │
│      │                                                           │
│      │  ┌──────────────────────────────────────────┐             │
│      │  │ prepare_run                              │             │
│      │  │   load snapshot for instanceId           │             │
│      │  │   ├─ exists & ConflictPolicy=Fail        │             │
│      │  │   │   → return error                     │             │
│      │  │   ├─ exists & UseExisting                │             │
│      │  │   │   → return its current status        │             │
│      │  │   ├─ exists & TerminateExisting          │             │
│      │  │   │   → delete + write fresh snapshot    │             │
│      │  │   └─ not exists                          │             │
│      │  │       → write initial snapshot           │             │
│      │  └──────────────────────────────────────────┘             │
│      │                                                           │
│      │  ┌──────────────────────────────────────────┐             │
│      │  │ execute loop, per task:                  │             │
│      │  │                                          │             │
│      │  │   bytes ─decode→ JS value                │             │
│      │  │             │                            │             │
│      │  │             ▼                            │             │
│      │  │      user task callback (TS)             │             │
│      │  │             │                            │             │
│      │  │   JS value ─encode→ bytes                │             │
│      │  │             │                            │             │
│      │  │             ▼                            │             │
│      │  │      SAVE_SNAPSHOT ────────────────► D1  │             │
│      │  │                                          │             │
│      │  └────┬───────────────────────────┬─────────┘             │
│      │       │                           │                       │
│      │       ▼ all tasks complete        ▼ hits delay / signal   │
│      │  finalize_execution            save park snapshot:        │
│      │  status=Completed              position_kind=AtDelay      │
│      │                                              |AtSignal    │
│      │                                              |AtFork      │
│      │                                delay_wake_at = …          │
│      │                                awaited_signal_name = …    │
│      │                                                           │
│      ▼                                                           │
│   ④ Response.json(status)   ◄── status carries park metadata     │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
      │                                       Worker may die here.
      ▼
 HTTP response (status: completed | awaiting_signal | waiting | …)
```

Each user task is its own checkpoint. A Worker eviction mid-task only loses the in-flight task; everything before it stays skipped on resume.

### How a parked workflow makes progress

Three paths converge on `engine.resume(workflow, instanceId)`. All three load the same snapshot and continue from the saved position; the differences are only in what triggers the call.

```
        ┌─────────────────────┐  GET /workflows/:id
        │ HTTP poll           │ ─────────────────────────┐
        └─────────────────────┘                          │
                                                         ▼
        ┌─────────────────────┐  POST /signal +    ┌──────────────────┐
        │ Signal delivery     │  inline resume     │ engine.resume    │
        │ engine.sendSignal() │ ─────────────────► │   load snapshot  │
        └─────────────────────┘                    │   resolve parked │
                                                   │   continue execute│
        ┌─────────────────────┐  scheduled() ───►  │   checkpoint     │
        │ Cron sweep          │                    └────────┬─────────┘
        │ engine.resumeAll()  │                             │
        └─────────────────────┘                             ▼
                                                  completed | parks again
```

`engine.resumeAll()` runs a single SQL pickup in `sayiir-d1` that returns three categories of instances:

| Category   | SQL condition                                                                    | Triggers on                                                       |
|------------|----------------------------------------------------------------------------------|-------------------------------------------------------------------|
| Ready      | `delay_wake_at <= now()`                                                         | Delay expired; timed signal hit timeout; fork's delayed branch fired. |
| Signalled  | `position_kind = 'AtSignal'` AND a buffered event row matches `awaited_signal_name` | Fire-and-forget `sendSignal()` where the caller didn't `resume()` inline. |
| Stale      | non-parked position (`AtTask | AtJoin | InLoop | NotStarted`) AND `updated_at <= now() - staleAfter` | Worker eviction mid-task — recovery path.                         |

The stale category explicitly excludes parked positions. A workflow correctly waiting on a signal never gets re-resumed by the cron sweep — only by a delivered event or an explicit `resume()`.

### Codec boundary — JS values across WASM

Sayiir's core operates on opaque `Bytes`. The codec in `sayiir-cloudflare/src/codec.rs` is the only thing that knows how to convert:

- **Encode** (JS → bytes): `JSON.stringify` with a replacer that intercepts `ArrayBuffer` / `Uint8Array` and emits a base64-tagged envelope `{"$sayiir_bin": "<b64>", "$sayiir_kind": "ArrayBuffer" | "Uint8Array"}`. Everything else stringifies natively.
- **Decode** (bytes → JS): substring fast-path — if the payload doesn't contain `"$sayiir_bin"`, plain `JSON.parse`. Otherwise `JSON.parse` with a reviver that rehydrates envelopes back to real binary types.

So `ArrayBuffer` / `Uint8Array` round-trip cleanly through task boundaries (~1.33× JSON overhead from base64), but the snapshot row size limit in D1 still applies — see *D1 snapshot size limit* in the [Cloudflare quick-start docs](https://docs.sayiir.dev/getting-started/cloudflare/#d1-snapshot-size-limit) before shipping large blobs through workflow state.

## Requirements

- Cloudflare Workers
- D1 database
- Optional: `zod` for input/output validation

## License

MIT

## Links

- [Documentation](https://docs.sayiir.dev)
- [Examples](https://github.com/sayiir/sayiir/tree/main/examples)
- [GitHub](https://github.com/sayiir/sayiir)
- [Discord](https://discord.gg/A2jWBFZsNK)
