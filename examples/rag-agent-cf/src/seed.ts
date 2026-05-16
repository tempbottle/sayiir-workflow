/**
 * First-run seeder.
 *
 * Inserts three short "about Sayiir" docs so a fresh deploy has something
 * to chat about before the user ingests their own URLs. The seed bypasses
 * the ingest workflow because seed docs aren't fetchable URLs — they go
 * straight into D1, Vectorize, and R2 in the same shape `upsert` would
 * produce.
 *
 * Gated by `SELECT COUNT(*) FROM docs == 0` so seeding only happens once.
 */

import {
  EMBEDDING_DIMS,
  EMBEDDING_MODEL,
  type RagContext,
} from "./context.js";

interface SeedDoc {
  url: string;
  title: string;
  text: string;
}

const SEED_DOCS: SeedDoc[] = [
  {
    url: "sayiir://docs/overview",
    title: "What is Sayiir?",
    text: `Sayiir is a durable workflow engine for orchestrating long-running, fault-tolerant tasks across distributed systems. The core engine is written in Rust and exposes idiomatic bindings for Python (via PyO3) and Node.js (via NAPI-RS). A separate package, @sayiir/cloudflare, compiles the same core to WebAssembly so workflows can run inside Cloudflare Workers.

Sayiir takes a checkpoint-and-exit approach to durability: every task result is persisted to a backend (PostgreSQL, SQLite, or Cloudflare D1) before the next task starts, so a process crash or worker eviction never loses more than the in-flight task. Workflows park on delays and external signals without holding any worker resources, then resume on cron sweeps or explicit signal delivery.

The execution model has three primitives that compose: linear task chains for sequential work, fork/join for parallel fan-out, and loops with self-critique for iterative refinement. Workflows can also wait for external events with optional timeouts, enabling human-in-the-loop steps and long-lived integrations.`,
  },
  {
    url: "sayiir://docs/cloudflare-workers",
    title: "Sayiir on Cloudflare Workers",
    text: `Sayiir on Cloudflare Workers solves the eviction problem inherent to ephemeral runtimes. Workers can be killed mid-request when they exceed CPU budget or when the runtime decides to recycle the isolate; without durable state, any orchestration logic running across multiple async steps loses work on eviction.

The @sayiir/cloudflare package uses Cloudflare D1 as the snapshot backend. Each task result is written to a SQLite-compatible database before the next task starts. A cron trigger sweeps for stuck or parked workflows once per minute and calls engine.resumeAll, which finds instances whose delays have expired or which were evicted mid-task and resumes them from the last checkpoint.

The fork primitive runs branches in parallel via Promise.all internally; each branch's result is checkpointed independently so a partial fork can resume without re-running completed branches. The waitForSignal primitive parks the workflow with an optional timeout — useful for human approval steps that may take minutes or days. When the timeout fires, the workflow advances with an empty signal payload, letting the next task choose a default behavior.`,
  },
  {
    url: "sayiir://docs/rag-patterns",
    title: "RAG patterns this example demonstrates",
    text: `Three reliability patterns map directly onto Sayiir's primitives for production RAG: hybrid retrieval, citation verification, and human-in-the-loop fallback.

Hybrid retrieval uses parallel fork branches to query multiple indexes at once — vector similarity via Vectorize, full-text matching via D1's FTS5 virtual tables, and a recency bias from D1's docs table. The branches return independent ranked lists, which are merged with Reciprocal Rank Fusion. Two retrieval modalities cover two failure modes: vectors miss exact-token matches like model names and error codes; keyword search misses paraphrased intent.

Citation verification is the loop's body. After the LLM drafts an answer with [n] inline citations, the verifier range-reads each cited chunk from R2 using the byte offsets recorded at ingest time and asks the LLM whether the chunk actually supports the cited claim. Unsupported citations are dropped and their claims rewritten in the next iteration. The loop exits when confidence crosses a threshold or after a maximum of three iterations.

The waitForSignal primitive with a five-minute timeout creates a graceful fallback: if confidence stays below 0.6, the workflow parks for a human to clarify the question, and on timeout it auto-resumes and saves the best-effort answer flagged as low confidence. The human is an optimization, not a hard dependency.`,
  },
];

export async function maybeSeed(ctx: RagContext): Promise<number> {
  const countRow = await ctx.db
    .prepare("SELECT COUNT(*) AS n FROM docs")
    .first<{ n: number }>();
  if (countRow && countRow.n > 0) return 0;

  let seeded = 0;
  for (const seed of SEED_DOCS) {
    await seedOne(ctx, seed);
    seeded += 1;
  }
  return seeded;
}

async function seedOne(ctx: RagContext, seed: SeedDoc): Promise<void> {
  const docId = crypto.randomUUID();
  const rawR2Key = docId;
  const encoder = new TextEncoder();
  const bytes = encoder.encode(seed.text);

  await ctx.raw.put(rawR2Key, bytes, {
    httpMetadata: { contentType: "text/plain; charset=utf-8" },
  });

  await ctx.db
    .prepare(
      `INSERT INTO docs (id, url, title, content_type, raw_r2_key)
       VALUES (?, ?, ?, 'text/plain', ?)`,
    )
    .bind(docId, seed.url, seed.title, rawR2Key)
    .run();

  // Each seed doc fits comfortably in a single chunk. Splitting them
  // would only add latency without exercising any retrieval feature.
  const chunkText = seed.text;
  const chunkId = `${docId}:0`;
  const byteStart = 0;
  const byteEnd = chunkText.length;

  // Embed the chunk.
  const result = (await ctx.ai.run(EMBEDDING_MODEL, {
    text: [chunkText],
  })) as { data: number[][] };
  const embedding = result.data?.[0];
  if (!embedding || embedding.length !== EMBEDDING_DIMS) {
    throw new Error(
      `Seed embedding failed for ${seed.url}: got ${embedding?.length ?? 0} dims`,
    );
  }

  // Vectorize upsert + D1 chunks insert.
  await ctx.vec.upsert([
    {
      id: chunkId,
      values: embedding,
      metadata: { doc_id: docId, ordinal: 0 },
    },
  ]);
  await ctx.db
    .prepare(
      `INSERT INTO chunks (id, doc_id, ordinal, text, byte_start, byte_end)
       VALUES (?, ?, 0, ?, ?, ?)`,
    )
    .bind(chunkId, docId, chunkText, byteStart, byteEnd)
    .run();
}
