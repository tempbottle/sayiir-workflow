/**
 * HTTP + cron entry point for the durable RAG assistant.
 *
 *   POST   /docs                       { url }                       → ingest a URL
 *   GET    /docs                                                      → list docs
 *   GET    /docs/:id                                                  → one doc + its chunks
 *   DELETE /docs/:id                                                  → drop from D1+Vectorize+R2
 *   POST   /ask                        { question, conversationId? } → ask a question
 *   GET    /ask/:instanceId                                           → poll status
 *   POST   /ask/:instanceId/clarify    { clarification }              → resolve a low-confidence pause
 *
 * Cron `* * * * *` resumes parked ingest workflows only — see the README
 * for why ask workflows aren't part of the sweep.
 */

import { Engine, WorkflowError } from "@sayiir/cloudflare";

import type { RagContext } from "./context.js";
import { buildIngestWorkflow } from "./workflow-ingest.js";
import {
  buildAskWorkflow,
  type AskInput,
  type ClarifySignal,
} from "./workflow-ask.js";
import { maybeSeed } from "./seed.js";

interface Env {
  DB: D1Database;
  AI: Ai;
  VECTORIZE: VectorizeIndex;
  RAW: R2Bucket;
}

const CONFIDENCE_THRESHOLD = 0.6;

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    const segs = url.pathname.split("/").filter(Boolean);
    try {
      // /docs
      if (segs[0] === "docs" && segs.length === 1) {
        if (request.method === "POST") return await postDocs(request, env);
        if (request.method === "GET") return await listDocs(env);
        return methodNotAllowed();
      }
      // /docs/:id
      if (segs[0] === "docs" && segs.length === 2) {
        const id = decodeURIComponent(segs[1]!);
        if (request.method === "GET") return await getDoc(env, id);
        if (request.method === "DELETE") return await deleteDoc(env, id);
        return methodNotAllowed();
      }
      // /ask
      if (segs[0] === "ask" && segs.length === 1) {
        if (request.method !== "POST") return methodNotAllowed();
        return await postAsk(request, env);
      }
      // /ask/:instanceId, /ask/:instanceId/clarify
      if (segs[0] === "ask" && segs.length >= 2) {
        const instanceId = decodeURIComponent(segs[1]!);
        const action = segs[2];
        if (!action && request.method === "GET") {
          return await pollAsk(env, instanceId);
        }
        if (action === "clarify" && request.method === "POST") {
          return await postClarify(request, env, instanceId);
        }
        return methodNotAllowed();
      }
      return new Response("Not found", { status: 404 });
    } catch (err) {
      return errorResponse(err);
    }
  },

  async scheduled(_event: ScheduledController, env: Env): Promise<void> {
    const engine = await Engine.create(env.DB, { conflictPolicy: "use_existing" });
    // Cron only sweeps ingest workflows. Ask workflows park at waitForSignal;
    // they're brought forward by GET /ask/:id polls or POST /clarify. See
    // README "How it works → Cron" for the rationale.
    const ctx = makeCtx(env, "<cron-sweep>");
    const wf = buildIngestWorkflow(ctx);
    await engine.resumeAll(wf, { limit: 25 });
  },
} satisfies ExportedHandler<Env>;

// ─── /docs handlers ────────────────────────────────────────────────────────

async function postDocs(request: Request, env: Env): Promise<Response> {
  const body = (await request.json().catch(() => ({}))) as { url?: string };
  if (!body.url || typeof body.url !== "string") {
    return new Response("Body must be { url: string }", { status: 400 });
  }

  const instanceId = `ingest-${crypto.randomUUID()}`;
  const ctx = makeCtx(env, instanceId);
  const engine = await Engine.create(env.DB, { conflictPolicy: "use_existing" });
  const wf = buildIngestWorkflow(ctx);

  const status = await engine.run(wf, instanceId, { url: body.url });
  const httpStatus =
    status.status === "completed"
      ? 200
      : status.status === "failed"
        ? 500
        : 202;
  return Response.json({ instanceId, status }, { status: httpStatus });
}

async function listDocs(env: Env): Promise<Response> {
  const rows = await env.DB.prepare(
    `SELECT id, url, title, fetched_at AS fetchedAt, content_type AS contentType
       FROM docs
      ORDER BY fetched_at DESC
      LIMIT 100`,
  ).all<{
    id: string;
    url: string;
    title: string | null;
    fetchedAt: string;
    contentType: string;
  }>();
  return Response.json({ docs: rows.results ?? [] });
}

async function getDoc(env: Env, id: string): Promise<Response> {
  const doc = await env.DB.prepare(
    `SELECT id, url, title, fetched_at AS fetchedAt, content_type AS contentType
       FROM docs WHERE id = ?`,
  )
    .bind(id)
    .first<{
      id: string;
      url: string;
      title: string | null;
      fetchedAt: string;
      contentType: string;
    }>();
  if (!doc) return new Response("Doc not found", { status: 404 });

  const chunks = await env.DB.prepare(
    `SELECT id, ordinal, length(text) AS textLen, byte_start AS byteStart, byte_end AS byteEnd
       FROM chunks
      WHERE doc_id = ?
      ORDER BY ordinal`,
  )
    .bind(id)
    .all<{
      id: string;
      ordinal: number;
      textLen: number;
      byteStart: number;
      byteEnd: number;
    }>();
  return Response.json({ doc, chunks: chunks.results ?? [] });
}

async function deleteDoc(env: Env, id: string): Promise<Response> {
  const chunkRows = await env.DB.prepare(
    "SELECT id FROM chunks WHERE doc_id = ?",
  )
    .bind(id)
    .all<{ id: string }>();
  const chunkIds = (chunkRows.results ?? []).map((r) => r.id);

  if (chunkIds.length > 0) {
    await env.VECTORIZE.deleteByIds(chunkIds);
  }
  await env.RAW.delete(id); // r2 key is the doc id
  // chunks rows cascade via the FK in the migration.
  const result = await env.DB.prepare("DELETE FROM docs WHERE id = ?")
    .bind(id)
    .run();
  if (!result.meta?.changes) {
    return new Response("Doc not found", { status: 404 });
  }
  return new Response(null, { status: 204 });
}

// ─── /ask handlers ─────────────────────────────────────────────────────────

async function postAsk(request: Request, env: Env): Promise<Response> {
  const body = (await request.json().catch(() => ({}))) as AskInput;
  if (!body.question || typeof body.question !== "string") {
    return new Response("Body must be { question: string }", { status: 400 });
  }

  // First-run seed so a fresh deploy has something to chat about. Idempotent
  // (gated on docs.count == 0).
  await maybeSeed(makeCtx(env, "<seed>"));

  const instanceId = `ask-${crypto.randomUUID()}`;
  const ctx = makeCtx(env, instanceId);
  const engine = await Engine.create(env.DB, { conflictPolicy: "use_existing" });
  const wf = buildAskWorkflow(ctx);

  // Always parks at waitForSignal. After this returns we inspect the saved
  // draft's confidence and either auto-skip (high) or surface the clarify URL.
  await engine.run(wf, instanceId, body);

  const draft = await readDraft(env, instanceId);
  if (!draft) {
    return Response.json(
      { instanceId, error: "draft not found after run" },
      { status: 500 },
    );
  }

  if (draft.confidence >= CONFIDENCE_THRESHOLD) {
    // High confidence: auto-skip the signal so the workflow completes inline.
    await engine.sendSignal(instanceId, "human_clarify", { skip: true });
    const final = await engine.resume(wf, instanceId);
    return Response.json({ instanceId, status: final });
  }

  // Low confidence: park for the human. Client polls GET /ask/:id or POSTs
  // a clarification.
  const status = await engine.resume(wf, instanceId);
  return Response.json(
    {
      instanceId,
      clarifyUrl: `/ask/${encodeURIComponent(instanceId)}/clarify`,
      status,
    },
    { status: 202 },
  );
}

async function pollAsk(env: Env, instanceId: string): Promise<Response> {
  const ctx = makeCtx(env, instanceId);
  const engine = await Engine.create(env.DB, { conflictPolicy: "use_existing" });
  const wf = buildAskWorkflow(ctx);
  // resume() is idempotent on a parked workflow. If the 5-min timeout has
  // passed, the engine advances past the signal and runs incorporateAndSave
  // with an empty payload (treated as a skip).
  const status = await engine.resume(wf, instanceId);
  return Response.json({ instanceId, status });
}

async function postClarify(
  request: Request,
  env: Env,
  instanceId: string,
): Promise<Response> {
  const body = (await request.json().catch(() => ({}))) as ClarifySignal;
  const ctx = makeCtx(env, instanceId);
  const engine = await Engine.create(env.DB, { conflictPolicy: "use_existing" });
  const wf = buildAskWorkflow(ctx);
  await engine.sendSignal(instanceId, "human_clarify", body);
  const status = await engine.resume(wf, instanceId);
  return Response.json({ instanceId, status });
}

// ─── helpers ───────────────────────────────────────────────────────────────

function makeCtx(env: Env, instanceId: string): RagContext {
  return {
    db: env.DB,
    ai: env.AI,
    vec: env.VECTORIZE,
    raw: env.RAW,
    instanceId,
  };
}

async function readDraft(
  env: Env,
  instanceId: string,
): Promise<{ confidence: number; content: string } | null> {
  const row = await env.DB.prepare(
    "SELECT content, confidence FROM messages WHERE id = ?",
  )
    .bind(`${instanceId}:assistant`)
    .first<{ content: string; confidence: number }>();
  return row ?? null;
}

function methodNotAllowed(): Response {
  return new Response("Method not allowed", { status: 405 });
}

function errorResponse(err: unknown): Response {
  if (err instanceof WorkflowError) {
    return Response.json({ error: err.message }, { status: 500 });
  }
  const message = err instanceof Error ? err.message : String(err);
  return Response.json({ error: message }, { status: 500 });
}
