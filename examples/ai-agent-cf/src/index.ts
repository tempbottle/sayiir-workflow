/**
 * HTTP + cron entry point for the AI research agent.
 *
 *   POST   /agents               { topic, maxResultsPerSource? }  → start
 *   GET    /agents/:id                                            → status
 *   POST   /agents/:id/approve   ApprovalSignal payload           → approve
 *   POST   /agents/:id/cancel    { reason?, cancelledBy? }        → cancel
 *
 * Cron (`* * * * *`) sweeps for parked/evicted instances via resumeAll().
 */

import { Engine, WorkflowError } from "@sayiir/cloudflare";
import {
  researchAgent,
  withAgentContext,
  type AgentInput,
  type ApprovalSignal,
} from "./workflow.js";

interface Env {
  DB: D1Database;
  AI: Ai;
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    const segments = url.pathname.split("/").filter(Boolean);

    try {
      if (segments[0] === "agents" && segments.length === 1) {
        if (request.method !== "POST") return methodNotAllowed();
        return await startAgent(request, env);
      }

      if (segments[0] === "agents" && segments.length >= 2) {
        const instanceId = decodeURIComponent(segments[1]);
        const action = segments[2];

        if (!action && request.method === "GET") {
          return await pollAgent(env, instanceId);
        }
        if (action === "approve" && request.method === "POST") {
          return await approveAgent(request, env, instanceId);
        }
        if (action === "cancel" && request.method === "POST") {
          return await cancelAgent(request, env, instanceId);
        }
      }

      return new Response("Not found", { status: 404 });
    } catch (err) {
      return errorResponse(err);
    }
  },

  async scheduled(_event: ScheduledController, env: Env): Promise<void> {
    const engine = await engineFor(env);
    await withAgentContext(
      { db: env.DB, ai: env.AI, instanceId: "<sweep>" },
      () => engine.resumeAll(researchAgent, { limit: 25 }),
    );
  },
} satisfies ExportedHandler<Env>;

// ─── Handlers ────────────────────────────────────────────────────────────

async function startAgent(request: Request, env: Env): Promise<Response> {
  const input = (await request.json()) as AgentInput;
  if (!input?.topic || typeof input.topic !== "string") {
    return new Response("Body must be { topic: string }", { status: 400 });
  }

  const instanceId = `agent-${crypto.randomUUID()}`;
  const engine = await engineFor(env);
  const status = await withAgentContext(
    { db: env.DB, ai: env.AI, instanceId },
    () => engine.run(researchAgent, instanceId, input),
  );

  return Response.json({ instanceId, status }, { status: statusCode(status) });
}

async function pollAgent(env: Env, instanceId: string): Promise<Response> {
  const engine = await engineFor(env);
  // resume() on a parked workflow is idempotent: if still parked it returns
  // the same parked status, so a poll-style GET works without ceremony.
  const status = await withAgentContext(
    { db: env.DB, ai: env.AI, instanceId },
    () => engine.resume(researchAgent, instanceId),
  );
  return Response.json({ instanceId, status }, { status: statusCode(status) });
}

async function approveAgent(
  request: Request,
  env: Env,
  instanceId: string,
): Promise<Response> {
  const payload = (await request.json().catch(() => ({}))) as ApprovalSignal;
  const engine = await engineFor(env);

  await engine.sendSignal(instanceId, "approval", payload);
  const status = await withAgentContext(
    { db: env.DB, ai: env.AI, instanceId },
    () => engine.resume(researchAgent, instanceId),
  );
  return Response.json({ instanceId, status }, { status: statusCode(status) });
}

async function cancelAgent(
  request: Request,
  env: Env,
  instanceId: string,
): Promise<Response> {
  const body = (await request.json().catch(() => ({}))) as {
    reason?: string;
    cancelledBy?: string;
  };
  const engine = await engineFor(env);
  await engine.cancel(instanceId, body);
  return new Response(null, { status: 204 });
}

// ─── Helpers ─────────────────────────────────────────────────────────────

function statusCode(status: { status: string }): number {
  switch (status.status) {
    case "completed":
      return 200;
    case "failed":
      return 500;
    case "cancelled":
      return 410;
    default:
      return 202;
  }
}

// Default conflict policy is "fail". `startAgent` builds its own instance id
// via crypto.randomUUID(), so we shouldn't ever collide — but if a caller
// retries (e.g. a queue redelivery), `use_existing` keeps the call idempotent
// instead of failing on the duplicate.
function engineFor(env: Env): Promise<Engine> {
  return Engine.create(env.DB, { conflictPolicy: "use_existing" });
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
