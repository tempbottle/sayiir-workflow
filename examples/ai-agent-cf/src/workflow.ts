/**
 * Durable AI research agent.
 *
 *   parseQuery
 *     │
 *     ▼
 *   fork ─── searchHackerNews
 *         └─ searchWikipedia
 *     │
 *     ▼
 *   join (mergeSources)
 *     │
 *     ▼
 *   initRefinement
 *     │
 *     ▼
 *   loop (≤ 3 iterations) — refineAnswer (Workers AI)
 *     │
 *     ▼
 *   saveDraft  ──► waitForSignal("approval", 24h)  ──► publish
 *
 * Tasks read Worker bindings and the instance id from module-scoped state
 * set per request. The values flowing through the workflow stay JSON
 * (which is what Sayiir checkpoints to D1).
 */

import {
  task,
  flow,
  branch,
  LoopResult,
  type Workflow,
} from "@sayiir/cloudflare";

// ─── Per-request context ─────────────────────────────────────────────────

export interface AgentContext {
  db: D1Database;
  ai: Ai;
  instanceId: string;
}

let agentContext: AgentContext | null = null;

export function withAgentContext<T>(ctx: AgentContext, fn: () => T): T {
  const prev = agentContext;
  agentContext = ctx;
  try {
    return fn();
  } finally {
    agentContext = prev;
  }
}

function ctx(): AgentContext {
  if (!agentContext) {
    throw new Error(
      "Agent context not set. Wrap engine calls in withAgentContext().",
    );
  }
  return agentContext;
}

// ─── Types ───────────────────────────────────────────────────────────────

export interface AgentInput {
  topic: string;
  maxResultsPerSource?: number;
}

interface ParsedQuery {
  topic: string;
  query: string;
  maxResults: number;
}

interface SourceHit {
  source: "hn" | "wikipedia";
  title: string;
  url: string;
  snippet: string;
}

interface BranchResult {
  topic: string;
  hits: SourceHit[];
}

interface MergedContext {
  topic: string;
  hits: SourceHit[];
}

interface RefinementState {
  topic: string;
  hits: SourceHit[];
  draft: string;
  iteration: number;
}

interface Draft {
  topic: string;
  body: string;
}

export interface ApprovalSignal {
  approvedBy?: string;
  edits?: string;
}

export interface Report {
  instanceId: string;
  topic: string;
  body: string;
  approvedBy?: string;
}

// ─── Tasks ───────────────────────────────────────────────────────────────

const parseQuery = task(
  "parse-query",
  (input: AgentInput): ParsedQuery => ({
    topic: input.topic,
    query: input.topic.trim().replace(/\s+/g, " "),
    maxResults: input.maxResultsPerSource ?? 3,
  }),
);

const searchHackerNews = task(
  "search-hn",
  async (q: ParsedQuery): Promise<BranchResult> => {
    const url = `https://hn.algolia.com/api/v1/search?query=${encodeURIComponent(
      q.query,
    )}&hitsPerPage=${q.maxResults}&tags=story`;
    const res = await fetch(url);
    if (!res.ok) throw new Error(`HN search failed: ${res.status}`);
    const json = (await res.json()) as {
      hits: { title?: string; url?: string; story_text?: string }[];
    };
    const hits: SourceHit[] = json.hits
      .filter((h) => h.title)
      .map((h) => ({
        source: "hn",
        title: h.title!,
        url: h.url ?? "",
        snippet: (h.story_text ?? "").slice(0, 400),
      }));
    return { topic: q.topic, hits };
  },
  { timeout: "20s", retries: 2 },
);

const searchWikipedia = task(
  "search-wikipedia",
  async (q: ParsedQuery): Promise<BranchResult> => {
    const url = `https://en.wikipedia.org/w/api.php?action=query&list=search&format=json&origin=*&srsearch=${encodeURIComponent(
      q.query,
    )}&srlimit=${q.maxResults}`;
    const res = await fetch(url);
    if (!res.ok) throw new Error(`Wikipedia search failed: ${res.status}`);
    const json = (await res.json()) as {
      query: { search: { title: string; snippet: string }[] };
    };
    const hits: SourceHit[] = json.query.search.map((s) => ({
      source: "wikipedia",
      title: s.title,
      url: `https://en.wikipedia.org/wiki/${encodeURIComponent(s.title.replace(/ /g, "_"))}`,
      snippet: stripHtml(s.snippet),
    }));
    return { topic: q.topic, hits };
  },
  { timeout: "20s", retries: 2 },
);

const initRefinement = task(
  "init-refinement",
  (merged: MergedContext): RefinementState => ({
    topic: merged.topic,
    hits: merged.hits,
    draft: "",
    iteration: 0,
  }),
);

/**
 * Iterative refinement loop body.
 *
 * Iteration 0 writes the first draft. Later iterations either signal `DONE`
 * (loop exits with the current state) or return a revised draft (loop
 * continues). Both branches return `RefinementState` so the loop's output
 * type is uniform; the finalize step extracts the draft.
 */
const refineAnswer = task(
  "refine-answer",
  async (
    state: RefinementState,
  ): Promise<LoopResult<RefinementState>> => {
    const { ai } = ctx();
    const sources = state.hits
      .map((h, i) => `[${i + 1}] ${h.title}\n${h.snippet}\nSource: ${h.url}`)
      .join("\n\n");

    const systemPrompt =
      "You are a research analyst. Synthesize a concise, factual briefing " +
      "from the provided sources. Cite sources inline as [n]. " +
      "If a previous draft is provided, critique it and emit a revised version.";

    const userPrompt = state.draft
      ? `Topic: ${state.topic}\n\nSources:\n${sources}\n\n` +
        `Previous draft (iteration ${state.iteration}):\n${state.draft}\n\n` +
        `If this draft is comprehensive and well-cited, reply with exactly "DONE".\n` +
        `Otherwise reply with the revised draft only.`
      : `Topic: ${state.topic}\n\nSources:\n${sources}\n\nWrite the initial briefing (200–400 words).`;

    const response = await ai.run("@cf/meta/llama-3.1-8b-instruct", {
      messages: [
        { role: "system", content: systemPrompt },
        { role: "user", content: userPrompt },
      ],
      max_tokens: 768,
    });

    const text = extractAiResponse(response).trim();

    if (state.draft && text === "DONE") {
      return LoopResult.done(state);
    }

    return LoopResult.again({
      ...state,
      draft: text || state.draft,
      iteration: state.iteration + 1,
    });
  },
  { timeout: "60s", retries: 1 },
);

const finalizeDraft = task(
  "finalize-draft",
  (state: RefinementState): Draft => ({
    topic: state.topic,
    body: state.draft,
  }),
);

const saveDraft = task("save-draft", async (draft: Draft): Promise<Draft> => {
  const { db, instanceId } = ctx();
  await db
    .prepare(
      "INSERT OR REPLACE INTO agent_drafts (instance_id, topic, body) VALUES (?, ?, ?)",
    )
    .bind(instanceId, draft.topic, draft.body)
    .run();
  return draft;
});

// Reads the draft saved by saveDraft and writes the (possibly edited) approved
// version into agent_reports. Going through D1 — rather than threading the
// draft past waitForSignal — keeps the workflow flat and lets a separate
// approval Worker resume it without re-supplying the draft.
const publish = task(
  "publish",
  async (approval: ApprovalSignal): Promise<Report> => {
    const { db, instanceId } = ctx();
    const row = await db
      .prepare("SELECT topic, body FROM agent_drafts WHERE instance_id = ?")
      .bind(instanceId)
      .first<{ topic: string; body: string }>();
    if (!row) {
      throw new Error(`Draft not found for instance ${instanceId}`);
    }
    const body = approval.edits ?? row.body;
    await db
      .prepare(
        "INSERT OR REPLACE INTO agent_reports (instance_id, topic, body, approved_by) VALUES (?, ?, ?, ?)",
      )
      .bind(instanceId, row.topic, body, approval.approvedBy ?? null)
      .run();
    return {
      instanceId,
      topic: row.topic,
      body,
      approvedBy: approval.approvedBy,
    };
  },
);

// ─── Flow ────────────────────────────────────────────────────────────────

export const researchAgent: Workflow<AgentInput, Report> = flow<AgentInput>(
  "research-agent",
)
  .then(parseQuery)
  .fork([branch("hn", searchHackerNews), branch("wikipedia", searchWikipedia)])
  .join("merge-sources", ([hn, wiki]) => mergeHits(hn, wiki))
  .then(initRefinement)
  .loop(refineAnswer, { maxIterations: 3, onMax: "exit_with_last" })
  .then(finalizeDraft)
  .then(saveDraft)
  .waitForSignal<ApprovalSignal>("approval", "approval", { timeout: "24h" })
  .then(publish)
  .build();

// ─── Helpers ─────────────────────────────────────────────────────────────

function mergeHits(a: BranchResult, b: BranchResult): MergedContext {
  const seen = new Set<string>();
  const hits = [...a.hits, ...b.hits].filter((h) => {
    const key = h.url || h.title;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
  return { topic: a.topic || b.topic, hits };
}

function stripHtml(s: string): string {
  return s.replace(/<[^>]*>/g, "");
}

interface AiChatResponse {
  response?: string;
  result?: { response?: string };
}
function extractAiResponse(raw: unknown): string {
  const r = raw as AiChatResponse;
  return r.response ?? r.result?.response ?? "";
}
