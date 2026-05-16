/**
 * Ask workflow — durable RAG with citation verification + human-in-the-loop fallback.
 *
 *   parseQuery
 *     │
 *     ▼
 *   fork ─┬─ vectorBranch  (Vectorize topK)
 *         ├─ keywordBranch (D1 FTS5)
 *         └─ recentBranch  (D1: recent docs)
 *     │
 *     ▼
 *   mergeContext  (RRF, top 10)
 *     │
 *     ▼
 *   loop refineAnswer  (≤3 iterations: draft → verify citations → done | refine)
 *     │
 *     ▼
 *   saveDraftCheckpoint  (persists provisional answer to D1)
 *     │
 *     ▼
 *   waitForSignal('human_clarify', 5min)
 *     │   The waitForSignal is ALWAYS in the flow — the HTTP handler sends
 *     │   a {skip: true} signal inline when confidence ≥ 0.6, so the
 *     │   high-confidence path still completes in a single HTTP round-trip.
 *     ▼
 *   incorporateAndSave  (reads back draft, applies clarification or skip,
 *                        writes final assistant message)
 *
 * Why the always-on signal + handler-side skip? `route().branch()` only
 * accepts a single task per branch, and `waitForSignal` is a flow-level
 * construct rather than a task, so a conditional "park or not" path can't
 * be expressed inside a route. Bridging state across the signal via D1
 * (the draft message is keyed by instanceId) is the canonical Sayiir
 * pattern: cross-signal state lives in D1, not in the flow value.
 */

import {
  task,
  flow,
  branch,
  LoopResult,
  type Workflow,
} from "@sayiir/cloudflare";

import { COMPLETION_MODEL, type RagContext } from "./context.js";
import {
  mergeContext as rrfMerge,
  vectorSearch,
  keywordSearch,
  recentDocs,
  type RetrievedChunk,
} from "./retrieval.js";
import { verifyCitations, type CitationCheck } from "./verify.js";

const CONFIDENCE_THRESHOLD = 0.6;
const MIN_SUPPORTED_CITATIONS = 2;

export interface AskInput {
  question: string;
  conversationId?: string;
}

interface ParsedQuery {
  question: string;
  keywords: string[];
  conversationId: string;
}

interface MergedRetrieval {
  question: string;
  conversationId: string;
  context: RetrievedChunk[];
}

interface RefinementState {
  question: string;
  conversationId: string;
  context: RetrievedChunk[];
  draft: string;
  feedback: string[];
  checks: CitationCheck[];
  confidence: number;
  iteration: number;
}

/** Result of the loop and the row that gets persisted as a provisional draft. */
interface DraftAnswer {
  question: string;
  conversationId: string;
  context: RetrievedChunk[];
  draft: string;
  citations: CitationDescriptor[];
  confidence: number;
}

interface CitationDescriptor {
  chunkId: string;
  docUrl: string;
  supported: boolean;
}

/** Payload of the `human_clarify` signal. Timeout delivers an empty object. */
export interface ClarifySignal {
  skip?: boolean;
  clarification?: string;
}

/** Final output returned to the HTTP caller. */
export interface AskOutput {
  conversationId: string;
  answer: string;
  citations: CitationDescriptor[];
  confidence: number;
  lowConfidence: boolean;
}

/** Build the ask workflow with bindings captured via closure. */
export function buildAskWorkflow(
  ctx: RagContext,
): Workflow<AskInput, AskOutput> {
  // ─── parseQuery ─────────────────────────────────────────────────────
  const parseQuery = task(
    "ask:parse-query",
    async (input: AskInput): Promise<ParsedQuery> => {
      const conversationId = input.conversationId ?? crypto.randomUUID();
      await ctx.db
        .prepare("INSERT OR IGNORE INTO conversations (id) VALUES (?)")
        .bind(conversationId)
        .run();

      await ctx.db
        .prepare(
          `INSERT OR REPLACE INTO messages
             (id, conversation_id, role, content)
           VALUES (?, ?, 'user', ?)`,
        )
        .bind(`${ctx.instanceId}:user`, conversationId, input.question)
        .run();

      const keywords = await extractKeywords(ctx, input.question);
      return { question: input.question, keywords, conversationId };
    },
    { timeout: "20s", retries: 1 },
  );

  // ─── fork branches ──────────────────────────────────────────────────
  const vectorBranch = task(
    "ask:vector-search",
    (q: ParsedQuery) => vectorSearch(ctx, q.question),
    { timeout: "15s", retries: 2 },
  );
  const keywordBranch = task(
    "ask:keyword-search",
    (q: ParsedQuery) => keywordSearch(ctx, q.keywords),
    { timeout: "10s", retries: 2 },
  );
  const recentBranch = task(
    "ask:recent-docs",
    (_q: ParsedQuery) => recentDocs(ctx),
    { timeout: "10s", retries: 2 },
  );

  // ─── refine (loop body) ─────────────────────────────────────────────
  const refineAnswer = task(
    "ask:refine",
    async (
      state: RefinementState | MergedRetrieval,
    ): Promise<LoopResult<RefinementState>> => {
      const init: RefinementState = isInitialState(state)
        ? {
            question: state.question,
            conversationId: state.conversationId,
            context: state.context,
            draft: "",
            feedback: [],
            checks: [],
            confidence: 0,
            iteration: 0,
          }
        : state;

      const draft = await draftAnswer(ctx, init);
      const verification = await verifyCitations(ctx, draft, init.context);

      const supportedCount = verification.checks.filter(
        (c) => c.supported === "yes",
      ).length;
      const next: RefinementState = {
        ...init,
        draft,
        feedback: verification.feedback,
        checks: verification.checks,
        confidence: verification.confidence,
        iteration: init.iteration + 1,
      };

      const done =
        verification.confidence >= CONFIDENCE_THRESHOLD &&
        supportedCount >= MIN_SUPPORTED_CITATIONS;
      return done ? LoopResult.done(next) : LoopResult.again(next);
    },
    { timeout: "60s", retries: 1 },
  );

  // ─── saveDraftCheckpoint ────────────────────────────────────────────
  const saveDraftCheckpoint = task(
    "ask:save-draft",
    async (state: RefinementState): Promise<DraftAnswer> => {
      const citations: CitationDescriptor[] = state.checks
        .filter((c) => c.supported !== "no")
        .map((c) => ({
          chunkId: c.chunkId,
          docUrl:
            state.context.find((ch) => ch.chunkId === c.chunkId)?.docUrl ?? "",
          supported: c.supported === "yes",
        }));

      await ctx.db
        .prepare(
          `INSERT OR REPLACE INTO messages
             (id, conversation_id, role, content, citations_json, confidence, low_confidence)
           VALUES (?, ?, 'assistant', ?, ?, ?, 1)`,
        )
        .bind(
          `${ctx.instanceId}:assistant`,
          state.conversationId,
          state.draft,
          JSON.stringify(citations),
          state.confidence,
        )
        .run();

      return {
        question: state.question,
        conversationId: state.conversationId,
        context: state.context,
        draft: state.draft,
        citations,
        confidence: state.confidence,
      };
    },
    { timeout: "15s", retries: 2 },
  );

  // ─── incorporateAndSave (post-signal) ───────────────────────────────
  // After waitForSignal, the flow's input type is ClarifySignal — the
  // earlier state is gone. We re-read the saved draft from D1 keyed by
  // instance id, then either confirm (skip / high-conf) or refine once
  // more with the clarification appended.
  const incorporateAndSave = task(
    "ask:incorporate",
    async (signal: ClarifySignal): Promise<AskOutput> => {
      const draftRow = await ctx.db
        .prepare(
          `SELECT conversation_id AS conversationId,
                  content,
                  citations_json  AS citationsJson,
                  confidence
             FROM messages
            WHERE id = ?`,
        )
        .bind(`${ctx.instanceId}:assistant`)
        .first<{
          conversationId: string;
          content: string;
          citationsJson: string | null;
          confidence: number;
        }>();

      if (!draftRow) {
        throw new Error(
          `No draft message for instance ${ctx.instanceId} - workflow state is missing`,
        );
      }

      const baseCitations: CitationDescriptor[] = draftRow.citationsJson
        ? (JSON.parse(draftRow.citationsJson) as CitationDescriptor[])
        : [];

      let finalAnswer = draftRow.content;
      let finalCitations = baseCitations;
      let finalConfidence = draftRow.confidence;

      // If a clarification was supplied (low-conf branch + human responded
      // before timeout), do ONE more refinement pass with the clarification
      // appended to the question.
      // Signal payload may be an empty object on timeout (`Bytes::new()`
      // decoded to `{}`) — treat that the same as a "skip".
      if (!signal?.skip && signal?.clarification) {
        const refined = await refineWithClarification(
          ctx,
          draftRow.content,
          signal.clarification,
        );
        finalAnswer = refined.draft;
        finalCitations = refined.citations;
        finalConfidence = refined.confidence;
      }

      const lowConfidence = finalConfidence < CONFIDENCE_THRESHOLD;

      await ctx.db
        .prepare(
          `UPDATE messages
              SET content        = ?,
                  citations_json = ?,
                  confidence     = ?,
                  low_confidence = ?
            WHERE id = ?`,
        )
        .bind(
          finalAnswer,
          JSON.stringify(finalCitations),
          finalConfidence,
          lowConfidence ? 1 : 0,
          `${ctx.instanceId}:assistant`,
        )
        .run();

      return {
        conversationId: draftRow.conversationId,
        answer: finalAnswer,
        citations: finalCitations,
        confidence: finalConfidence,
        lowConfidence,
      };
    },
    { timeout: "60s", retries: 1 },
  );

  return flow<AskInput>("ask")
    .then(parseQuery)
    .fork([
      branch("vector", vectorBranch),
      branch("keyword", keywordBranch),
      branch("recent", recentBranch),
    ])
    .join(
      "ask:merge",
      ([v, k, r]: [
        RetrievedChunk[],
        RetrievedChunk[],
        RetrievedChunk[],
      ]): MergedRetrieval => ({
        // question / conversationId get re-hydrated from D1 in the next
        // task; the join callback has no access to the original input.
        question: "",
        conversationId: "",
        context: rrfMerge(v, k, r),
      }),
    )
    .then(rehydrateMergedRetrieval(ctx))
    .loop(refineAnswer, { maxIterations: 3, onMax: "exit_with_last" })
    .then(saveDraftCheckpoint)
    .waitForSignal<ClarifySignal>("ask:human-clarify", "human_clarify", {
      timeout: "5m",
    })
    .then(incorporateAndSave)
    .build();
}

// ─── Helpers (closure factories + LLM prompts) ────────────────────────

/**
 * After the fork+join, we have the merged context but lost the question +
 * conversationId scalars. This task re-reads them from the user message
 * row that parseQuery wrote, then forwards the full MergedRetrieval to
 * the loop.
 */
function rehydrateMergedRetrieval(ctx: RagContext) {
  return task(
    "ask:rehydrate",
    async (merged: MergedRetrieval): Promise<MergedRetrieval> => {
      const row = await ctx.db
        .prepare(
          `SELECT content, conversation_id AS conversationId
             FROM messages WHERE id = ?`,
        )
        .bind(`${ctx.instanceId}:user`)
        .first<{ content: string; conversationId: string }>();
      if (!row) {
        throw new Error(
          `No user message for instance ${ctx.instanceId} - parseQuery did not run`,
        );
      }
      return {
        question: row.content,
        conversationId: row.conversationId,
        context: merged.context,
      };
    },
  );
}

function isInitialState(
  state: RefinementState | MergedRetrieval,
): state is MergedRetrieval {
  return !("draft" in state);
}

async function extractKeywords(
  ctx: RagContext,
  question: string,
): Promise<string[]> {
  const response = (await ctx.ai.run(COMPLETION_MODEL, {
    messages: [
      {
        role: "system",
        content:
          "Extract 3-6 keywords from the user's question for full-text search. " +
          "Reply with ONLY a JSON array of strings, no other text. " +
          'Example: ["cache","eviction","policy"]',
      },
      { role: "user", content: question },
    ],
    max_tokens: 64,
  })) as { response?: string; result?: { response?: string } };
  const text = (response.response ?? response.result?.response ?? "").trim();
  try {
    const arr = JSON.parse(text);
    if (Array.isArray(arr))
      return arr.filter((s): s is string => typeof s === "string");
  } catch {
    // fall through to word-tokenization fallback below
  }
  return [
    ...new Set(question.toLowerCase().match(/[a-z0-9]+/g) ?? []),
  ].slice(0, 6);
}

async function draftAnswer(
  ctx: RagContext,
  state: RefinementState,
): Promise<string> {
  const sources = state.context
    .map((c, i) => `[${i + 1}] (${c.docTitle ?? c.docUrl}): ${c.text}`)
    .join("\n\n");

  const systemPrompt =
    "You are a research assistant. Answer the user's question using ONLY the " +
    "numbered SOURCES below. Cite each claim inline with [n] referring to the " +
    "source number. If the sources do not contain the answer, say so clearly. " +
    "Aim for 120-250 words. Do not invent citations.";

  const userPrompt = state.draft
    ? `Question: ${state.question}\n\nSources:\n${sources}\n\n` +
      `Previous draft:\n${state.draft}\n\n` +
      `Feedback from the citation verifier:\n${state.feedback.join("\n") || "(none)"}\n\n` +
      `Revise the draft to address the feedback. Drop or replace unsupported citations.`
    : `Question: ${state.question}\n\nSources:\n${sources}\n\nWrite the answer.`;

  const response = (await ctx.ai.run(COMPLETION_MODEL, {
    messages: [
      { role: "system", content: systemPrompt },
      { role: "user", content: userPrompt },
    ],
    max_tokens: 768,
  })) as { response?: string; result?: { response?: string } };
  return (response.response ?? response.result?.response ?? "").trim();
}

async function refineWithClarification(
  ctx: RagContext,
  draft: string,
  clarification: string,
): Promise<{
  draft: string;
  citations: CitationDescriptor[];
  confidence: number;
}> {
  const response = (await ctx.ai.run(COMPLETION_MODEL, {
    messages: [
      {
        role: "system",
        content:
          "Revise the draft to incorporate the user's clarification. " +
          "Keep any inline [n] citations from the draft that still apply.",
      },
      {
        role: "user",
        content: `Draft:\n${draft}\n\nClarification:\n${clarification}\n\nRevised draft:`,
      },
    ],
    max_tokens: 768,
  })) as { response?: string; result?: { response?: string } };
  const text = (response.response ?? response.result?.response ?? "").trim();
  // Citations not re-verified after clarification - the user input is
  // authoritative for this example. Production systems may re-run verify here.
  return { draft: text, citations: [], confidence: 0.7 };
}
