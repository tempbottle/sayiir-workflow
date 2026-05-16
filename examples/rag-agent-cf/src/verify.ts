/**
 * Citation verification — the inside of the refinement loop.
 *
 * For each `[n]` citation in the draft, re-fetch the cited chunk *from R2*
 * via the chunk's `byte_start..byte_end` range, then ask the LLM whether the
 * chunk actually supports the cited claim. Unsupported citations are
 * dropped, and the sentences that depended on them are flagged for the
 * refinement step to rewrite.
 *
 * The point: the model can't cite-and-hallucinate against context it
 * never saw, because the citation is checked against the *source* text
 * stored in R2 (which was written at ingest time and never overwritten).
 */

import { COMPLETION_MODEL, type RagContext } from "./context.js";
import type { RetrievedChunk } from "./retrieval.js";

/** Outcome of verifying a single citation. */
export interface CitationCheck {
  chunkId: string;
  supported: "yes" | "no" | "partial";
  /** The claim sentence that cited this chunk (best-effort extraction). */
  claim: string;
}

/** Result of running verification across all citations in a draft. */
export interface VerificationResult {
  /** Per-citation outcomes — `supported: "no"` entries should be dropped. */
  checks: CitationCheck[];
  /** Fraction of citations marked "yes" (partial counts as 0.5). */
  confidence: number;
  /**
   * Plain-English notes the refinement step can feed back to the model
   * ("citation [3] doesn't support the claim about latency").
   */
  feedback: string[];
}

/**
 * Verify every `[n]` citation in `draft` against the source text in R2.
 *
 * `context` is the same merged retrieval result the draft was built from —
 * citations are 1-indexed into this list (so `[1]` refers to `context[0]`).
 */
export async function verifyCitations(
  ctx: RagContext,
  draft: string,
  context: RetrievedChunk[],
): Promise<VerificationResult> {
  const cited = extractCitations(draft, context);
  if (cited.length === 0) {
    // No citations at all — minimal-confidence answer.
    return { checks: [], confidence: 0, feedback: ["The draft contains no citations."] };
  }

  // Re-fetch each cited chunk from R2 via byte-range GET. This is the
  // canonical "the model only sees what's actually in the source" check.
  const checks = await Promise.all(
    cited.map(async ({ chunk, claim }) => checkOne(ctx, chunk, claim)),
  );

  const score = (c: CitationCheck) =>
    c.supported === "yes" ? 1 : c.supported === "partial" ? 0.5 : 0;
  const total = checks.reduce((sum, c) => sum + score(c), 0);
  const confidence = checks.length === 0 ? 0 : total / checks.length;

  const feedback = checks
    .filter((c) => c.supported !== "yes")
    .map((c) => `Citation for chunk ${c.chunkId} is ${c.supported}: "${c.claim}"`);

  return { checks, confidence, feedback };
}

// ─── Internal helpers ────────────────────────────────────────────────────

interface CitedClaim {
  chunk: RetrievedChunk;
  claim: string;
}

/**
 * Walk the draft, find `[n]` markers, and for each one extract the sentence
 * containing it as the "claim" and the corresponding chunk from the context.
 * Citations out of range are silently dropped.
 */
function extractCitations(draft: string, context: RetrievedChunk[]): CitedClaim[] {
  const out: CitedClaim[] = [];
  const seen = new Set<string>();
  const sentences = draft.split(/(?<=[.!?])\s+/);
  for (const sentence of sentences) {
    const matches = sentence.matchAll(/\[(\d+)\]/g);
    for (const m of matches) {
      const idx = parseInt(m[1]!, 10) - 1;
      const chunk = context[idx];
      if (!chunk) continue;
      // Deduplicate by chunkId+sentence so the same citation in the same
      // sentence isn't checked twice.
      const key = `${chunk.chunkId}::${sentence}`;
      if (seen.has(key)) continue;
      seen.add(key);
      out.push({ chunk, claim: sentence.trim() });
    }
  }
  return out;
}

async function checkOne(
  ctx: RagContext,
  chunk: RetrievedChunk,
  claim: string,
): Promise<CitationCheck> {
  // R2 range-GET the exact bytes of this chunk from the raw doc.
  const obj = await ctx.raw.get(chunk.rawR2Key, {
    range: { offset: chunk.byteStart, length: chunk.byteEnd - chunk.byteStart },
  });
  // Fall back to the indexed text if R2 doesn't have the raw bytes for some
  // reason (deleted doc, partial ingest). This keeps verify resilient
  // rather than failing the whole loop iteration.
  const sourceText = obj != null ? await obj.text() : chunk.text;

  const verdict = await askVerdict(ctx, sourceText, claim);
  return { chunkId: chunk.chunkId, supported: verdict, claim };
}

async function askVerdict(
  ctx: RagContext,
  sourceText: string,
  claim: string,
): Promise<"yes" | "no" | "partial"> {
  const response = (await ctx.ai.run(COMPLETION_MODEL, {
    messages: [
      {
        role: "system",
        content:
          "You are a fact-checker. Given a SOURCE and a CLAIM, reply with " +
          "exactly one word: YES if the source fully supports the claim, " +
          "PARTIAL if it supports it weakly or only in part, NO if it does " +
          "not support the claim. No explanation, just one word.",
      },
      {
        role: "user",
        content: `SOURCE:\n${sourceText}\n\nCLAIM:\n${claim}`,
      },
    ],
    max_tokens: 4,
  })) as { response?: string; result?: { response?: string } };

  const text = (response.response ?? response.result?.response ?? "").trim().toLowerCase();
  if (text.startsWith("yes")) return "yes";
  if (text.startsWith("partial")) return "partial";
  return "no";
}
