/**
 * Hybrid retrieval helpers used by the three fork branches of the ask flow.
 *
 * - `vectorSearch` — semantic top-K via Workers AI embedding + Vectorize.
 * - `keywordSearch` — D1 FTS5 over chunk text.
 * - `recentDocs`   — chunks from docs added in the last 24h (recency bias).
 *
 * `mergeContext` does Reciprocal Rank Fusion (RRF) across all three result
 * lists. RRF is the canonical "combine multiple rankers without learning
 * weights" technique: each result's score is the sum of `1 / (k + rank_i)`
 * across the rankers it appears in. `k = 60` is the literature default.
 */

import { EMBEDDING_MODEL, type RagContext } from "./context.js";

/** A retrieved chunk plus its origin metadata. */
export interface RetrievedChunk {
  chunkId: string;
  docId: string;
  text: string;
  byteStart: number;
  byteEnd: number;
  rawR2Key: string;
  docUrl: string;
  docTitle: string | null;
}

const RRF_K = 60;
const VECTOR_TOP_K = 8;
const KEYWORD_TOP_K = 5;
const RECENT_TOP_K = 3;
const RECENT_HOURS = 24;
const MERGED_TOP_K = 10;

/** Embed a query string and search Vectorize for the top-K nearest chunks. */
export async function vectorSearch(
  ctx: RagContext,
  query: string,
): Promise<RetrievedChunk[]> {
  const embedding = await embedQuery(ctx, query);
  const matches = await ctx.vec.query(embedding, { topK: VECTOR_TOP_K });
  const ids = matches.matches.map((m) => String(m.id));
  if (ids.length === 0) return [];
  return await loadChunks(ctx, ids);
}

/** FTS5 keyword search over `chunks_fts`. */
export async function keywordSearch(
  ctx: RagContext,
  keywords: string[],
): Promise<RetrievedChunk[]> {
  // Build a simple OR-of-terms FTS5 query. We escape each term to be safe
  // against FTS5 syntax characters (`"`, `*`, etc.) by wrapping in quotes.
  const ftsQuery = keywords
    .filter((k) => k.trim().length > 0)
    .map((k) => `"${k.replace(/"/g, '""')}"`)
    .join(" OR ");
  if (!ftsQuery) return [];

  const rows = await ctx.db
    .prepare(
      `SELECT c.id          AS chunkId,
              c.doc_id      AS docId,
              c.text        AS text,
              c.byte_start  AS byteStart,
              c.byte_end    AS byteEnd,
              d.raw_r2_key  AS rawR2Key,
              d.url         AS docUrl,
              d.title       AS docTitle
         FROM chunks_fts
         JOIN chunks c ON c.rowid = chunks_fts.rowid
         JOIN docs   d ON d.id    = c.doc_id
        WHERE chunks_fts MATCH ?
        ORDER BY rank
        LIMIT ?`,
    )
    .bind(ftsQuery, KEYWORD_TOP_K)
    .all<RetrievedChunk>();
  return rows.results ?? [];
}

/** Chunks from docs ingested in the last 24h — biases toward fresh content. */
export async function recentDocs(ctx: RagContext): Promise<RetrievedChunk[]> {
  const rows = await ctx.db
    .prepare(
      `SELECT c.id          AS chunkId,
              c.doc_id      AS docId,
              c.text        AS text,
              c.byte_start  AS byteStart,
              c.byte_end    AS byteEnd,
              d.raw_r2_key  AS rawR2Key,
              d.url         AS docUrl,
              d.title       AS docTitle
         FROM docs d
         JOIN chunks c ON c.doc_id = d.id
        WHERE d.fetched_at >= datetime('now', ?)
        ORDER BY d.fetched_at DESC, c.ordinal ASC
        LIMIT ?`,
    )
    .bind(`-${RECENT_HOURS} hours`, RECENT_TOP_K)
    .all<RetrievedChunk>();
  return rows.results ?? [];
}

/**
 * RRF-merge three ranked lists into a single top-N. Chunks appearing in
 * more lists outrank chunks that only appear in one, which is the whole
 * point of hybrid retrieval.
 */
export function mergeContext(
  vector: RetrievedChunk[],
  keyword: RetrievedChunk[],
  recent: RetrievedChunk[],
): RetrievedChunk[] {
  const scores = new Map<string, number>();
  const byId = new Map<string, RetrievedChunk>();

  const add = (list: RetrievedChunk[]) => {
    list.forEach((chunk, i) => {
      const score = 1 / (RRF_K + i + 1);
      scores.set(chunk.chunkId, (scores.get(chunk.chunkId) ?? 0) + score);
      byId.set(chunk.chunkId, chunk);
    });
  };
  add(vector);
  add(keyword);
  add(recent);

  return [...scores.entries()]
    .sort(([, a], [, b]) => b - a)
    .slice(0, MERGED_TOP_K)
    .map(([id]) => byId.get(id)!)
    .filter(Boolean);
}

// ─── Internal helpers ────────────────────────────────────────────────────

async function embedQuery(ctx: RagContext, text: string): Promise<number[]> {
  const result = (await ctx.ai.run(EMBEDDING_MODEL, { text: [text] })) as {
    data: number[][];
  };
  const v = result.data?.[0];
  if (!v) throw new Error("Embedding model returned no vector");
  return v;
}

async function loadChunks(
  ctx: RagContext,
  chunkIds: string[],
): Promise<RetrievedChunk[]> {
  const placeholders = chunkIds.map(() => "?").join(",");
  const rows = await ctx.db
    .prepare(
      `SELECT c.id          AS chunkId,
              c.doc_id      AS docId,
              c.text        AS text,
              c.byte_start  AS byteStart,
              c.byte_end    AS byteEnd,
              d.raw_r2_key  AS rawR2Key,
              d.url         AS docUrl,
              d.title       AS docTitle
         FROM chunks c
         JOIN docs   d ON d.id = c.doc_id
        WHERE c.id IN (${placeholders})`,
    )
    .bind(...chunkIds)
    .all<RetrievedChunk>();
  const byId = new Map((rows.results ?? []).map((r) => [r.chunkId, r]));
  // Preserve the Vectorize ranking by reordering by the input id list.
  return chunkIds.map((id) => byId.get(id)).filter((r): r is RetrievedChunk => r != null);
}
