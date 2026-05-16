/**
 * Ingest workflow — fetch a URL, chunk it, embed, store in Vectorize + R2 + D1.
 *
 *   prepareDoc ──► embedAndIndex
 *
 * The flow is intentionally two coarse tasks rather than six fine-grained
 * ones. Sayiir checkpoints each task's *return value* as a JSON row in
 * D1, which has a ~1MB row size limit. A chunked Wikipedia article + its
 * embeddings can easily exceed that as an intermediate blob, so we keep
 * the heavy data (chunk text, embeddings) inside one task each, and only
 * pass a tiny `{docId}` envelope across the checkpoint boundary.
 *
 * - `prepareDoc`: fetches the URL, extracts text, chunks it, puts the
 *   extracted text to R2 (one object per doc), and inserts the docs +
 *   chunks rows into D1. Idempotent on `INSERT OR REPLACE` so a re-run
 *   after eviction converges.
 * - `embedAndIndex`: reads chunks back from D1 by `docId`, embeds them
 *   in batches via Workers AI, and upserts the vectors into Vectorize.
 *
 * Trade-off: one Worker eviction during `prepareDoc` loses the entire
 * fetch+chunk+R2 work. For the example that's acceptable — see the
 * "D1 snapshot size limit" section of the README for the broader story
 * and the planned S3 snapshot backend for large blobs.
 */

import { task, flow, type Workflow } from "@sayiir/cloudflare";

import {
  EMBEDDING_MODEL,
  EMBEDDING_DIMS,
  type RagContext,
} from "./context.js";

const CHUNK_TARGET_CHARS = 1800;   // ~512 tokens at 3.5 chars/token avg
const CHUNK_OVERLAP_CHARS = 180;   // ~50-token overlap
const USER_AGENT = "sayiir-rag-cf/0.1 (+https://github.com/sayiir/sayiir)";
const EMBED_BATCH = 32;

export interface IngestInput {
  url: string;
}

export interface IngestOutput {
  docId: string;
  url: string;
  chunkCount: number;
}

interface PreparedDoc {
  docId: string;
  url: string;
  chunkCount: number;
}

interface Chunk {
  ordinal: number;
  text: string;
  byteStart: number;
  byteEnd: number;
}

/** Build the ingest workflow with bindings captured via closure. */
export function buildIngestWorkflow(
  ctx: RagContext,
): Workflow<IngestInput, IngestOutput> {
  const prepareDoc = task(
    "ingest:prepare",
    async (input: IngestInput): Promise<PreparedDoc> => {
      // Fetch.
      const res = await fetch(input.url, {
        headers: { "User-Agent": USER_AGENT },
      });
      if (!res.ok) throw new Error(`Fetch failed: ${res.status} ${input.url}`);
      const contentType = res.headers.get("content-type") ?? "text/html";
      const raw = await res.text();

      // Extract title + plaintext.
      const title =
        raw.match(/<title>([^<]+)<\/title>/i)?.[1]?.trim() ??
        raw.match(/<h1[^>]*>([^<]+)<\/h1>/i)?.[1]?.trim() ??
        null;
      const text = raw
        .replace(/<script[\s\S]*?<\/script>/gi, " ")
        .replace(/<style[\s\S]*?<\/style>/gi, " ")
        .replace(/<[^>]+>/g, " ")
        .replace(/&nbsp;/g, " ")
        .replace(/&amp;/g, "&")
        .replace(/&lt;/g, "<")
        .replace(/&gt;/g, ">")
        .replace(/\s+/g, " ")
        .trim();

      // Chunk.
      const chunks = chunkText(text);

      // Persist: R2 (extracted text) + D1 docs + D1 chunks.
      const docId = crypto.randomUUID();
      const rawR2Key = docId;

      const encoder = new TextEncoder();
      await ctx.raw.put(rawR2Key, encoder.encode(text), {
        httpMetadata: { contentType: "text/plain; charset=utf-8" },
      });

      await ctx.db
        .prepare(
          `INSERT OR REPLACE INTO docs
             (id, url, title, content_type, raw_r2_key)
           VALUES (?, ?, ?, ?, ?)`,
        )
        .bind(docId, input.url, title, contentType, rawR2Key)
        .run();

      if (chunks.length > 0) {
        const stmts = chunks.map((c) =>
          ctx.db
            .prepare(
              `INSERT OR REPLACE INTO chunks
                 (id, doc_id, ordinal, text, byte_start, byte_end)
               VALUES (?, ?, ?, ?, ?, ?)`,
            )
            .bind(
              `${docId}:${c.ordinal}`,
              docId,
              c.ordinal,
              c.text,
              c.byteStart,
              c.byteEnd,
            ),
        );
        await ctx.db.batch(stmts);
      }

      // Only the docId envelope crosses the checkpoint — keeps D1's
      // task-result row well under the size limit.
      return { docId, url: input.url, chunkCount: chunks.length };
    },
    { timeout: "60s", retries: 2 },
  );

  const embedAndIndex = task(
    "ingest:embed-index",
    async (prepared: PreparedDoc): Promise<IngestOutput> => {
      // Re-read chunks from D1 (cheap; one read).
      const rows = await ctx.db
        .prepare(
          `SELECT id, ordinal, text FROM chunks WHERE doc_id = ? ORDER BY ordinal`,
        )
        .bind(prepared.docId)
        .all<{ id: string; ordinal: number; text: string }>();
      const chunks = rows.results ?? [];

      // Embed in batches and upsert each batch immediately so partial
      // progress is reflected in Vectorize on re-run.
      for (let i = 0; i < chunks.length; i += EMBED_BATCH) {
        const batch = chunks.slice(i, i + EMBED_BATCH);
        const result = (await ctx.ai.run(EMBEDDING_MODEL, {
          text: batch.map((c) => c.text),
        })) as { data: number[][] };
        if (!result.data || result.data.length !== batch.length) {
          throw new Error(
            `Embedding batch returned ${result.data?.length ?? 0} vectors for ${batch.length} chunks`,
          );
        }
        const vectors = batch.map((chunk, j) => {
          const embedding = result.data[j]!;
          if (embedding.length !== EMBEDDING_DIMS) {
            throw new Error(
              `Embedding dim mismatch on ${chunk.id}: got ${embedding.length}, expected ${EMBEDDING_DIMS}`,
            );
          }
          return {
            id: chunk.id,
            values: embedding,
            metadata: { doc_id: prepared.docId, ordinal: chunk.ordinal },
          };
        });
        await ctx.vec.upsert(vectors);
      }

      return {
        docId: prepared.docId,
        url: prepared.url,
        chunkCount: chunks.length,
      };
    },
    { timeout: "180s", retries: 1 },
  );

  return flow<IngestInput>("ingest-doc")
    .then(prepareDoc)
    .then(embedAndIndex)
    .build();
}

// ─── Helpers ─────────────────────────────────────────────────────────────

/**
 * Fixed-character chunking with overlap. Aims for paragraph-respecting
 * splits where it can — splits on `\n\n` first, then sentences, then a
 * hard character boundary. Byte offsets are into the extracted plaintext
 * (which is what R2 stores), so verify.ts's range-GET returns exactly
 * the cited chunk text.
 */
function chunkText(text: string): Chunk[] {
  const chunks: Chunk[] = [];
  if (text.length === 0) return chunks;

  let ordinal = 0;
  let cursor = 0;

  while (cursor < text.length) {
    const targetEnd = Math.min(cursor + CHUNK_TARGET_CHARS, text.length);
    let end = targetEnd;
    if (end < text.length) {
      const paragraphBreak = text.lastIndexOf("\n\n", end);
      if (paragraphBreak > cursor + CHUNK_TARGET_CHARS / 2) {
        end = paragraphBreak;
      } else {
        const sentenceBreak = text.lastIndexOf(". ", end);
        if (sentenceBreak > cursor + CHUNK_TARGET_CHARS / 2) {
          end = sentenceBreak + 1;
        }
      }
    }
    const slice = text.slice(cursor, end).trim();
    if (slice.length > 0) {
      chunks.push({ ordinal, text: slice, byteStart: cursor, byteEnd: end });
      ordinal += 1;
    }
    if (end >= text.length) break;
    cursor = Math.max(end - CHUNK_OVERLAP_CHARS, cursor + 1);
  }
  return chunks;
}
