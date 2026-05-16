/**
 * Per-request context — the Cloudflare bindings + the optional
 * instance id of the workflow currently being run.
 *
 * The workflows in `workflow-ingest.ts` and `workflow-ask.ts` are built as
 * factory functions that close over a `RagContext`. Each HTTP handler builds
 * a fresh flow per request (cheap — just function references), so there are
 * no module-scoped globals to leak across concurrent requests on the same
 * isolate. This is the canonical pattern for passing bindings into Sayiir
 * tasks on Workers.
 */

export interface RagContext {
  readonly db: D1Database;
  readonly ai: Ai;
  readonly vec: VectorizeIndex;
  readonly raw: R2Bucket;
  /**
   * Instance id of the workflow currently being run. Tasks use it to scope
   * D1 writes (e.g. the conversation row) and to identify the workflow in
   * logs. Set per request before calling `engine.run` / `engine.resume`.
   */
  readonly instanceId: string;
}

/** Embedding model and its output dimensionality (must match the Vectorize index). */
export const EMBEDDING_MODEL = "@cf/baai/bge-base-en-v1.5";
export const EMBEDDING_DIMS = 768;

/** Completion model used for query parsing, refinement, and citation verification. */
export const COMPLETION_MODEL = "@cf/meta/llama-3.1-8b-instruct";
