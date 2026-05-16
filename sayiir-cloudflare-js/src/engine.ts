/**
 * Durable workflow engine for Cloudflare Workers over D1.
 *
 * Wraps the WASM `WasmDurableEngine` and `WasmContinuationStepper` with a
 * typed TypeScript API. All operations are async (D1 is async).
 *
 * Two execution paths:
 *   - `runWorkflow()` — stepper-based, no persistence (prototyping/testing)
 *   - `Engine` class — durable with D1 checkpointing (production)
 */

import type { Workflow } from "sayiir-flow-js";
import type { WorkflowStatus } from "./types.js";
import { WorkflowError } from "./types.js";

import {
  WasmDurableEngine,
  WasmContinuationStepper,
  type WasmWorkflow,
  type WasmWorkflowStatus,
} from "../wasm/sayiir_cloudflare.js";

/**
 * Opaque handle to a Cloudflare D1 database binding (`env.DB`).
 *
 * The facade passes this object straight through to WASM, which uses
 * `sqlx-d1` internally. Import the structural type from
 * `@cloudflare/workers-types` if you need to call query methods on the
 * binding directly from your worker.
 */
export type D1Database = object;

/**
 * What to do when {@link Engine.run} is called with an `instanceId` that
 * already has a snapshot in D1.
 *
 * Default is `"fail"`. Without an explicit policy the engine rejects the
 * call rather than overwriting in-progress state — call `resume()` for
 * known instances, or opt into `"use_existing"` / `"terminate_existing"`
 * for idempotent retries / hard restarts.
 */
export type ConflictPolicy = "fail" | "use_existing" | "terminate_existing";

/** Options for {@link Engine.create}. */
export interface EngineOptions {
  /** Default: `"fail"`. */
  conflictPolicy?: ConflictPolicy;
}

/** Options for durable run/resume. */
export interface DurableRunOptions {
  instanceId: string;
}

/** Durable workflow engine backed by Cloudflare D1. */
export class Engine {
  /** @internal */
  private readonly _inner: WasmDurableEngine;

  private constructor(inner: WasmDurableEngine) {
    this._inner = inner;
  }

  /**
   * Create an engine backed by a D1 database.
   *
   * Call once at startup and reuse across requests.
   *
   * The optional `conflictPolicy` controls what {@link Engine.run} does when
   * an instance id is reused; defaults to `"fail"` so duplicate `run()`
   * calls don't silently overwrite in-progress workflows.
   */
  static async create(db: D1Database, opts?: EngineOptions): Promise<Engine> {
    const inner = await WasmDurableEngine.create(db, opts?.conflictPolicy);
    return new Engine(inner);
  }

  /**
   * Run a workflow to completion (or until it parks) with checkpointing.
   *
   * If a snapshot for `instanceId` already exists, the engine's configured
   * `conflictPolicy` decides the outcome:
   * - `"fail"` (default) — rejects; call `resume()` for known instances.
   * - `"use_existing"` — returns the existing status without re-executing.
   * - `"terminate_existing"` — deletes the prior snapshot and starts fresh.
   */
  async run<TIn, TOut>(
    workflow: Workflow<TIn, TOut>,
    instanceId: string,
    input: TIn,
  ): Promise<WorkflowStatus<TOut>> {
    const raw = await this._inner.run(
      workflow._inner as unknown as WasmWorkflow,
      instanceId,
      input,
      workflow._taskRegistry,
    );
    return parseWorkflowStatus<TOut>(raw);
  }

  /** Resume a workflow from a saved checkpoint. */
  async resume<TIn, TOut>(
    workflow: Workflow<TIn, TOut>,
    instanceId: string,
  ): Promise<WorkflowStatus<TOut>> {
    const raw = await this._inner.resume(
      workflow._inner as unknown as WasmWorkflow,
      instanceId,
      workflow._taskRegistry,
    );
    return parseWorkflowStatus<TOut>(raw);
  }

  /** Request cancellation of a running workflow. */
  async cancel(
    instanceId: string,
    opts?: { reason?: string; cancelledBy?: string },
  ): Promise<void> {
    await this._inner.cancel(instanceId, opts?.reason, opts?.cancelledBy);
  }

  /** Request pausing of a running workflow. */
  async pause(
    instanceId: string,
    opts?: { reason?: string; pausedBy?: string },
  ): Promise<void> {
    await this._inner.pause(instanceId, opts?.reason, opts?.pausedBy);
  }

  /** Unpause a paused workflow so it can be resumed. */
  async unpause(instanceId: string): Promise<void> {
    await this._inner.unpause(instanceId);
  }

  /** Send an external signal to a workflow instance. */
  async sendSignal(
    instanceId: string,
    signalName: string,
    payload: unknown,
  ): Promise<void> {
    await this._inner.sendSignal(instanceId, signalName, payload);
  }

  /**
   * Find and resume workflow instances that are ready or stuck.
   *
   * Picks up three categories in a single pass:
   *   - **Ready** — parked at a delay, timed signal, or fork-with-delayed-
   *     branch whose wake time has passed.
   *   - **Signalled** — parked at `waitForSignal` (with or without timeout)
   *     and has a buffered event waiting to be consumed. Covers fire-and-
   *     forget `sendSignal()` where the caller does not also invoke
   *     `resume()`.
   *   - **Stale** — actively executing (not parked) but not updated within
   *     `staleAfter` seconds. Recovers from Worker eviction / CPU-limit
   *     kills. Excludes parked positions (`AtFork`, `AtSignal`, `AtDelay`)
   *     so workflows correctly waiting on an external event don't get
   *     periodically re-resumed every staleAfter window.
   *
   * Returns the statuses of all resumed instances. Use `limit` to stay
   * within Worker CPU budgets — remaining instances are picked up on the
   * next cron tick.
   *
   * **Note on signal latency.** Calling `sendSignal()` writes the event but
   * does not bump the snapshot's `updated_at`. The signalled instance is
   * picked up by the *next* cron tick that runs this method — if you call
   * `engine.resume()` inline from your `sendSignal` handler (the common
   * pattern), the cron path is just a safety net.
   *
   * @param workflow  The workflow definition to resume with.
   * @param opts.staleAfter  Seconds before a non-parked instance is
   *                         considered stuck (default: 300 — 5 min).
   * @param opts.limit  Maximum instances to resume per call (default: 10).
   *
   * @example
   * ```ts
   * async scheduled(event: ScheduledEvent, env: Env) {
   *   const engine = await Engine.create(env.DB);
   *   await engine.resumeAll(myWorkflow);
   * }
   * ```
   */
  async resumeAll<TIn, TOut>(
    workflow: Workflow<TIn, TOut>,
    opts?: { staleAfter?: number; limit?: number },
  ): Promise<WorkflowStatus<TOut>[]> {
    const staleAfter = opts?.staleAfter ?? 300;
    const limit = opts?.limit ?? 10;
    // Pickup query (ready / signalled / stale branches) lives in
    // sayiir-d1::SQLiteBackend::find_resumable_instances — see that
    // method's doc for the category definitions and the allow-list
    // governing the stale branch.
    const ids = await this._inner.findResumableInstances(staleAfter, limit);

    const statuses: WorkflowStatus<TOut>[] = [];
    for (const instanceId of ids) {
      statuses.push(await this.resume(workflow, instanceId));
    }
    return statuses;
  }
}

/**
 * Run a workflow to completion and return its output (no persistence).
 *
 * Uses the WASM continuation stepper. Supports async tasks.
 * For durable execution with checkpointing, use `Engine` instead.
 *
 * When called with `opts`, uses the durable engine. If the workflow does not
 * complete (e.g. it parks on a delay or signal), a `WorkflowError` is thrown.
 *
 * @example
 * ```ts
 * // Prototype — no persistence
 * const result = await runWorkflow(wf, input);
 *
 * // Production — same function, just add options
 * const engine = await Engine.create(env.DB);
 * const status = await engine.run(wf, "run-1", input);
 * ```
 */
export async function runWorkflow<TIn, TOut>(
  workflow: Workflow<TIn, TOut>,
  input: TIn,
  opts?: DurableRunOptions & { engine: Engine },
): Promise<TOut> {
  if (opts) {
    const status = await opts.engine.run(workflow, opts.instanceId, input);
    if (status.status !== "completed") {
      throw new WorkflowError(
        `Workflow did not complete (status=${status.status}). ` +
          `Use engine.run() to inspect the full status.`,
      );
    }
    return status.output;
  }

  const stepper = new WasmContinuationStepper(workflow._inner as unknown as WasmWorkflow, input);
  let step = stepper.current();

  while (step.kind === "task") {
    const taskFn = workflow._taskRegistry[step.taskId!];
    if (!taskFn) {
      throw new WorkflowError(`Task '${step.taskId}' not found in registry`);
    }
    const taskInput = step.inputJson != null ? JSON.parse(step.inputJson) : undefined;
    const output = await taskFn(taskInput);
    step = stepper.submitResult(output);
  }

  if (step.kind === "done") {
    return (step.outputJson != null ? JSON.parse(step.outputJson) : undefined) as TOut;
  }

  throw new WorkflowError(`Unexpected step kind: ${step.kind}`);
}

// ---- Internal helpers ----

function parseWorkflowStatus<TOut>(
  raw: WasmWorkflowStatus,
): WorkflowStatus<TOut> {
  switch (raw.status) {
    case "completed":
      return {
        status: "completed",
        output: (raw.outputJson != null
          ? JSON.parse(raw.outputJson)
          : undefined) as TOut,
      };
    case "in_progress":
      return { status: "in_progress" };
    case "failed":
      return { status: "failed", error: raw.error ?? "unknown error" };
    case "cancelled":
      return {
        status: "cancelled",
        reason: raw.reason,
        cancelledBy: raw.cancelledBy,
      };
    case "paused":
      return {
        status: "paused",
        reason: raw.reason,
        pausedBy: raw.pausedBy,
      };
    case "waiting":
      return {
        status: "waiting",
        wakeAt: raw.wakeAt ?? "",
        delayId: raw.delayId ?? "",
      };
    case "awaiting_signal":
      return {
        status: "awaiting_signal",
        signalId: raw.signalId ?? "",
        signalName: raw.signalName ?? "",
        wakeAt: raw.wakeAt,
      };
    default:
      throw new WorkflowError(`unknown workflow status: ${raw.status}`);
  }
}
