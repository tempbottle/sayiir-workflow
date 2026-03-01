/**
 * Task definition for workflow steps.
 *
 * The pure `task()` factory is re-exported from sayiir-flow-js.
 * `getTaskContext()` stays here because it depends on the native addon.
 */

import type { NapiTaskExecutionContext } from "./native.js";
import { getNative } from "./native.js";

// Re-export from sayiir-flow-js
export { task } from "sayiir-flow-js";
export type { TaskFn } from "sayiir-flow-js";

/** Task execution context available from within a running task. */
export type TaskExecutionContext = NapiTaskExecutionContext;

/**
 * Get the current task execution context.
 *
 * Returns `null` if called outside of a task execution.
 *
 * ```ts
 * const ctx = getTaskContext();
 * if (ctx) {
 *   console.log(`Running task ${ctx.taskId} in workflow ${ctx.workflowId}`);
 * }
 * ```
 */
export function getTaskContext(): TaskExecutionContext | null {
  return getNative().getTaskContext();
}
