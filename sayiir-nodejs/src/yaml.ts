/**
 * YAML workflow loader.
 *
 * Define workflow structure in YAML, register handlers in code, load and run:
 *
 * ```ts
 * import { task, runWorkflow } from "sayiir";
 * import { loadWorkflow } from "sayiir/yaml";
 *
 * const validateOrder = task("validate_order", (order) => ({ ...order, valid: true }));
 *
 * const workflow = loadWorkflow("order-pipeline.yaml");
 * const result = await runWorkflow(workflow, { id: 42, amount: 99.99 });
 * ```
 */

import { readFileSync } from "node:fs";
import { type Workflow, Flow, branch as flowBranch } from "./flow.js";
import { _globalTaskRegistry } from "./task.js";
import type { LoopResult, TaskCallback } from "./types.js";
import { getNative } from "./native.js";

/** Envelope carrying accumulated context between YAML wrapper tasks. */
interface Envelope {
  __ctx: {
    input: unknown;
    tasks: Record<string, { output: unknown }>;
  };
  __val: unknown;
}

// Re-export types for the yaml subpath
export type { Workflow };

/**
 * Load a YAML workflow definition.
 *
 * Handlers are looked up by name from two sources:
 * 1. The global task registry (populated by `task()`)
 * 2. The explicit `handlers` map (takes precedence)
 *
 * @param pathOrYaml - Path to a YAML file, or a YAML string.
 * @param handlers - Optional explicit handler map, keyed by handler name.
 */
export function loadWorkflow(
  pathOrYaml: string,
  handlers?: Record<string, TaskCallback>,
): Workflow<unknown, unknown> {
  let yamlStr: string;
  if (pathOrYaml.endsWith(".yaml") || pathOrYaml.endsWith(".yml")) {
    yamlStr = readFileSync(pathOrYaml, "utf-8");
  } else {
    yamlStr = pathOrYaml;
  }

  const native = getNative();
  const definition = native.parseYamlWorkflow(yamlStr) as unknown as YamlDefinition;

  // Merge handler registries
  const allHandlers: Record<string, TaskCallback> = {};
  for (const [id, fn] of _globalTaskRegistry) {
    allHandlers[id] = fn;
  }
  if (handlers) {
    Object.assign(allHandlers, handlers);
  }

  return buildWorkflow(definition, allHandlers);
}

// ---------------------------------------------------------------------------
// YAML schema types (from Rust parser output)
// ---------------------------------------------------------------------------

interface YamlDefinition {
  id: string;
  metadata?: Record<string, unknown>;
  tasks: YamlStep[];
}

type YamlStep =
  | YamlTaskStep
  | YamlDelayStep
  | YamlSignalStep
  | YamlForkStep
  | YamlBranchStep
  | YamlLoopStep
  | YamlChildStep;

interface YamlTaskStep {
  type: "task";
  id: string;
  handler?: string;
  action?: Record<string, unknown>;
  input?: string;
  timeout_secs?: number;
  retry?: Record<string, unknown>;
}

interface YamlDelayStep {
  type: "delay";
  id: string;
  duration_secs: number;
}

interface YamlSignalStep {
  type: "wait_for_signal";
  id: string;
  signal_name: string;
  timeout_secs?: number;
}

interface YamlForkStep {
  type: "fork";
  id: string;
  branches: Array<{ id: string; tasks: YamlStep[] }>;
}

interface YamlBranchStep {
  type: "branch";
  id: string;
  branches: Array<{ when: string; tasks: YamlStep[] }>;
  default?: YamlStep[];
}

interface YamlLoopStep {
  type: "loop";
  id: string;
  until?: string;
  for_each?: string;
  body: YamlStep[];
  max_iterations: number;
  on_max: string;
}

interface YamlChildStep {
  type: "child_workflow";
  id: string;
  tasks: YamlStep[];
}

// ---------------------------------------------------------------------------
// Workflow builder
// ---------------------------------------------------------------------------

function buildWorkflow(
  definition: YamlDefinition,
  handlers: Record<string, TaskCallback>,
): Workflow<unknown, unknown> {
  const f = new Flow<unknown, unknown>(definition.id);

  // Init: wrap input in envelope
  f.then("__yaml_init", (input: unknown) => ({
    __ctx: { input, tasks: {} },
    __val: input,
  }));

  // Compile all steps
  compileSteps(f, definition.tasks, handlers);

  // Finalize: extract __val
  f.then("__yaml_finalize", (env: unknown) => (env as Envelope).__val);

  return f.build();
}

function compileSteps(
  f: Flow<unknown, unknown>,
  steps: YamlStep[],
  handlers: Record<string, TaskCallback>,
): void {
  for (const step of steps) {
    switch (step.type) {
      case "task":
        compileTaskStep(f, step, handlers);
        break;
      case "delay":
        f.delay(step.id, step.duration_secs * 1000);
        break;
      case "wait_for_signal":
        f.waitForSignal(step.id, step.signal_name, {
          timeout: step.timeout_secs != null ? step.timeout_secs * 1000 : undefined,
        });
        break;
      case "fork":
        compileForkStep(f, step, handlers);
        break;
      case "branch":
        compileBranchStep(f, step, handlers);
        break;
      case "loop":
        compileLoopStep(f, step, handlers);
        break;
      case "child_workflow":
        compileChildStep(f, step, handlers);
        break;
    }
  }
}

function compileTaskStep(
  f: Flow<unknown, unknown>,
  step: YamlTaskStep,
  handlers: Record<string, TaskCallback>,
): void {
  const handlerName = step.handler;
  if (handlerName) {
    const handlerFn = handlers[handlerName];
    if (!handlerFn) {
      throw new Error(
        `Handler '${handlerName}' not found. Register it with task() or pass it via handlers.`,
      );
    }
    const wrapper = makeTaskWrapper(step.id, handlerFn, step.input);
    f.then(`__yaml_${step.id}`, wrapper);
  } else if (step.action) {
    throw new Error(
      `Built-in actions (${step.action.type ?? "?"}) are not yet supported in Node.js YAML workflows. Use a handler instead.`,
    );
  } else {
    throw new Error(`Task '${step.id}' must have either 'handler' or 'action'`);
  }
}

function compileForkStep(
  f: Flow<unknown, unknown>,
  step: YamlForkStep,
  handlers: Record<string, TaskCallback>,
): void {
  // Create branch definitions for the fork
  const branches = step.branches.map((yamlBranch) => {
    const wrappers = makeBranchWrappers(yamlBranch.tasks as YamlTaskStep[], handlers);
    if (wrappers.length === 0) {
      throw new Error(`Fork branch '${yamlBranch.id}' has no tasks`);
    }
    // Use first wrapper as the branch task
    return flowBranch(yamlBranch.id, wrappers[0]);
  });

  // For multi-task branches, we need to chain them.
  // Since the Flow API's branch() only takes one function, compose them.
  const composedBranches = step.branches.map((yamlBranch) => {
    const wrappers = makeBranchWrappers(yamlBranch.tasks as YamlTaskStep[], handlers);
    if (wrappers.length === 1) {
      return flowBranch(yamlBranch.id, wrappers[0]);
    }
    // Compose multiple wrappers into a single function
    const composed = (input: unknown) => {
      let current = input;
      for (const wrapper of wrappers) {
        current = wrapper(current);
      }
      return current;
    };
    return flowBranch(yamlBranch.id, composed);
  });

  f.fork(composedBranches)
    .join(`__yaml_${step.id}_join`, (branchResults: unknown) => {
      const results = branchResults as Record<string, Envelope>;
      let mergedCtx: Envelope["__ctx"] | null = null;
      for (const envelope of Object.values(results)) {
        if (!mergedCtx) {
          mergedCtx = JSON.parse(JSON.stringify(envelope.__ctx));
        } else {
          Object.assign(mergedCtx!.tasks, envelope.__ctx.tasks);
        }
      }
      const outputs: Record<string, unknown> = {};
      for (const [name, envelope] of Object.entries(results)) {
        outputs[name] = envelope.__val;
      }
      return {
        __ctx: mergedCtx ?? { input: null, tasks: {} },
        __val: outputs,
      };
    });
}

function compileBranchStep(
  f: Flow<unknown, unknown>,
  step: YamlBranchStep,
  handlers: Record<string, TaskCallback>,
): void {
  const native = getNative();
  const yamlBranches = step.branches;

  // Keys are branch indices as strings
  const keys = yamlBranches.map((_, i) => String(i));

  // Key function: evaluate 'when' conditions
  const keyFn = (envelope: unknown) => {
    const ctx = (envelope as Envelope).__ctx;
    for (let i = 0; i < yamlBranches.length; i++) {
      if (native.evalJmespathTruthy(yamlBranches[i].when, ctx)) {
        return String(i);
      }
    }
    return "default";
  };

  const routeBuilder = f.route(keyFn, keys);

  for (let i = 0; i < yamlBranches.length; i++) {
    const wrappers = makeBranchWrappers(yamlBranches[i].tasks as YamlTaskStep[], handlers);
    if (wrappers.length > 0) {
      const composed = composeWrappers(wrappers);
      routeBuilder.branch(String(i), `__yaml_${step.id}_b${i}`, composed);
    }
  }

  if (step.default) {
    const defaultWrappers = makeBranchWrappers(step.default as YamlTaskStep[], handlers);
    if (defaultWrappers.length > 0) {
      const composed = composeWrappers(defaultWrappers);
      routeBuilder.defaultBranch(`__yaml_${step.id}_default`, composed);
    }
  }

  routeBuilder.done();
}

function compileLoopStep(
  f: Flow<unknown, unknown>,
  step: YamlLoopStep,
  handlers: Record<string, TaskCallback>,
): void {
  const native = getNative();
  const bodyWrappers = (step.body as YamlTaskStep[]).map((t) => {
    if (t.type !== "task") {
      throw new Error(`Non-task steps in loop bodies are not yet supported (${t.type})`);
    }
    const handlerName = t.handler;
    if (!handlerName || !handlers[handlerName]) {
      throw new Error(`Handler '${handlerName}' not found for loop body task '${t.id}'`);
    }
    return makeTaskWrapper(t.id, handlers[handlerName], t.input);
  });

  const loopBody = (envelope: unknown): LoopResult<unknown> => {
    let current = envelope;
    for (const wrapper of bodyWrappers) {
      current = wrapper(current);
    }

    const ctx = (current as Envelope).__ctx;
    if (step.until) {
      if (native.evalJmespathTruthy(step.until, ctx)) {
        return { _loop: "done", value: current };
      }
      return { _loop: "again", value: current };
    }

    // No condition — done after one iteration
    return { _loop: "done", value: current };
  };

  f.loop(`__yaml_${step.id}`, loopBody, {
    maxIterations: step.max_iterations,
    onMax: step.on_max as "fail" | "exit_with_last",
  });
}

function compileChildStep(
  f: Flow<unknown, unknown>,
  step: YamlChildStep,
  handlers: Record<string, TaskCallback>,
): void {
  const childFlow = new Flow<unknown, unknown>(`child_${step.id}`);
  compileSteps(childFlow, step.tasks, handlers);
  const childWorkflow = childFlow.build();
  f.thenFlow(childWorkflow);
}

// ---------------------------------------------------------------------------
// Envelope helpers
// ---------------------------------------------------------------------------

function makeTaskWrapper(
  stepId: string,
  handlerFn: TaskCallback,
  inputExpr?: string,
): TaskCallback {
  const native = getNative();
  return (envelope: unknown) => {
    const env = envelope as Envelope;
    const ctx = env.__ctx;

    const handlerInput = inputExpr
      ? native.evalJmespath(inputExpr, ctx)
      : env.__val;

    const result = handlerFn(handlerInput);
    ctx.tasks[stepId] = { output: result };
    return { __ctx: ctx, __val: result };
  };
}

function makeBranchWrappers(
  tasks: YamlTaskStep[],
  handlers: Record<string, TaskCallback>,
): TaskCallback[] {
  return tasks.map((t) => {
    if (t.type !== "task") {
      throw new Error(`Non-task steps in branches are not yet supported (${t.type})`);
    }
    const handlerName = t.handler;
    if (!handlerName || !handlers[handlerName]) {
      throw new Error(`Handler '${handlerName}' not found for task '${t.id}'`);
    }
    return makeTaskWrapper(t.id, handlers[handlerName], t.input);
  });
}

function composeWrappers(wrappers: TaskCallback[]): TaskCallback {
  if (wrappers.length === 1) return wrappers[0];
  return (input: unknown) => {
    let current = input;
    for (const wrapper of wrappers) {
      current = wrapper(current);
    }
    return current;
  };
}
