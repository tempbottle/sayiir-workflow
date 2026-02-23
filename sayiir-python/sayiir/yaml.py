"""YAML workflow loader.

Define workflow structure in YAML, register handlers in code, load and run::

    from sayiir import task, run_workflow
    from sayiir.yaml import load_workflow

    @task
    def validate_order(order):
        return {**order, "valid": True}

    workflow = load_workflow("order-pipeline.yaml")
    result = run_workflow(workflow, {"id": 42, "amount": 99.99})
"""

import copy
from collections.abc import Callable
from pathlib import Path
from typing import Any

from ._sayiir import eval_jmespath, eval_jmespath_truthy, parse_yaml_workflow
from .flow import Flow, Workflow
from .loop_result import LoopResult

# Global task registry populated by @task decorator.
_GLOBAL_TASK_REGISTRY: dict[str, Callable[..., Any]] = {}


def _register_global_task(task_id: str, func: Callable[..., Any]) -> None:
    """Register a task in the global registry (called by @task decorator)."""
    _GLOBAL_TASK_REGISTRY[task_id] = func


def load_workflow(
    path_or_yaml: str,
    *,
    handlers: dict[str, Callable[..., Any]] | None = None,
) -> Workflow:
    """Load a YAML workflow definition.

    Handlers are looked up by name from two sources:
    1. The global task registry (populated by ``@task``)
    2. The explicit ``handlers`` dict (takes precedence)

    Args:
        path_or_yaml: Path to a YAML file, or a YAML string.
        handlers: Optional explicit handler map, keyed by handler name.

    Returns:
        A compiled Workflow ready for execution with ``run_workflow``.

    Raises:
        ValueError: If the YAML is invalid or a referenced handler is missing.
    """
    if Path(path_or_yaml).suffix in (".yaml", ".yml") and Path(path_or_yaml).exists():
        yaml_str = Path(path_or_yaml).read_text()
    else:
        yaml_str = path_or_yaml

    definition = parse_yaml_workflow(yaml_str)

    all_handlers: dict[str, Callable[..., Any]] = {**_GLOBAL_TASK_REGISTRY}
    if handlers:
        all_handlers.update(handlers)

    return _build_workflow(definition, all_handlers)


def _build_workflow(
    definition: dict[str, Any],
    handlers: dict[str, Callable[..., Any]],
) -> Workflow:
    """Build a Workflow from a parsed YAML definition."""
    flow = Flow(definition["id"])

    # Init: wrap input in envelope
    flow = flow.then(
        lambda inp: {"__ctx": {"input": inp, "tasks": {}}, "__val": inp},
        name="__yaml_init",
    )

    # Compile all steps
    _compile_steps(flow, definition["tasks"], handlers)

    # Finalize: extract __val from envelope
    flow = flow.then(lambda env: env["__val"], name="__yaml_finalize")

    return flow.build()


# ---------------------------------------------------------------------------
# Step compilers
# ---------------------------------------------------------------------------


def _compile_steps(
    flow: Flow,
    steps: list[dict[str, Any]],
    handlers: dict[str, Callable[..., Any]],
) -> None:
    """Walk YAML steps and add them to the Flow."""
    for step in steps:
        step_type = step["type"]
        if step_type == "task":
            _compile_task_step(flow, step, handlers)
        elif step_type == "delay":
            _compile_delay_step(flow, step)
        elif step_type == "wait_for_signal":
            _compile_signal_step(flow, step)
        elif step_type == "fork":
            _compile_fork_step(flow, step, handlers)
        elif step_type == "branch":
            _compile_branch_step(flow, step, handlers)
        elif step_type == "loop":
            _compile_loop_step(flow, step, handlers)
        elif step_type == "child_workflow":
            _compile_child_step(flow, step, handlers)
        else:
            raise ValueError(f"Unknown YAML step type: {step_type}")


def _compile_task_step(
    flow: Flow,
    step: dict[str, Any],
    handlers: dict[str, Callable[..., Any]],
) -> None:
    """Compile a task step with envelope wrapper."""
    step_id = step["id"]
    handler_name = step.get("handler")
    action = step.get("action")
    input_expr = step.get("input")

    if handler_name:
        handler_fn = handlers.get(handler_name)
        if handler_fn is None:
            raise ValueError(
                f"Handler '{handler_name}' not found. "
                f"Register it with @task or pass it via handlers=."
            )
    elif action:
        raise NotImplementedError(
            f"Built-in actions ({action.get('type', '?')}) are not yet "
            f"supported in Python YAML workflows. Use a handler instead."
        )
    else:
        raise ValueError(f"Task '{step_id}' must have either 'handler' or 'action'")

    wrapper = _make_task_wrapper(step_id, handler_fn, input_expr)
    flow.then(wrapper, name=f"__yaml_{step_id}")


def _compile_delay_step(flow: Flow, step: dict[str, Any]) -> None:
    """Compile a delay step."""
    flow.delay(step["id"], step["duration_secs"])


def _compile_signal_step(flow: Flow, step: dict[str, Any]) -> None:
    """Compile a wait_for_signal step."""
    timeout = step.get("timeout_secs")
    flow.wait_for_signal(
        step["signal_name"],
        name=step["id"],
        timeout=float(timeout) if timeout is not None else None,
    )


def _compile_fork_step(
    flow: Flow,
    step: dict[str, Any],
    handlers: dict[str, Callable[..., Any]],
) -> None:
    """Compile a fork step with parallel branches."""
    fork_builder = flow.fork()

    for yaml_branch in step["branches"]:
        branch_id = yaml_branch["id"]
        wrappers = _make_branch_wrappers(yaml_branch["tasks"], handlers)

        if not wrappers:
            raise ValueError(f"Fork branch '{branch_id}' has no tasks")

        # First wrapper gets branch_id as name (used as key in join results)
        fork_builder = fork_builder.branch(*wrappers, name=branch_id)

    # Join: merge contexts from all branches
    fork_id = step["id"]

    def _make_join() -> Callable[..., Any]:
        def fork_join(branch_results: dict[str, Any]) -> dict[str, Any]:
            merged_ctx: dict[str, Any] | None = None
            for envelope in branch_results.values():
                if merged_ctx is None:
                    merged_ctx = copy.deepcopy(envelope["__ctx"])
                else:
                    merged_ctx["tasks"].update(envelope["__ctx"]["tasks"])
            outputs = {name: env["__val"] for name, env in branch_results.items()}
            return {"__ctx": merged_ctx or {"input": None, "tasks": {}}, "__val": outputs}

        return fork_join

    fork_builder.join(_make_join(), name=f"__yaml_{fork_id}_join")


def _compile_branch_step(
    flow: Flow,
    step: dict[str, Any],
    handlers: dict[str, Callable[..., Any]],
) -> None:
    """Compile a conditional branch step."""
    yaml_branches = step["branches"]
    default_tasks = step.get("default")

    # Declared keys are branch indices as strings
    keys = [str(i) for i in range(len(yaml_branches))]
    if default_tasks:
        # default is handled by .default_branch(), not as a key
        pass

    # Key function: evaluate 'when' conditions against envelope context
    def _make_key_fn(branches: list[dict[str, Any]]) -> Callable[..., Any]:
        def key_fn(envelope: dict[str, Any]) -> str:
            ctx = envelope["__ctx"]
            for i, branch in enumerate(branches):
                if eval_jmespath_truthy(branch["when"], ctx):
                    return str(i)
            return "default"

        return key_fn

    route_builder = flow.route(_make_key_fn(yaml_branches), keys=keys)

    for i, yaml_branch in enumerate(yaml_branches):
        wrappers = _make_branch_wrappers(yaml_branch["tasks"], handlers)
        if wrappers:
            route_builder = route_builder.branch(str(i), *wrappers, name=f"__yaml_{step['id']}_b{i}")

    if default_tasks:
        default_wrappers = _make_branch_wrappers(default_tasks, handlers)
        if default_wrappers:
            route_builder = route_builder.default_branch(
                *default_wrappers, name=f"__yaml_{step['id']}_default"
            )

    route_builder.done()


def _compile_loop_step(
    flow: Flow,
    step: dict[str, Any],
    handlers: dict[str, Callable[..., Any]],
) -> None:
    """Compile a loop step."""
    until_expr = step.get("until")
    for_each_expr = step.get("for_each")
    max_iterations = step.get("max_iterations", 1000)
    on_max = step.get("on_max", "fail")

    if not step.get("body"):
        raise ValueError(f"Loop '{step['id']}' has no body tasks")

    # For simplicity, compose the loop body into a single function
    body_tasks = step["body"]

    def _make_loop_body(
        tasks: list[dict[str, Any]],
        hdlrs: dict[str, Callable[..., Any]],
        until: str | None,
        for_each: str | None,
    ) -> Callable[..., Any]:
        # Pre-build wrapper functions for each task in the body
        task_wrappers = []
        for t in tasks:
            if t["type"] != "task":
                raise NotImplementedError(
                    f"Non-task steps in loop bodies are not yet supported ({t['type']})"
                )
            handler_name = t.get("handler")
            if not handler_name or handler_name not in hdlrs:
                raise ValueError(
                    f"Handler '{handler_name}' not found for loop body task '{t['id']}'"
                )
            tw = _make_task_wrapper(t["id"], hdlrs[handler_name], t.get("input"))
            task_wrappers.append(tw)

        def loop_body(envelope: dict[str, Any]) -> LoopResult:
            # Run all body tasks in sequence
            current = envelope
            for tw in task_wrappers:
                current = tw(current)

            # Evaluate termination condition
            ctx = current["__ctx"]
            if until:
                if eval_jmespath_truthy(until, ctx):
                    return LoopResult.done(current)
                return LoopResult.again(current)
            elif for_each:
                # for_each not yet supported
                raise NotImplementedError("for_each loops are not yet supported")
            else:
                # No condition — always done after one iteration
                return LoopResult.done(current)

        return loop_body

    body_fn = _make_loop_body(body_tasks, handlers, until_expr, for_each_expr)
    flow.loop(body_fn, max_iterations=max_iterations, on_max=on_max, name=f"__yaml_{step['id']}")


def _compile_child_step(
    flow: Flow,
    step: dict[str, Any],
    handlers: dict[str, Callable[..., Any]],
) -> None:
    """Compile a child_workflow step."""
    child_flow = Flow(f"child_{step['id']}")
    _compile_steps(child_flow, step["tasks"], handlers)
    child_workflow = child_flow.build()
    flow.then_flow(child_workflow)


# ---------------------------------------------------------------------------
# Envelope helpers
# ---------------------------------------------------------------------------


def _make_task_wrapper(
    step_id: str,
    handler_fn: Callable[..., Any],
    input_expr: str | None,
) -> Callable[..., Any]:
    """Create an envelope-aware wrapper around a handler function."""

    def wrapper(envelope: dict[str, Any]) -> dict[str, Any]:
        ctx = envelope["__ctx"]
        if input_expr:
            handler_input = eval_jmespath(input_expr, ctx)
        else:
            handler_input = envelope["__val"]

        result = handler_fn(handler_input)
        ctx["tasks"][step_id] = {"output": result}
        return {"__ctx": ctx, "__val": result}

    wrapper._task_id = f"__yaml_{step_id}"  # type: ignore[attr-defined]
    return wrapper


def _make_branch_wrappers(
    tasks: list[dict[str, Any]],
    handlers: dict[str, Callable[..., Any]],
) -> list[Callable[..., Any]]:
    """Create envelope-aware wrappers for a list of task steps."""
    wrappers = []
    for t in tasks:
        if t["type"] != "task":
            raise NotImplementedError(
                f"Non-task steps in branches are not yet supported ({t['type']})"
            )
        handler_name = t.get("handler")
        if not handler_name or handler_name not in handlers:
            raise ValueError(
                f"Handler '{handler_name}' not found for task '{t['id']}'"
            )
        wrapper = _make_task_wrapper(t["id"], handlers[handler_name], t.get("input"))
        wrappers.append(wrapper)
    return wrappers
