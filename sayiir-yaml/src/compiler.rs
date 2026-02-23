use std::collections::HashMap;
use std::sync::Arc;

use crate::envelope::{
    YamlBranchKeyTask, YamlForkJoinTask, YamlLoopWrapperTask, YamlWrapperTask, make_finalize_task,
    make_init_task,
};
use crate::error::YamlError;
use crate::schema::{
    ActionConfig, BranchStep, ChildWorkflowStep, DelayStep, ForkStep, LoopStep, RetryConfig, Step,
    TaskStep, WaitForSignalStep, WorkflowDefinition,
};
use bytes::Bytes;
use sayiir_core::codec::{Decoder, Encoder, sealed};
use sayiir_core::error::BoxError;
use sayiir_core::registry::TaskRegistry;
use sayiir_core::task::{BytesFuture, CoreTask};
use sayiir_core::workflow::{MaxIterationsPolicy, SerializableContinuation};

/// A codec that passes `Bytes` through without any serialization.
/// Used for registering wrapper tasks that already handle their own encoding.
struct BytesPassthroughCodec;

impl Encoder for BytesPassthroughCodec {}
impl Decoder for BytesPassthroughCodec {}

impl sealed::EncodeValue<Bytes> for BytesPassthroughCodec {
    fn encode_value(&self, value: &Bytes) -> Result<Bytes, BoxError> {
        Ok(value.clone())
    }
}

impl sealed::DecodeValue<Bytes> for BytesPassthroughCodec {
    fn decode_value(&self, bytes: Bytes) -> Result<Bytes, BoxError> {
        Ok(bytes)
    }
}

/// Output of compilation.
pub fn compile(
    def: &WorkflowDefinition,
    user_registry: &TaskRegistry,
) -> Result<(SerializableContinuation, TaskRegistry), YamlError> {
    let mut ctx = CompileContext::new(user_registry);
    let body = ctx.compile_steps(&def.tasks, None)?;

    // Build the full chain: init -> body -> finalize
    let init_id = "__yaml::init".to_string();
    let finalize_id = "__yaml::finalize".to_string();

    let passthrough = Arc::new(BytesPassthroughCodec);
    ctx.output_registry
        .register(&init_id, passthrough.clone(), make_init_task_wrapper());
    ctx.output_registry
        .register(&finalize_id, passthrough, make_finalize_task_wrapper());

    let finalize = SerializableContinuation::Task {
        id: finalize_id,
        timeout_ms: None,
        retry_policy: None,
        version: None,
        next: None,
    };

    let chain = append_to_chain(body, finalize);

    let root = SerializableContinuation::Task {
        id: init_id,
        timeout_ms: None,
        retry_policy: None,
        version: None,
        next: Some(Box::new(chain)),
    };

    Ok((root, ctx.output_registry))
}

struct CompileContext<'a> {
    user_registry: &'a TaskRegistry,
    output_registry: TaskRegistry,
}

impl<'a> CompileContext<'a> {
    fn new(user_registry: &'a TaskRegistry) -> Self {
        Self {
            user_registry,
            output_registry: TaskRegistry::new(),
        }
    }

    fn compile_steps(
        &mut self,
        steps: &[Step],
        mut prev_step_id: Option<String>,
    ) -> Result<SerializableContinuation, YamlError> {
        if steps.is_empty() {
            return Err(YamlError::Compile("empty step list".into()));
        }

        let mut nodes: Vec<SerializableContinuation> = Vec::new();

        for step in steps {
            let (node, new_prev) = self.compile_step(step, prev_step_id.as_deref())?;
            nodes.push(node);
            prev_step_id = new_prev;
        }

        // Chain nodes together
        let mut result = nodes.pop().unwrap();
        while let Some(mut node) = nodes.pop() {
            set_next(&mut node, result);
            result = node;
        }

        Ok(result)
    }

    fn compile_step(
        &mut self,
        step: &Step,
        prev_step_id: Option<&str>,
    ) -> Result<(SerializableContinuation, Option<String>), YamlError> {
        match step {
            Step::Task(task) => self.compile_task(task, prev_step_id),
            Step::Delay(delay) => Ok(Self::compile_delay(delay)),
            Step::WaitForSignal(signal) => Ok(Self::compile_signal(signal)),
            Step::Fork(fork) => self.compile_fork(fork, prev_step_id),
            Step::Branch(branch) => self.compile_branch(branch, prev_step_id),
            Step::Loop(loop_step) => self.compile_loop(loop_step, prev_step_id),
            Step::ChildWorkflow(child) => self.compile_child_workflow(child, prev_step_id),
        }
    }

    fn compile_task(
        &mut self,
        task: &TaskStep,
        prev_step_id: Option<&str>,
    ) -> Result<(SerializableContinuation, Option<String>), YamlError> {
        // Resolve the handler
        let handler: Box<
            dyn CoreTask<Input = bytes::Bytes, Output = bytes::Bytes, Future = BytesFuture>
                + Send
                + Sync,
        > = if let Some(handler_name) = &task.handler {
            self.user_registry
                .get(handler_name)
                .ok_or_else(|| YamlError::MissingHandler(handler_name.clone()))?
        } else if let Some(action) = &task.action {
            Self::make_action_handler(action)?
        } else {
            return Err(YamlError::Compile(format!(
                "task '{}' must have either 'handler' or 'action'",
                task.id
            )));
        };

        let wrapper_id = format!("__yaml::wrap::{}", task.id);
        let handler_arc: Arc<
            dyn CoreTask<Input = bytes::Bytes, Output = bytes::Bytes, Future = BytesFuture>
                + Send
                + Sync,
        > = Arc::from(handler);

        let wrapper = YamlWrapperTask {
            step_id: task.id.clone(),
            prev_step_id: prev_step_id.map(String::from),
            input_expr: task.input.clone(),
            handler: handler_arc,
        };

        let codec = Arc::new(BytesPassthroughCodec);
        self.output_registry.register(&wrapper_id, codec, wrapper);

        let node = SerializableContinuation::Task {
            id: wrapper_id,
            timeout_ms: task.timeout_secs.map(|s| s * 1000),
            retry_policy: task.retry.as_ref().map(RetryConfig::to_retry_policy),
            version: None,
            next: None,
        };

        Ok((node, Some(task.id.clone())))
    }

    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    fn compile_delay(delay: &DelayStep) -> (SerializableContinuation, Option<String>) {
        let node = SerializableContinuation::Delay {
            id: delay.id.clone(),
            duration_ms: (delay.duration_secs * 1000.0) as u64,
            next: None,
        };
        (node, None)
    }

    fn compile_signal(signal: &WaitForSignalStep) -> (SerializableContinuation, Option<String>) {
        let node = SerializableContinuation::AwaitSignal {
            id: signal.id.clone(),
            signal_name: signal.signal_name.clone(),
            timeout_ms: signal.timeout_secs.map(|s| s * 1000),
            next: None,
        };
        (node, None)
    }

    fn compile_fork(
        &mut self,
        fork: &ForkStep,
        prev_step_id: Option<&str>,
    ) -> Result<(SerializableContinuation, Option<String>), YamlError> {
        if fork.branches.is_empty() {
            return Err(YamlError::Compile(
                "fork must have at least one branch".into(),
            ));
        }

        let mut compiled_branches = Vec::new();
        let branch_ids: Vec<String> = fork.branches.iter().map(|b| b.id.clone()).collect();

        for branch in &fork.branches {
            let branch_body = self.compile_steps(&branch.tasks, prev_step_id.map(String::from))?;
            compiled_branches.push(branch_body);
        }

        // Create join task that merges contexts
        let join_id = format!("__yaml::join::{}", fork.id);
        let join_task = YamlForkJoinTask {
            branch_ids: branch_ids.clone(),
        };
        self.output_registry
            .register(&join_id, Arc::new(BytesPassthroughCodec), join_task);

        let join = SerializableContinuation::Task {
            id: join_id.clone(),
            timeout_ms: None,
            retry_policy: None,
            version: None,
            next: None,
        };

        let node = SerializableContinuation::Fork {
            id: fork.id.clone(),
            branches: compiled_branches,
            join: Some(Box::new(join)),
        };

        Ok((node, Some(join_id)))
    }

    fn compile_branch(
        &mut self,
        branch: &BranchStep,
        prev_step_id: Option<&str>,
    ) -> Result<(SerializableContinuation, Option<String>), YamlError> {
        if branch.branches.is_empty() {
            return Err(YamlError::Compile(
                "branch must have at least one condition".into(),
            ));
        }

        // The core's to_runnable looks up key_fn by convention: "{branch_id}::key_fn"
        let kf_id = sayiir_core::workflow::key_fn_id(&branch.id);
        let conditions: Vec<String> = branch.branches.iter().map(|b| b.when.clone()).collect();
        let key_task = YamlBranchKeyTask { conditions };
        self.output_registry
            .register(&kf_id, Arc::new(BytesPassthroughCodec), key_task);

        // Compile branches keyed by index
        let mut branch_map: HashMap<String, Box<SerializableContinuation>> = HashMap::new();
        for (i, cond_branch) in branch.branches.iter().enumerate() {
            let body = self.compile_steps(&cond_branch.tasks, prev_step_id.map(String::from))?;
            branch_map.insert(i.to_string(), Box::new(body));
        }

        let default = if let Some(default_steps) = &branch.default {
            Some(Box::new(self.compile_steps(
                default_steps,
                prev_step_id.map(String::from),
            )?))
        } else {
            None
        };

        let branch_node = SerializableContinuation::Branch {
            id: branch.id.clone(),
            branches: branch_map,
            default,
            next: None,
        };

        Ok((branch_node, None))
    }

    fn compile_loop(
        &mut self,
        loop_step: &LoopStep,
        prev_step_id: Option<&str>,
    ) -> Result<(SerializableContinuation, Option<String>), YamlError> {
        let on_max: MaxIterationsPolicy = loop_step
            .on_max
            .parse()
            .map_err(|_| YamlError::Compile(format!("invalid on_max: '{}'", loop_step.on_max)))?;

        // For the loop body, the last task needs to be a loop-aware wrapper
        if loop_step.body.is_empty() {
            return Err(YamlError::Compile(
                "loop body must have at least one step".into(),
            ));
        }

        // Compile all body steps except the last as regular tasks
        let body_steps = &loop_step.body;
        if body_steps.len() == 1 {
            // Single task in loop body — make it loop-aware
            let body_node = self.compile_loop_body_task(&body_steps[0], prev_step_id, loop_step)?;

            let node = SerializableContinuation::Loop {
                id: loop_step.id.clone(),
                body: Box::new(body_node),
                max_iterations: loop_step.max_iterations,
                on_max,
                next: None,
            };

            Ok((node, Some(loop_step.id.clone())))
        } else {
            // Multiple tasks: all but last are regular wrappers, last is loop-aware
            let mut body_nodes: Vec<SerializableContinuation> = Vec::new();
            let mut current_prev: Option<String> = prev_step_id.map(String::from);

            for (i, step) in body_steps.iter().enumerate() {
                if i == body_steps.len() - 1 {
                    // Last step: loop-aware
                    let body_node =
                        self.compile_loop_body_task(step, current_prev.as_deref(), loop_step)?;
                    body_nodes.push(body_node);
                } else {
                    let (node, new_prev) = self.compile_step(step, current_prev.as_deref())?;
                    body_nodes.push(node);
                    current_prev = new_prev;
                }
            }

            // Chain body nodes
            let mut body = body_nodes.pop().unwrap();
            while let Some(mut node) = body_nodes.pop() {
                set_next(&mut node, body);
                body = node;
            }

            let node = SerializableContinuation::Loop {
                id: loop_step.id.clone(),
                body: Box::new(body),
                max_iterations: loop_step.max_iterations,
                on_max,
                next: None,
            };

            Ok((node, Some(loop_step.id.clone())))
        }
    }

    fn compile_loop_body_task(
        &mut self,
        step: &Step,
        prev_step_id: Option<&str>,
        loop_step: &LoopStep,
    ) -> Result<SerializableContinuation, YamlError> {
        let Step::Task(task) = step else {
            return Err(YamlError::Compile(
                "loop body's last step must be a task".into(),
            ));
        };

        let handler: Box<
            dyn CoreTask<Input = bytes::Bytes, Output = bytes::Bytes, Future = BytesFuture>
                + Send
                + Sync,
        > = if let Some(handler_name) = &task.handler {
            self.user_registry
                .get(handler_name)
                .ok_or_else(|| YamlError::MissingHandler(handler_name.clone()))?
        } else if let Some(action) = &task.action {
            Self::make_action_handler(action)?
        } else {
            return Err(YamlError::Compile(format!(
                "task '{}' must have either 'handler' or 'action'",
                task.id
            )));
        };

        let wrapper_id = format!("__yaml::loop_wrap::{}", task.id);
        let handler_arc: Arc<
            dyn CoreTask<Input = bytes::Bytes, Output = bytes::Bytes, Future = BytesFuture>
                + Send
                + Sync,
        > = Arc::from(handler);

        let wrapper = YamlLoopWrapperTask {
            step_id: task.id.clone(),
            prev_step_id: prev_step_id.map(String::from),
            input_expr: task.input.clone(),
            handler: handler_arc,
            for_each_expr: loop_step.for_each.clone(),
            until_expr: loop_step.until.clone(),
        };

        let codec = Arc::new(BytesPassthroughCodec);
        self.output_registry.register(&wrapper_id, codec, wrapper);

        Ok(SerializableContinuation::Task {
            id: wrapper_id,
            timeout_ms: task.timeout_secs.map(|s| s * 1000),
            retry_policy: task.retry.as_ref().map(RetryConfig::to_retry_policy),
            version: None,
            next: None,
        })
    }

    fn compile_child_workflow(
        &mut self,
        child: &ChildWorkflowStep,
        prev_step_id: Option<&str>,
    ) -> Result<(SerializableContinuation, Option<String>), YamlError> {
        let child_body = self.compile_steps(&child.tasks, prev_step_id.map(String::from))?;
        let node = SerializableContinuation::ChildWorkflow {
            id: child.id.clone(),
            child: Box::new(child_body),
            next: None,
        };
        Ok((node, Some(child.id.clone())))
    }

    #[allow(clippy::unnecessary_wraps)] // Err arm present when http feature is disabled
    fn make_action_handler(
        action: &ActionConfig,
    ) -> Result<
        Box<
            dyn CoreTask<Input = bytes::Bytes, Output = bytes::Bytes, Future = BytesFuture>
                + Send
                + Sync,
        >,
        YamlError,
    > {
        match action {
            ActionConfig::Shell(config) => Ok(Box::new(crate::actions::shell::ShellAction {
                command: config.command.clone(),
                args: config.args.clone(),
            })),
            #[cfg(feature = "http")]
            ActionConfig::Http(config) => Ok(Box::new(crate::actions::http::HttpAction {
                method: config.method.clone(),
                url: config.url.clone(),
                headers: config.headers.clone(),
                timeout_secs: config.timeout_secs,
            })),
            #[cfg(feature = "lambda")]
            ActionConfig::Lambda(config) => Ok(Box::new(crate::actions::lambda::LambdaAction {
                function_name: config.function_name.clone(),
                region: config.region.clone(),
                qualifier: config.qualifier.clone(),
            })),
            #[cfg(not(feature = "http"))]
            ActionConfig::Http(_) => Err(YamlError::Compile(
                "HTTP action requires the 'http' feature".into(),
            )),
        }
    }
}

/// Helper: set the `next` field of a `SerializableContinuation` node.
fn set_next(node: &mut SerializableContinuation, next_node: SerializableContinuation) {
    match node {
        SerializableContinuation::Task { next, .. }
        | SerializableContinuation::Delay { next, .. }
        | SerializableContinuation::AwaitSignal { next, .. }
        | SerializableContinuation::Branch { next, .. }
        | SerializableContinuation::Loop { next, .. }
        | SerializableContinuation::ChildWorkflow { next, .. } => {
            *next = Some(Box::new(next_node));
        }
        SerializableContinuation::Fork { join, .. } => {
            if let Some(join_node) = join {
                set_next(join_node, next_node);
            } else {
                *join = Some(Box::new(next_node));
            }
        }
    }
}

/// Helper: append a node to the end of a chain.
fn append_to_chain(
    mut chain: SerializableContinuation,
    new_node: SerializableContinuation,
) -> SerializableContinuation {
    fn get_tail(node: &mut SerializableContinuation) -> &mut Option<Box<SerializableContinuation>> {
        match node {
            SerializableContinuation::Task { next, .. }
            | SerializableContinuation::Delay { next, .. }
            | SerializableContinuation::AwaitSignal { next, .. }
            | SerializableContinuation::Branch { next, .. }
            | SerializableContinuation::Loop { next, .. }
            | SerializableContinuation::ChildWorkflow { next, .. } => {
                if next.is_some() {
                    get_tail(next.as_mut().unwrap())
                } else {
                    next
                }
            }
            SerializableContinuation::Fork { join, .. } => {
                if let Some(join_node) = join {
                    get_tail(join_node)
                } else {
                    join
                }
            }
        }
    }

    let tail = get_tail(&mut chain);
    *tail = Some(Box::new(new_node));
    chain
}

/// Newtype wrapper so we can register init task with the registry.
struct InitTaskWrapper;

impl CoreTask for InitTaskWrapper {
    type Input = bytes::Bytes;
    type Output = bytes::Bytes;
    type Future = BytesFuture;

    fn run(&self, input: bytes::Bytes) -> Self::Future {
        let inner = make_init_task();
        inner.run(input)
    }
}

fn make_init_task_wrapper() -> InitTaskWrapper {
    InitTaskWrapper
}

struct FinalizeTaskWrapper;

impl CoreTask for FinalizeTaskWrapper {
    type Input = bytes::Bytes;
    type Output = bytes::Bytes;
    type Future = BytesFuture;

    fn run(&self, input: bytes::Bytes) -> Self::Future {
        let inner = make_finalize_task();
        inner.run(input)
    }
}

fn make_finalize_task_wrapper() -> FinalizeTaskWrapper {
    FinalizeTaskWrapper
}
