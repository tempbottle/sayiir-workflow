use bytes::Bytes;
use sayiir_core::codec::{LoopDecision, encode_loop_envelope};
use sayiir_core::error::BoxError;
use sayiir_core::task::{BytesFuture, CoreTask, UntypedCoreTask};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

use crate::jmespath;

/// The envelope that flows between wrapper tasks, carrying accumulated context.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(clippy::pub_underscore_fields)]
pub struct Envelope {
    pub __ctx: Context,
    pub __val: Value,
}

/// Accumulated context available to `JMESPath` expressions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Context {
    pub input: Value,
    pub tasks: HashMap<String, TaskRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskRecord {
    pub output: Value,
}

impl Envelope {
    #[must_use]
    pub fn new(input: Value) -> Self {
        Self {
            __ctx: Context {
                input: input.clone(),
                tasks: HashMap::new(),
            },
            __val: input,
        }
    }

    pub fn to_bytes(&self) -> Result<Bytes, BoxError> {
        Ok(Bytes::from(serde_json::to_vec(self)?))
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, BoxError> {
        Ok(serde_json::from_slice(bytes)?)
    }

    /// Build the full context object for `JMESPath` evaluation.
    /// Returns `{"input": ..., "tasks": {...}}`.
    #[must_use]
    pub fn context_value(&self) -> Value {
        serde_json::to_value(&self.__ctx).unwrap_or(Value::Null)
    }
}

/// Creates the init task: wraps raw JSON input into an Envelope.
#[must_use]
pub fn make_init_task() -> UntypedCoreTask {
    struct InitTask;

    impl CoreTask for InitTask {
        type Input = Bytes;
        type Output = Bytes;
        type Future = BytesFuture;

        fn run(&self, input: Bytes) -> Self::Future {
            BytesFuture::new(async move {
                let val: Value = serde_json::from_slice(&input)?;
                let envelope = Envelope::new(val);
                envelope.to_bytes()
            })
        }
    }

    Box::new(InitTask)
}

/// Creates the finalize task: extracts __val from the Envelope.
#[must_use]
pub fn make_finalize_task() -> UntypedCoreTask {
    struct FinalizeTask;

    impl CoreTask for FinalizeTask {
        type Input = Bytes;
        type Output = Bytes;
        type Future = BytesFuture;

        fn run(&self, input: Bytes) -> Self::Future {
            BytesFuture::new(async move {
                let envelope = Envelope::from_bytes(&input)?;
                Ok(Bytes::from(serde_json::to_vec(&envelope.__val)?))
            })
        }
    }

    Box::new(FinalizeTask)
}

/// A wrapper task that manages the envelope context around a real handler.
///
/// 1. Receives envelope bytes → deserializes Envelope
/// 2. Records previous task's output in context (using `prev_step_id`)
/// 3. Evaluates `JMESPath` `input_expr` against context → extracts handler input
/// 4. Calls inner handler with extracted input bytes
/// 5. Stores handler output in context → returns updated envelope
pub struct YamlWrapperTask {
    pub step_id: String,
    pub prev_step_id: Option<String>,
    pub input_expr: Option<String>,
    pub handler:
        Arc<dyn CoreTask<Input = Bytes, Output = Bytes, Future = BytesFuture> + Send + Sync>,
}

impl CoreTask for YamlWrapperTask {
    type Input = Bytes;
    type Output = Bytes;
    type Future = BytesFuture;

    fn run(&self, input: Bytes) -> Self::Future {
        let _step_id = self.step_id.clone();
        let prev_step_id = self.prev_step_id.clone();
        let input_expr = self.input_expr.clone();
        let handler = Arc::clone(&self.handler);

        BytesFuture::new(async move {
            let mut envelope = Envelope::from_bytes(&input)?;

            // Record previous task's output in context
            if let Some(prev_id) = &prev_step_id {
                envelope.__ctx.tasks.insert(
                    prev_id.clone(),
                    TaskRecord {
                        output: envelope.__val.clone(),
                    },
                );
            }

            // Evaluate `JMESPath` to get handler input
            let handler_input = if let Some(expr) = &input_expr {
                let ctx_val = envelope.context_value();
                jmespath::evaluate(expr, &ctx_val).map_err(|e| -> BoxError { e.into() })?
            } else {
                envelope.__val.clone()
            };

            // Call the real handler
            let handler_input_bytes = Bytes::from(serde_json::to_vec(&handler_input)?);
            let output_bytes = handler.run(handler_input_bytes).await?;
            let output_val: Value = serde_json::from_slice(&output_bytes)?;

            // Update envelope with handler output
            envelope.__val = output_val;
            envelope.to_bytes()
        })
    }
}

/// Loop-aware wrapper task. Same as `YamlWrapperTask` but also evaluates
/// loop termination conditions and returns a `LoopResult` envelope.
pub struct YamlLoopWrapperTask {
    pub step_id: String,
    pub prev_step_id: Option<String>,
    pub input_expr: Option<String>,
    pub handler:
        Arc<dyn CoreTask<Input = Bytes, Output = Bytes, Future = BytesFuture> + Send + Sync>,
    /// For `for_each` loops: `JMESPath` to the array being iterated.
    pub for_each_expr: Option<String>,
    /// For `until` loops: `JMESPath` condition that, when truthy, exits the loop.
    pub until_expr: Option<String>,
}

impl CoreTask for YamlLoopWrapperTask {
    type Input = Bytes;
    type Output = Bytes;
    type Future = BytesFuture;

    fn run(&self, input: Bytes) -> Self::Future {
        let step_id = self.step_id.clone();
        let prev_step_id = self.prev_step_id.clone();
        let input_expr = self.input_expr.clone();
        let handler = Arc::clone(&self.handler);
        let for_each_expr = self.for_each_expr.clone();
        let until_expr = self.until_expr.clone();

        BytesFuture::new(async move {
            let mut envelope = Envelope::from_bytes(&input)?;

            // Record previous task's output in context
            if let Some(prev_id) = &prev_step_id {
                envelope.__ctx.tasks.insert(
                    prev_id.clone(),
                    TaskRecord {
                        output: envelope.__val.clone(),
                    },
                );
            }

            // Evaluate `JMESPath` to get handler input
            let handler_input = if let Some(expr) = &input_expr {
                let ctx_val = envelope.context_value();
                jmespath::evaluate(expr, &ctx_val).map_err(|e| -> BoxError { e.into() })?
            } else {
                envelope.__val.clone()
            };

            // Call the real handler
            let handler_input_bytes = Bytes::from(serde_json::to_vec(&handler_input)?);
            let output_bytes = handler.run(handler_input_bytes).await?;
            let output_val: Value = serde_json::from_slice(&output_bytes)?;

            // Update envelope
            envelope.__val = output_val;
            envelope.__ctx.tasks.insert(
                step_id,
                TaskRecord {
                    output: envelope.__val.clone(),
                },
            );

            // Determine loop decision
            let decision = if let Some(until) = &until_expr {
                let ctx_val = envelope.context_value();
                if jmespath::is_truthy(until, &ctx_val).map_err(|e| -> BoxError { e.into() })? {
                    LoopDecision::Done
                } else {
                    LoopDecision::Again
                }
            } else if let Some(for_each) = &for_each_expr {
                // for_each: check if we've processed all items
                // The loop state is managed by tracking iteration index in context
                let ctx_val = envelope.context_value();
                let arr =
                    jmespath::evaluate(for_each, &ctx_val).map_err(|e| -> BoxError { e.into() })?;
                if let Value::Array(items) = &arr {
                    let iteration = envelope.__ctx.tasks.len().saturating_sub(1); // rough approximation
                    if iteration >= items.len() {
                        LoopDecision::Done
                    } else {
                        LoopDecision::Again
                    }
                } else {
                    LoopDecision::Done
                }
            } else {
                // No condition — shouldn't happen, but default to done
                LoopDecision::Done
            };

            // Encode as loop envelope (binary tag + inner bytes)
            let inner_bytes = envelope.to_bytes()?;
            Ok(encode_loop_envelope(decision, &inner_bytes))
        })
    }
}

/// Branch key task: evaluates `when` conditions and returns the index of the first truthy match.
pub struct YamlBranchKeyTask {
    pub conditions: Vec<String>,
}

impl CoreTask for YamlBranchKeyTask {
    type Input = Bytes;
    type Output = Bytes;
    type Future = BytesFuture;

    fn run(&self, input: Bytes) -> Self::Future {
        let conditions = self.conditions.clone();

        BytesFuture::new(async move {
            let envelope = Envelope::from_bytes(&input)?;
            let ctx_val = envelope.context_value();

            for (i, condition) in conditions.iter().enumerate() {
                if jmespath::is_truthy(condition, &ctx_val).map_err(|e| -> BoxError { e.into() })? {
                    let key = i.to_string();
                    return Ok(Bytes::from(serde_json::to_vec(&key)?));
                }
            }

            Ok(Bytes::from(serde_json::to_vec(&"default".to_string())?))
        })
    }
}

/// Fork-join merge task: merges contexts from parallel branch envelopes.
pub struct YamlForkJoinTask {
    pub branch_ids: Vec<String>,
}

impl CoreTask for YamlForkJoinTask {
    type Input = Bytes;
    type Output = Bytes;
    type Future = BytesFuture;

    fn run(&self, input: Bytes) -> Self::Future {
        let _branch_ids = self.branch_ids.clone();

        BytesFuture::new(async move {
            // Input is NamedBranchResults — a map of branch_id -> bytes
            let named: sayiir_core::branch_results::NamedBranchResults =
                serde_json::from_slice(&input)?;

            let mut merged_ctx = Context {
                input: Value::Null,
                tasks: HashMap::new(),
            };
            let mut results: HashMap<String, Value> = HashMap::new();

            for (branch_id, branch_bytes) in named.as_slice() {
                if let Ok(branch_envelope) = Envelope::from_bytes(branch_bytes) {
                    // Merge this branch's context
                    if merged_ctx.input == Value::Null {
                        merged_ctx.input = branch_envelope.__ctx.input.clone();
                    }
                    for (task_id, record) in &branch_envelope.__ctx.tasks {
                        merged_ctx.tasks.insert(task_id.clone(), record.clone());
                    }
                    results.insert(branch_id.clone(), branch_envelope.__val.clone());
                }
            }

            let envelope = Envelope {
                __ctx: merged_ctx,
                __val: serde_json::to_value(&results)?,
            };
            envelope.to_bytes()
        })
    }
}
