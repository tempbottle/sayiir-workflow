use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowDefinition {
    pub id: String,
    #[serde(default)]
    pub metadata: HashMap<String, serde_json::Value>,
    pub tasks: Vec<Step>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Step {
    #[serde(rename = "task")]
    Task(TaskStep),
    #[serde(rename = "delay")]
    Delay(DelayStep),
    #[serde(rename = "wait_for_signal")]
    WaitForSignal(WaitForSignalStep),
    #[serde(rename = "fork")]
    Fork(ForkStep),
    #[serde(rename = "branch")]
    Branch(BranchStep),
    #[serde(rename = "loop")]
    Loop(LoopStep),
    #[serde(rename = "child_workflow")]
    ChildWorkflow(ChildWorkflowStep),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskStep {
    pub id: String,
    /// Name of a user-registered handler. Mutually exclusive with `action`.
    #[serde(default)]
    pub handler: Option<String>,
    /// Built-in action config. Mutually exclusive with `handler`.
    #[serde(default)]
    pub action: Option<ActionConfig>,
    /// `JMESPath` expression evaluated against the context to produce handler input.
    /// If omitted, the previous task's output is passed through.
    #[serde(default)]
    pub input: Option<String>,
    #[serde(default)]
    pub timeout_secs: Option<u64>,
    #[serde(default)]
    pub retry: Option<RetryConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ActionConfig {
    #[serde(rename = "http")]
    Http(HttpConfig),
    #[serde(rename = "shell")]
    Shell(ShellConfig),
    #[cfg(feature = "lambda")]
    #[serde(rename = "lambda")]
    Lambda(LambdaConfig),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpConfig {
    pub method: String,
    pub url: String,
    #[serde(default)]
    pub headers: HashMap<String, String>,
    /// `JMESPath` expression for the request body.
    #[serde(default)]
    pub body: Option<String>,
    #[serde(default)]
    pub timeout_secs: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShellConfig {
    pub command: String,
    #[serde(default)]
    pub args: Vec<String>,
    /// `JMESPath` expression for stdin.
    #[serde(default)]
    pub stdin: Option<String>,
}

#[cfg(feature = "lambda")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LambdaConfig {
    pub function_name: String,
    #[serde(default)]
    pub region: Option<String>,
    /// `JMESPath` expression for the payload.
    #[serde(default)]
    pub payload: Option<String>,
    #[serde(default)]
    pub qualifier: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryConfig {
    pub max_retries: u32,
    #[serde(default = "default_initial_delay_secs")]
    pub initial_delay_secs: f64,
    #[serde(default = "default_backoff_multiplier")]
    pub backoff_multiplier: f32,
    #[serde(default)]
    pub max_delay_secs: Option<f64>,
}

fn default_initial_delay_secs() -> f64 {
    1.0
}

fn default_backoff_multiplier() -> f32 {
    2.0
}

impl RetryConfig {
    pub fn to_retry_policy(&self) -> sayiir_core::task::RetryPolicy {
        sayiir_core::task::RetryPolicy {
            max_retries: self.max_retries,
            initial_delay: std::time::Duration::from_secs_f64(self.initial_delay_secs),
            backoff_multiplier: self.backoff_multiplier,
            max_delay: self.max_delay_secs.map(std::time::Duration::from_secs_f64),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DelayStep {
    pub id: String,
    pub duration_secs: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WaitForSignalStep {
    pub id: String,
    pub signal_name: String,
    #[serde(default)]
    pub timeout_secs: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForkStep {
    pub id: String,
    pub branches: Vec<ForkBranch>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForkBranch {
    pub id: String,
    pub tasks: Vec<Step>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BranchStep {
    pub id: String,
    pub branches: Vec<ConditionalBranch>,
    #[serde(default)]
    pub default: Option<Vec<Step>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConditionalBranch {
    /// `JMESPath` expression evaluated against context; first truthy match wins.
    pub when: String,
    pub tasks: Vec<Step>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoopStep {
    pub id: String,
    #[serde(default)]
    pub for_each: Option<String>,
    #[serde(default)]
    pub until: Option<String>,
    pub body: Vec<Step>,
    #[serde(default = "default_max_iterations")]
    pub max_iterations: u32,
    #[serde(default = "default_on_max")]
    pub on_max: String,
}

fn default_max_iterations() -> u32 {
    1000
}

fn default_on_max() -> String {
    "fail".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChildWorkflowStep {
    pub id: String,
    pub tasks: Vec<Step>,
}
