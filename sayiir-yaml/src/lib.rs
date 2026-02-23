#![deny(clippy::pedantic)]
#![allow(
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::module_name_repetitions
)]

pub mod actions;
pub mod compiler;
pub mod envelope;
pub mod error;
pub mod jmespath;
pub mod schema;

pub use error::YamlError;
pub use schema::WorkflowDefinition;

use sayiir_core::registry::TaskRegistry;
use sayiir_core::workflow::SerializableContinuation;

/// Parse a YAML string into a `WorkflowDefinition`.
pub fn parse_workflow(yaml: &str) -> Result<WorkflowDefinition, YamlError> {
    Ok(serde_yml::from_str(yaml)?)
}

/// Compile a YAML workflow definition into a `SerializableContinuation` and `TaskRegistry`.
///
/// The `user_registry` provides user-defined handler implementations referenced by `handler:` in YAML.
/// Built-in actions (http, shell, lambda) are created automatically by the compiler.
pub fn compile(
    def: &WorkflowDefinition,
    user_registry: &TaskRegistry,
) -> Result<(SerializableContinuation, TaskRegistry), YamlError> {
    compiler::compile(def, user_registry)
}
