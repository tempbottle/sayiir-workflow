//! YAML workflow support — parse definitions and evaluate `JMESPath` expressions.

use napi::bindgen_prelude::*;
use napi_derive::napi;

/// Parse a YAML workflow definition string and return it as a JS object.
///
/// The Rust parser validates the schema structure; invalid YAML or missing
/// required fields produce an error.
#[napi]
pub fn parse_yaml_workflow(yaml_str: String) -> Result<serde_json::Value> {
    let def = sayiir_yaml::parse_workflow(&yaml_str)
        .map_err(|e| Error::new(Status::InvalidArg, e.to_string()))?;
    serde_json::to_value(&def).map_err(|e| Error::new(Status::GenericFailure, e.to_string()))
}

/// Evaluate a `JMESPath` expression against a JS value.
///
/// Returns the result of the expression. Throws on invalid expressions
/// or evaluation errors.
#[napi]
pub fn eval_jmespath(expr: String, data: serde_json::Value) -> Result<serde_json::Value> {
    sayiir_yaml::jmespath::evaluate(&expr, &data).map_err(|e| Error::new(Status::InvalidArg, e))
}

/// Check if a `JMESPath` expression evaluates to a truthy value.
#[napi]
pub fn eval_jmespath_truthy(expr: String, data: serde_json::Value) -> Result<bool> {
    sayiir_yaml::jmespath::is_truthy(&expr, &data).map_err(|e| Error::new(Status::InvalidArg, e))
}
