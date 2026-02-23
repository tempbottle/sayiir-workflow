//! YAML workflow support — parse definitions and evaluate `JMESPath` expressions.

use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::PyString;

/// Parse a YAML workflow definition string and return it as a Python dict.
///
/// The Rust parser validates the schema structure; invalid YAML or missing
/// required fields produce a `ValueError`.
#[pyfunction]
pub fn parse_yaml_workflow(py: Python<'_>, yaml_str: &str) -> PyResult<Py<PyAny>> {
    let def = sayiir_yaml::parse_workflow(yaml_str)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;
    let json_str = serde_json::to_string(&def)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;
    let json_mod = py.import(intern!(py, "json"))?;
    Ok(json_mod
        .call_method1(intern!(py, "loads"), (json_str,))?
        .unbind())
}

/// Evaluate a `JMESPath` expression against a Python dict/value.
///
/// Returns the result of the expression. Raises `ValueError` on invalid
/// expressions or evaluation errors.
#[pyfunction]
pub fn eval_jmespath(py: Python<'_>, expr: &str, data: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    let json_mod = py.import(intern!(py, "json"))?;
    let json_str: String = json_mod
        .call_method1(intern!(py, "dumps"), (data,))?
        .cast::<PyString>()?
        .extract()?;
    let json_data: serde_json::Value = serde_json::from_str(&json_str)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;
    let result = sayiir_yaml::jmespath::evaluate(expr, &json_data)
        .map_err(PyErr::new::<pyo3::exceptions::PyValueError, _>)?;
    let result_str = serde_json::to_string(&result)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;
    Ok(json_mod
        .call_method1(intern!(py, "loads"), (result_str,))?
        .unbind())
}

/// Check if a `JMESPath` expression evaluates to a truthy value.
#[pyfunction]
pub fn eval_jmespath_truthy(py: Python<'_>, expr: &str, data: &Bound<'_, PyAny>) -> PyResult<bool> {
    let json_mod = py.import(intern!(py, "json"))?;
    let json_str: String = json_mod
        .call_method1(intern!(py, "dumps"), (data,))?
        .cast::<PyString>()?
        .extract()?;
    let json_data: serde_json::Value = serde_json::from_str(&json_str)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;
    sayiir_yaml::jmespath::is_truthy(expr, &json_data)
        .map_err(PyErr::new::<pyo3::exceptions::PyValueError, _>)
}
