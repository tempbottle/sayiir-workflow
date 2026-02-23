use serde_json::Value;

/// Evaluate a `JMESPath` expression against a JSON value.
pub fn evaluate(expr: &str, data: &Value) -> Result<Value, String> {
    let compiled =
        jmespath::compile(expr).map_err(|e| format!("invalid JMESPath '{expr}': {e}"))?;
    let json_str =
        serde_json::to_string(data).map_err(|e| format!("failed to serialize data: {e}"))?;
    let variable = jmespath::Variable::from_json(&json_str)
        .map_err(|e| format!("failed to parse as JMESPath variable: {e}"))?;
    let result = compiled
        .search(variable)
        .map_err(|e| format!("JMESPath search failed for '{expr}': {e}"))?;
    let result_json =
        serde_json::to_string(&*result).map_err(|e| format!("failed to serialize result: {e}"))?;
    serde_json::from_str(&result_json).map_err(|e| format!("failed to parse result: {e}"))
}

/// Evaluate a `JMESPath` expression and return whether the result is truthy.
///
/// Truthy: non-null, non-false, non-empty string, non-empty array, non-empty object, any number.
pub fn is_truthy(expr: &str, data: &Value) -> Result<bool, String> {
    let result = evaluate(expr, data)?;
    Ok(value_is_truthy(&result))
}

fn value_is_truthy(v: &Value) -> bool {
    match v {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::String(s) => !s.is_empty(),
        Value::Array(a) => !a.is_empty(),
        Value::Object(o) => !o.is_empty(),
        Value::Number(_) => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_evaluate_simple() {
        let data = json!({"foo": {"bar": 42}});
        let result = evaluate("foo.bar", &data).unwrap();
        assert_eq!(result, json!(42));
    }

    #[test]
    fn test_evaluate_nested() {
        let data = json!({"tasks": {"validate": {"output": {"amount": 100}}}});
        let result = evaluate("tasks.validate.output.amount", &data).unwrap();
        assert_eq!(result, json!(100));
    }

    #[test]
    fn test_is_truthy() {
        let data = json!({"ok": true, "empty": "", "list": [1]});
        assert!(is_truthy("ok", &data).unwrap());
        assert!(!is_truthy("empty", &data).unwrap());
        assert!(is_truthy("list", &data).unwrap());
        assert!(!is_truthy("missing", &data).unwrap());
    }
}
