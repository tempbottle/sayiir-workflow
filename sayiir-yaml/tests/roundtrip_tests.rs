use bytes::Bytes;
use sayiir_core::registry::TaskRegistry;
use sayiir_runtime::execute_continuation_async;
use sayiir_runtime::serialization::JsonCodec;
use std::sync::Arc;

fn json_bytes(val: &serde_json::Value) -> Bytes {
    Bytes::from(serde_json::to_vec(val).unwrap())
}

fn from_json_bytes<T: serde::de::DeserializeOwned>(bytes: &Bytes) -> T {
    serde_json::from_slice(bytes).unwrap()
}

#[tokio::test]
async fn test_linear_pipeline() {
    let yaml = r#"
id: linear
tasks:
  - type: task
    id: double
    handler: double
  - type: task
    id: add_ten
    handler: add_ten
"#;

    // Build user registry
    let codec = Arc::new(JsonCodec);
    let mut user_registry = TaskRegistry::new();
    user_registry.register_fn(
        "double",
        codec.clone(),
        |input: serde_json::Value| async move {
            let n = input.as_i64().unwrap_or(0);
            Ok(serde_json::json!(n * 2))
        },
    );
    user_registry.register_fn(
        "add_ten",
        codec.clone(),
        |input: serde_json::Value| async move {
            let n = input.as_i64().unwrap_or(0);
            Ok(serde_json::json!(n + 10))
        },
    );

    // Parse and compile
    let def = sayiir_yaml::parse_workflow(yaml).unwrap();
    let (continuation, mut yaml_registry) = sayiir_yaml::compile(&def, &user_registry).unwrap();

    // Merge registries so to_runnable can find all tasks
    yaml_registry.merge(user_registry);

    // Convert to runnable
    let runnable = continuation.to_runnable(&yaml_registry).unwrap();

    // Execute
    let input = json_bytes(&serde_json::json!(5));
    let result = execute_continuation_async(&runnable, input, &JsonCodec)
        .await
        .unwrap();
    let output: serde_json::Value = from_json_bytes(&result);

    // 5 -> double -> 10 -> add_ten -> 20
    assert_eq!(output, serde_json::json!(20));
}

#[tokio::test]
async fn test_shell_action() {
    let yaml = r#"
id: shell-test
tasks:
  - type: task
    id: echo_hello
    action:
      type: shell
      command: /bin/echo
      args: ["hello world"]
"#;

    let user_registry = TaskRegistry::new();
    let def = sayiir_yaml::parse_workflow(yaml).unwrap();
    let (continuation, yaml_registry) = sayiir_yaml::compile(&def, &user_registry).unwrap();

    let runnable = continuation.to_runnable(&yaml_registry).unwrap();

    let input = json_bytes(&serde_json::json!(""));
    let result = execute_continuation_async(&runnable, input, &JsonCodec)
        .await
        .unwrap();
    let output: serde_json::Value = from_json_bytes(&result);

    // Shell action returns {exit_code, stdout, stderr}
    assert_eq!(output["exit_code"], 0);
    assert!(output["stdout"].as_str().unwrap().contains("hello world"));
}

#[tokio::test]
async fn test_jmespath_input() {
    let yaml = r#"
id: jmespath-test
tasks:
  - type: task
    id: extract
    handler: extract
    input: "input.name"
"#;

    let codec = Arc::new(JsonCodec);
    let mut user_registry = TaskRegistry::new();
    user_registry.register_fn(
        "extract",
        codec.clone(),
        |input: serde_json::Value| async move {
            // Just pass through — the JMESPath should have extracted "name" from the input
            Ok(input)
        },
    );

    let def = sayiir_yaml::parse_workflow(yaml).unwrap();
    let (continuation, mut yaml_registry) = sayiir_yaml::compile(&def, &user_registry).unwrap();
    yaml_registry.merge(user_registry);

    let runnable = continuation.to_runnable(&yaml_registry).unwrap();

    let input = json_bytes(&serde_json::json!({"name": "Alice", "age": 30}));
    let result = execute_continuation_async(&runnable, input, &JsonCodec)
        .await
        .unwrap();
    let output: serde_json::Value = from_json_bytes(&result);

    assert_eq!(output, serde_json::json!("Alice"));
}

#[tokio::test]
async fn test_context_references() {
    let yaml = r#"
id: context-test
tasks:
  - type: task
    id: step1
    handler: identity
  - type: task
    id: step2
    handler: identity
    input: "tasks.step1.output"
"#;

    let codec = Arc::new(JsonCodec);
    let mut user_registry = TaskRegistry::new();
    user_registry.register_fn(
        "identity",
        codec.clone(),
        |input: serde_json::Value| async move { Ok(input) },
    );

    let def = sayiir_yaml::parse_workflow(yaml).unwrap();
    let (continuation, mut yaml_registry) = sayiir_yaml::compile(&def, &user_registry).unwrap();
    yaml_registry.merge(user_registry);

    let runnable = continuation.to_runnable(&yaml_registry).unwrap();

    let input = json_bytes(&serde_json::json!({"value": 42}));
    let result = execute_continuation_async(&runnable, input, &JsonCodec)
        .await
        .unwrap();
    let output: serde_json::Value = from_json_bytes(&result);

    // step1 receives {"value": 42}, step2 references step1's output
    assert_eq!(output, serde_json::json!({"value": 42}));
}

#[tokio::test]
async fn test_missing_handler_error() {
    let yaml = r#"
id: missing-handler
tasks:
  - type: task
    id: step1
    handler: nonexistent
"#;

    let user_registry = TaskRegistry::new();
    let def = sayiir_yaml::parse_workflow(yaml).unwrap();
    let result = sayiir_yaml::compile(&def, &user_registry);
    assert!(result.is_err());
    assert!(matches!(
        result,
        Err(sayiir_yaml::YamlError::MissingHandler(_))
    ));
}

#[tokio::test]
async fn test_no_handler_or_action_error() {
    let yaml = r#"
id: no-handler
tasks:
  - type: task
    id: step1
"#;

    let user_registry = TaskRegistry::new();
    let def = sayiir_yaml::parse_workflow(yaml).unwrap();
    let result = sayiir_yaml::compile(&def, &user_registry);
    assert!(result.is_err());
    assert!(matches!(result, Err(sayiir_yaml::YamlError::Compile(_))));
}
