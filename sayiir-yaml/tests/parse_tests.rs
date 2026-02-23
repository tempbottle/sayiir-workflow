use sayiir_yaml::{YamlError, parse_workflow};

#[test]
fn test_parse_simple_workflow() {
    let yaml = r#"
id: order-processing
metadata:
  version: "1.0"
tasks:
  - type: task
    id: validate
    handler: validate_order
    input: "input"
  - type: task
    id: process
    handler: process_order
    input: "tasks.validate.output"
"#;

    let def = parse_workflow(yaml).unwrap();
    assert_eq!(def.id, "order-processing");
    assert_eq!(def.tasks.len(), 2);
}

#[test]
fn test_parse_with_delay() {
    let yaml = r#"
id: delayed-workflow
tasks:
  - type: task
    id: step1
    handler: my_handler
  - type: delay
    id: wait_5s
    duration_secs: 5.0
  - type: task
    id: step2
    handler: my_handler
"#;

    let def = parse_workflow(yaml).unwrap();
    assert_eq!(def.tasks.len(), 3);
}

#[test]
fn test_parse_with_signal() {
    let yaml = r#"
id: signal-workflow
tasks:
  - type: task
    id: step1
    handler: my_handler
  - type: wait_for_signal
    id: approval
    signal_name: manager_approval
    timeout_secs: 3600
  - type: task
    id: step2
    handler: my_handler
"#;

    let def = parse_workflow(yaml).unwrap();
    assert_eq!(def.tasks.len(), 3);
}

#[test]
fn test_parse_with_fork() {
    let yaml = r#"
id: parallel-workflow
tasks:
  - type: fork
    id: parallel_ops
    branches:
      - id: branch_a
        tasks:
          - type: task
            id: task_a
            handler: handler_a
      - id: branch_b
        tasks:
          - type: task
            id: task_b
            handler: handler_b
"#;

    let def = parse_workflow(yaml).unwrap();
    assert_eq!(def.tasks.len(), 1);
}

#[test]
fn test_parse_with_branch() {
    let yaml = r#"
id: conditional-workflow
tasks:
  - type: branch
    id: route_order
    branches:
      - when: "tasks.validate.output.amount > `1000`"
        tasks:
          - type: task
            id: high_value
            handler: high_value_handler
      - when: "tasks.validate.output.amount <= `1000`"
        tasks:
          - type: task
            id: low_value
            handler: low_value_handler
    default:
      - type: task
        id: fallback
        handler: fallback_handler
"#;

    let def = parse_workflow(yaml).unwrap();
    assert_eq!(def.tasks.len(), 1);
}

#[test]
fn test_parse_with_loop() {
    let yaml = r#"
id: retry-workflow
tasks:
  - type: loop
    id: retry_loop
    until: "tasks.attempt.output.success"
    max_iterations: 5
    on_max: fail
    body:
      - type: task
        id: attempt
        handler: attempt_handler
"#;

    let def = parse_workflow(yaml).unwrap();
    assert_eq!(def.tasks.len(), 1);
}

#[test]
fn test_parse_with_retry() {
    let yaml = r#"
id: retry-config
tasks:
  - type: task
    id: flaky_task
    handler: my_handler
    retry:
      max_retries: 3
      initial_delay_secs: 1.0
      backoff_multiplier: 2.0
      max_delay_secs: 10.0
"#;

    let def = parse_workflow(yaml).unwrap();
    assert_eq!(def.tasks.len(), 1);
}

#[test]
fn test_parse_with_shell_action() {
    let yaml = r#"
id: shell-workflow
tasks:
  - type: task
    id: run_script
    action:
      type: shell
      command: /bin/echo
      args: ["hello"]
"#;

    let def = parse_workflow(yaml).unwrap();
    assert_eq!(def.tasks.len(), 1);
}

#[test]
fn test_parse_invalid_yaml() {
    let yaml = "not: valid: yaml: {{{}}}";
    let result = parse_workflow(yaml);
    assert!(matches!(result, Err(YamlError::Parse(_))));
}

#[test]
fn test_parse_missing_required_fields() {
    let yaml = r#"
tasks:
  - type: task
    handler: my_handler
"#;
    // Missing 'id' at top level
    let result = parse_workflow(yaml);
    assert!(result.is_err());
}
