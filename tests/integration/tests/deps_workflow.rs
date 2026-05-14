#![allow(clippy::unwrap_used, clippy::expect_used)]

//! Tests for the `workflow! { deps: ... }` field and the underlying
//! `Deps` / `from_deps` / `verify_deps` machinery.

use sayiir_core::deps::Deps;
use sayiir_core::error::{BoxError, BuildError, BuildErrors};
use sayiir_core::task::CoreTask;
use sayiir_core::workflow::SerializableWorkflow;
use sayiir_macros::{task, workflow};
use sayiir_runtime::InProcessRunner;
use sayiir_runtime::WorkflowRunner;
use sayiir_runtime::serialization::JsonCodec;
use std::sync::Arc;

type BuildResult = Result<SerializableWorkflow<JsonCodec, u32, ()>, BuildErrors>;

// ─── Fixtures ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct HttpClient {
    base_url: String,
}

#[derive(Debug, Clone)]
struct Counter {
    factor: u32,
}

#[task(id = "deps_fetch")]
async fn deps_fetch(input: u32, #[inject] client: Arc<HttpClient>) -> Result<u32, BoxError> {
    // Use the injected client (only to prove the dep was wired).
    Ok(input + client.base_url.len() as u32)
}

#[task(id = "deps_multiply")]
async fn deps_multiply(input: u32, #[inject] counter: Arc<Counter>) -> Result<u32, BoxError> {
    Ok(input * counter.factor)
}

#[task(id = "deps_plain")]
async fn deps_plain(input: u32) -> Result<u32, BoxError> {
    Ok(input + 1)
}

// ─── 1. from_deps / verify_deps are generated for every task ────────────────

#[test]
fn no_inject_task_from_deps_returns_default() {
    let deps = Deps::new();
    // Compiles and runs even when Deps is empty.
    let _t = DepsPlainTask::from_deps(&deps);
    assert!(DepsPlainTask::verify_deps(&deps).is_empty());
}

#[test]
fn inject_task_from_deps_resolves_arc() {
    let client = Arc::new(HttpClient {
        base_url: "https://api.example.com".to_string(),
    });
    let deps = Deps::builder().insert(client.clone()).build();

    let task = DepsFetchTask::from_deps(&deps);
    let fut = task.run(10u32);
    let result = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(fut)
        .unwrap();
    assert_eq!(result, 10 + client.base_url.len() as u32);
}

#[test]
fn verify_deps_reports_missing_arc() {
    let deps = Deps::new();
    let missing = DepsFetchTask::verify_deps(&deps);
    assert_eq!(missing.len(), 1);
    assert!(missing[0].type_name.contains("HttpClient"));
}

#[test]
fn verify_deps_passes_when_all_present() {
    let deps = Deps::builder()
        .insert(Arc::new(HttpClient {
            base_url: "x".into(),
        }))
        .build();
    assert!(DepsFetchTask::verify_deps(&deps).is_empty());
}

// ─── 2. workflow! macro with `deps:` field ───────────────────────────────────

#[test]
fn workflow_macro_builds_with_deps() {
    let deps = Deps::builder()
        .insert(Arc::new(HttpClient {
            base_url: "https://api".into(),
        }))
        .insert(Arc::new(Counter { factor: 5 }))
        .build();

    let wf: BuildResult = workflow! {
        name: "checkout",
        codec: JsonCodec,
        deps: &deps,
        steps: [deps_fetch, deps_multiply, deps_plain]
    };

    if let Err(errs) = &wf {
        panic!("build failed: {errs}");
    }
}

#[test]
fn workflow_macro_surfaces_missing_dep_as_build_error() {
    let deps = Deps::new(); // empty — both tasks have unmet inject deps.

    let wf: BuildResult = workflow! {
        name: "checkout-broken",
        codec: JsonCodec,
        deps: &deps,
        steps: [deps_fetch, deps_multiply]
    };

    let errs = match wf {
        Ok(_) => panic!("expected MissingDep BuildErrors"),
        Err(e) => e,
    };
    let collected: Vec<_> = errs.into_iter().collect();
    assert_eq!(collected.len(), 2);

    let task_ids: Vec<&str> = collected
        .iter()
        .filter_map(|e| {
            if let BuildError::MissingDep { task_id, .. } = e {
                Some(task_id.as_str())
            } else {
                None
            }
        })
        .collect();
    assert!(task_ids.contains(&"deps_fetch"));
    assert!(task_ids.contains(&"deps_multiply"));

    // Each error should name the missing type.
    for e in &collected {
        let s = format!("{e}");
        assert!(s.contains("HttpClient") || s.contains("Counter"));
    }
}

#[test]
fn workflow_macro_partial_missing_dep() {
    // HttpClient present, Counter missing.
    let deps = Deps::builder()
        .insert(Arc::new(HttpClient {
            base_url: "x".into(),
        }))
        .build();

    let wf: BuildResult = workflow! {
        name: "checkout-partial",
        codec: JsonCodec,
        deps: &deps,
        steps: [deps_fetch, deps_multiply]
    };

    let errs = match wf {
        Ok(_) => panic!("expected MissingDep for Counter"),
        Err(e) => e,
    };
    let collected: Vec<_> = errs.into_iter().collect();
    assert_eq!(collected.len(), 1);
    match &collected[0] {
        BuildError::MissingDep { task_id, type_name } => {
            assert_eq!(task_id, "deps_multiply");
            assert!(type_name.contains("Counter"));
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn workflow_macro_mixing_inject_and_plain_tasks() {
    let deps = Deps::builder()
        .insert(Arc::new(HttpClient {
            base_url: "https://api".into(),
        }))
        .insert(Arc::new(Counter { factor: 3 }))
        .build();

    let wf: BuildResult = workflow! {
        name: "mixed",
        codec: JsonCodec,
        deps: &deps,
        steps: [deps_plain, deps_fetch, deps_multiply]
    };

    assert!(wf.is_ok());
}

// ─── 3. Sub-workflow composition shares the same Deps ────────────────────────

#[test]
fn workflow_macro_flow_shares_deps() {
    let deps = Deps::builder()
        .insert(Arc::new(HttpClient {
            base_url: "https://api".into(),
        }))
        .insert(Arc::new(Counter { factor: 2 }))
        .build();

    let child: SerializableWorkflow<JsonCodec, u32, ()> = match workflow! {
        name: "child",
        codec: JsonCodec,
        deps: &deps,
        steps: [deps_multiply]
    } {
        Ok(wf) => wf,
        Err(e) => panic!("child build failed: {e}"),
    };

    let parent: BuildResult = workflow! {
        name: "parent",
        codec: JsonCodec,
        deps: &deps,
        steps: [deps_fetch, flow child]
    };

    if let Err(e) = &parent {
        panic!("parent build failed: {e}");
    }
}

// ─── 4. Backward compatibility: workflow! without deps still works ──────────

#[test]
fn workflow_macro_without_deps_uses_new_for_plain_tasks() {
    // No `deps:` field — only tasks with no injected dependencies are usable.
    let wf: BuildResult = workflow! {
        name: "no-deps",
        codec: JsonCodec,
        steps: [deps_plain]
    };
    assert!(wf.is_ok());
}

// ─── 5. End-to-end execution with InProcessRunner ────────────────────────────

#[tokio::test]
async fn workflow_with_deps_executes_in_process() {
    let deps = Deps::builder()
        .insert(Arc::new(HttpClient {
            base_url: "abc".into(),
        })) // len 3
        .insert(Arc::new(Counter { factor: 10 }))
        .build();

    let serializable: SerializableWorkflow<JsonCodec, u32, ()> = match workflow! {
        name: "exec",
        codec: JsonCodec,
        deps: &deps,
        steps: [deps_fetch, deps_multiply, deps_plain]
    } {
        Ok(wf) => wf,
        Err(e) => panic!("build failed: {e}"),
    };

    let runner = InProcessRunner;
    // 5 + 3 = 8, 8 * 10 = 80, 80 + 1 = 81
    let status = runner.run(serializable.workflow(), 5u32).await.unwrap();
    assert!(matches!(
        status,
        sayiir_core::workflow::WorkflowStatus::Completed
    ));
}
