# sayiir-di

Type-keyed dependency-injection container for Sayiir workflows and tasks.

`Deps` is a small `HashMap<TypeId, …>` that lets you register cloneable values
once and resolve them by type from anywhere a `&Deps` is available — including
inside `#[task]` constructors and the `workflow!` macro.

```rust
use sayiir_di::Deps;
use std::sync::Arc;

#[derive(Clone)]
struct HttpClient;

let deps = Deps::builder()
    .insert(Arc::new(HttpClient))
    .build();

let client: Arc<HttpClient> = deps.expect();
```

See the `workflow!` macro's `deps:` field for the typical integration.
