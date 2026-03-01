# sayiir-flow-js

Pure TypeScript workflow builder DSL for [Sayiir](https://docs.sayiir.dev) — no native dependencies.

This package contains the type-safe flow builder, task definitions, and core types used to **define** workflows. It does not include execution or persistence — those are provided by binding packages:

- **[sayiir](https://www.npmjs.com/package/sayiir)** — Node.js (NAPI-RS native bindings)
- **sayiir-cloudflare** — Cloudflare Workers (WASM) *(coming soon)*

## When to use this package directly

Most users should install `sayiir` (Node.js) or `sayiir-cloudflare` instead. Use `sayiir-flow-js` directly if you are:

- Building a new Sayiir binding package for another runtime
- Writing shared workflow definitions that must be runtime-agnostic

## Usage

```ts
import { Flow, task, createFlowFactory, type FlowBuilderBackend } from "sayiir-flow-js";

// Define tasks (pure — no runtime dependency)
const greet = task("greet", (name: string) => `Hello, ${name}!`);

// Build a flow with an injected backend
const myFlow = new Flow<string>(myBuilderBackend)
  .then(greet)
  .build();
```

### `createFlowFactory`

Binding packages use `createFlowFactory` to wire up their backend:

```ts
import { createFlowFactory } from "sayiir-flow-js";

// Each binding provides its own builder constructor
export const flow = createFlowFactory((name) => new NativeFlowBuilder(name));
```

## License

MIT
