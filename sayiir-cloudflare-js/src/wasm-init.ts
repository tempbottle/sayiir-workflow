/**
 * Eagerly initializes the WASM module.
 *
 * Cloudflare Workers (and Wrangler in dev) resolve `.wasm` imports to a
 * `WebAssembly.Module`. `initSync` is a no-op on repeat calls, so it's safe
 * to import this from multiple entry points.
 */

import { initSync } from "../wasm/sayiir_cloudflare.js";
import wasmModule from "../wasm/sayiir_cloudflare_bg.wasm";

initSync({ module: wasmModule });
