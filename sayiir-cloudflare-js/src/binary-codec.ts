/**
 * Rehydrate binary envelopes produced by the WASM codec into real
 * `ArrayBuffer` / `Uint8Array` instances.
 *
 * The Rust side of the codec (in `sayiir-cloudflare/src/codec.rs`) replaces
 * binary values with a tagged envelope of the form
 *
 *   { "$sayiir_bin": "<base64>", "$sayiir_kind": "ArrayBuffer" | "Uint8Array" }
 *
 * Direct task↔WASM invocations (the durable engine's task registry bridge)
 * round-trip through the codec's `parse_with_reviver_func` and never come
 * through here. But the stepper path and the durable-engine status return
 * a JSON *string* (`inputJson` / `outputJson`) to JS, so those callers must
 * decode with this helper instead of plain `JSON.parse`.
 */

const TAG_BIN = "$sayiir_bin";
const TAG_KIND = "$sayiir_kind";

/**
 * `JSON.parse` + rehydrate in one pass. Skips the tree walk entirely
 * when the raw payload contains no envelope tag (the common case).
 */
export function parseAndRehydrate(json: string): unknown {
  const parsed = JSON.parse(json);
  if (!json.includes(TAG_BIN)) return parsed;
  return rehydrateBinaries(parsed);
}

export function rehydrateBinaries(value: unknown): unknown {
  if (value == null || typeof value !== "object") return value;

  if (Array.isArray(value)) {
    for (let i = 0; i < value.length; i++) {
      value[i] = rehydrateBinaries(value[i]);
    }
    return value;
  }

  const obj = value as Record<string, unknown>;
  const bin = obj[TAG_BIN];
  const kind = obj[TAG_KIND];
  if (typeof bin === "string" && typeof kind === "string") {
    const bytes = decodeBase64ToBytes(bin);
    switch (kind) {
      case "Uint8Array":
        return bytes;
      case "ArrayBuffer":
        return bytes.buffer.slice(
          bytes.byteOffset,
          bytes.byteOffset + bytes.byteLength,
        );
      // Unknown kind: leave the envelope as-is so the caller sees the
      // raw data rather than silently dropping it.
    }
  }

  for (const k of Object.keys(obj)) {
    obj[k] = rehydrateBinaries(obj[k]);
  }
  return obj;
}

/**
 * Decode base64 to a Uint8Array using only Web Platform APIs. Workers
 * don't expose Node's `Buffer`; `atob` returns a Latin-1 string we can
 * walk byte-by-byte.
 */
function decodeBase64ToBytes(b64: string): Uint8Array {
  const bin = atob(b64);
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) {
    out[i] = bin.charCodeAt(i);
  }
  return out;
}
