/**
 * Rehydrate binary envelopes produced by the Rust codec into real
 * `Buffer` / `Uint8Array` / `ArrayBuffer` instances.
 *
 * The Rust side of the codec (in `sayiir-node/src/codec.rs`) replaces
 * binary values with a tagged envelope of the form
 *
 *   { "$sayiir_bin": "<base64>", "$sayiir_kind": "Buffer" | "Uint8Array" | "ArrayBuffer" }
 *
 * Base64 is used over a JSON array of numbers so the payload stays at
 * ~1.33× the raw byte count instead of ~5–7×. When the stepper / durable
 * engine paths return a JSON string to JS (`inputJson` for the next
 * task, `outputJson` for the final result), JS calls `JSON.parse` and
 * gets the envelope back as a plain object. This helper walks the parsed
 * value and reconstructs the original binary type.
 *
 * Direct task↔native invocations (the durable engine's task registry
 * bridge) do their rehydration in Rust and never come through here.
 */

const TAG_BIN = "$sayiir_bin";
const TAG_KIND = "$sayiir_kind";

/**
 * Parse JSON and rehydrate any binary envelopes in one pass. Skips the
 * tree walk entirely when the raw payload contains no envelope tag,
 * which is the common case.
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
    const buf = Buffer.from(bin, "base64");
    switch (kind) {
      case "Buffer":
        return buf;
      case "Uint8Array":
        return new Uint8Array(buf.buffer, buf.byteOffset, buf.byteLength);
      case "ArrayBuffer":
        return buf.buffer.slice(
          buf.byteOffset,
          buf.byteOffset + buf.byteLength,
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
