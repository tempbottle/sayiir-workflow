//! JSON-based codec bridging JavaScript values and the Rust core's `Bytes` representation.
//!
//! The Rust workflow engine operates entirely on opaque `Bytes` — it never inspects
//! task inputs or outputs. The Node.js binding layer must convert between
//! JavaScript values and `Bytes` at two boundaries:
//!
//! 1. **Encode** — before handing a JS return value to the Rust engine
//! 2. **Decode** — before passing stored `Bytes` back into a JS task
//!
//! ## Binary types
//!
//! `serde_json::Value` has no native representation for `Buffer` /
//! `Uint8Array` / `ArrayBuffer`, and the napi-rs serde bridge would mangle
//! them into numeric-keyed objects (`Buffer`) or empty objects (`ArrayBuffer`).
//! To make binary values survive a task checkpoint, the codec walks the JS
//! value tree and substitutes any binary value with a tagged plain object:
//!
//! ```text
//! { "$sayiir_bin": [byte, byte, ...], "$sayiir_kind": "Buffer" | "Uint8Array" | "ArrayBuffer" }
//! ```
//!
//! On decode, a matching post-walk rehydrates those tagged objects back into
//! the corresponding binary type. Other typed arrays (`Int32Array`,
//! `Float64Array`, etc.) are not yet handled — call `.buffer` to obtain an
//! `ArrayBuffer` first, or pull the bytes into a `Uint8Array`.
//!
//! ## Fork/join branch results
//!
//! Fork/join branches produce a `NamedBranchResults` (serialized by serde as
//! `[[name, [u8…]], …]`). If decoded naively with `JSON.parse`, this would yield
//! an array-of-arrays instead of the `Record<string, value>` that JS join tasks
//! expect. [`decode_to_js_value`] detects this shape and converts it into a JS
//! object where each value is individually JSON-decoded.

use bytes::Bytes;
use napi::bindgen_prelude::*;
use napi::sys;
use napi::{Env, JsValue};
use sayiir_core::branch_results::NamedBranchResults;
use serde_json::{Map, Number, Value as JsonValue};

const TAG_BIN: &str = "$sayiir_bin";
const TAG_KIND: &str = "$sayiir_kind";

/// Encodes a JavaScript value to JSON bytes.
///
/// Walks the value tree, replacing `Buffer` / `Uint8Array` / `ArrayBuffer`
/// nodes with a tagged plain-object envelope so they survive serialization.
/// Falls back to napi-rs's bulk `env.from_js_value` (one FFI call) when
/// the tree contains no binary types — much faster than the manual walker
/// for the common case.
pub fn encode_js_value(env: &Env, value: Unknown<'_>) -> Result<Bytes> {
    let json = if contains_binary_type(&value)? {
        js_to_json(&value)?
    } else {
        env.from_js_value(value)?
    };
    let json_bytes =
        serde_json::to_vec(&json).map_err(|e| Error::new(Status::GenericFailure, e.to_string()))?;
    Ok(Bytes::from(json_bytes))
}

/// Walk the JS value tree looking for any `Buffer` / `Uint8Array` /
/// `ArrayBuffer` node. Short-circuits on first hit. Used as a fast-path
/// gate for [`encode_js_value`] to decide whether to use the manual
/// walker (handles binaries) or napi-rs's bulk serde bridge.
fn contains_binary_type(value: &Unknown<'_>) -> Result<bool> {
    let env_raw = value.value().env;
    let raw = value.value().value;

    if is_arraybuffer(env_raw, raw)? || is_typedarray(env_raw, raw)? || is_buffer(env_raw, raw)? {
        return Ok(true);
    }
    if value.get_type()? != ValueType::Object {
        return Ok(false);
    }
    let mut is_arr = false;
    check_status!(
        unsafe { sys::napi_is_array(env_raw, raw, &raw mut is_arr) },
        "napi_is_array"
    )?;
    if is_arr {
        let arr: Array = unsafe { Array::from_napi_value(env_raw, raw) }?;
        for i in 0..arr.len() {
            let Some(item) = arr.get::<Unknown>(i)? else {
                continue;
            };
            if contains_binary_type(&item)? {
                return Ok(true);
            }
        }
        return Ok(false);
    }
    let obj: Object = unsafe { Object::from_napi_value(env_raw, raw) }?;
    for key in Object::keys(&obj)? {
        if let Some(v) = obj.get::<Unknown>(&key)?
            && contains_binary_type(&v)?
        {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Decodes JSON bytes to a JavaScript value.
///
/// Branch results (fork/join) are handled by [`decode_branch_results`] —
/// see that function's doc. For regular task inputs/outputs, the bytes are
/// parsed as JSON and any tagged binary envelopes are rehydrated back into
/// `Buffer` / `Uint8Array` / `ArrayBuffer`.
pub fn decode_to_js_value<'env>(env: &'env Env, bytes: &Bytes) -> Result<Unknown<'env>> {
    // Branch results serialize as `[[name, [u8…]], …]`, always starting
    // with `[`. Skip the speculative typed parse for any payload that
    // doesn't look like an array — saves a full deserialization on every
    // task input/output.
    if bytes.first() == Some(&b'[')
        && let Ok(named) = serde_json::from_slice::<NamedBranchResults>(bytes)
        && !named.is_empty()
    {
        return decode_branch_results(env, &named);
    }

    let serde_val: JsonValue =
        serde_json::from_slice(bytes).map_err(|e| Error::new(Status::InvalidArg, e.to_string()))?;
    let unknown: Unknown = env.to_js_value(&serde_val)?;
    // Skip the rehydrate walk entirely when no binary envelopes are
    // present — common case is a payload with zero `$sayiir_bin` tags.
    if !contains_binary_tag(bytes) {
        return Ok(unknown);
    }
    rehydrate_binaries(env, unknown)
}

/// Cheap byte-level check: does the JSON payload contain a binary tag?
fn contains_binary_tag(bytes: &[u8]) -> bool {
    // The tag is always quoted as a JSON key — `"$sayiir_bin"`. A naive
    // window scan is fast enough; the tag is rare in user payloads.
    bytes
        .windows(BIN_TAG_NEEDLE.len())
        .any(|w| w == BIN_TAG_NEEDLE)
}

const BIN_TAG_NEEDLE: &[u8] = b"\"$sayiir_bin\"";

/// Converts deserialized `NamedBranchResults` into a JS object.
///
/// Each branch value is individually JSON-decoded (and binary-tagged
/// envelopes are rehydrated as part of that path).
fn decode_branch_results<'env>(
    env: &'env Env,
    named: &NamedBranchResults,
) -> Result<Unknown<'env>> {
    let mut obj = Object::new(env)?;
    for (name, data) in named.as_slice() {
        let serde_val: JsonValue = serde_json::from_slice(data)
            .map_err(|e| Error::new(Status::InvalidArg, e.to_string()))?;
        let raw: Unknown = env.to_js_value(&serde_val)?;
        let rehydrated = rehydrate_binaries(env, raw)?;
        obj.set(name.as_str(), rehydrated)?;
    }
    obj.into_unknown(env)
}

// ─── encode side: JS → serde_json::Value with binary substitution ───────

fn js_to_json(value: &Unknown<'_>) -> Result<JsonValue> {
    let env_raw = value.value().env;
    let raw = value.value().value;

    // Binary types first. Note: Node's `napi_is_buffer` is misnamed — it
    // returns true for ANY `Uint8Array` because the C impl checks
    // `IsUint8Array()` (Buffer is a Uint8Array subclass). We disambiguate
    // by looking at `value.constructor.name` so a plain Uint8Array round-
    // trips as Uint8Array and a Node Buffer round-trips as Buffer.
    if is_arraybuffer(env_raw, raw)? {
        let buf: ArrayBuffer = unsafe { ArrayBuffer::from_napi_value(env_raw, raw) }?;
        return Ok(make_binary_json(&buf, "ArrayBuffer"));
    }
    if is_typedarray(env_raw, raw)? || is_buffer(env_raw, raw)? {
        // BufferSlice covers both Buffer and any u8-typed array — the
        // underlying napi_get_buffer_info call returns the same bytes for
        // either. Disambiguating Buffer vs Uint8Array is done via the JS
        // constructor name (see above).
        let slice: BufferSlice = unsafe { BufferSlice::from_napi_value(env_raw, raw) }?;
        let kind = match constructor_name(env_raw, raw)?.as_deref() {
            Some("Buffer") => "Buffer",
            _ => "Uint8Array",
        };
        return Ok(make_binary_json(&slice, kind));
    }

    let ty = value.get_type()?;
    match ty {
        ValueType::Null | ValueType::Undefined => Ok(JsonValue::Null),
        ValueType::Boolean => {
            let b = unsafe { bool::from_napi_value(env_raw, raw) }?;
            Ok(JsonValue::Bool(b))
        }
        ValueType::Number => {
            let n = unsafe { f64::from_napi_value(env_raw, raw) }?;
            Ok(Number::from_f64(n).map_or(JsonValue::Null, JsonValue::Number))
        }
        ValueType::String => {
            let s = unsafe { String::from_napi_value(env_raw, raw) }?;
            Ok(JsonValue::String(s))
        }
        // BigInt and other extended types are not yet supported — they
        // would need explicit handling here (or fall back to a string
        // representation). For now they hit the error arm below.
        ValueType::Object => {
            let mut is_arr = false;
            check_status!(
                unsafe { sys::napi_is_array(env_raw, raw, &raw mut is_arr) },
                "napi_is_array"
            )?;
            if is_arr {
                let arr: Array = unsafe { Array::from_napi_value(env_raw, raw) }?;
                let mut out = Vec::with_capacity(arr.len() as usize);
                for i in 0..arr.len() {
                    let Some(item) = arr.get::<Unknown>(i)? else {
                        continue;
                    };
                    out.push(js_to_json(&item)?);
                }
                return Ok(JsonValue::Array(out));
            }

            let obj: Object = unsafe { Object::from_napi_value(env_raw, raw) }?;
            let mut map = Map::new();
            for key in Object::keys(&obj)? {
                let v: Option<Unknown> = obj.get::<Unknown>(&key)?;
                if let Some(v) = v {
                    map.insert(key, js_to_json(&v)?);
                }
            }
            Ok(JsonValue::Object(map))
        }
        ValueType::Function | ValueType::Symbol | ValueType::External => Err(Error::new(
            Status::InvalidArg,
            format!("JS value of type {ty:?} cannot be serialized"),
        )),
        _ => Err(Error::new(
            Status::InvalidArg,
            format!("Unsupported JS value type: {ty:?}"),
        )),
    }
}

fn make_binary_json(bytes: &[u8], kind: &str) -> JsonValue {
    let arr: Vec<JsonValue> = bytes
        .iter()
        .map(|&b| JsonValue::Number(Number::from(b)))
        .collect();
    let mut map = Map::with_capacity(2);
    map.insert(TAG_BIN.to_string(), JsonValue::Array(arr));
    map.insert(TAG_KIND.to_string(), JsonValue::String(kind.to_string()));
    JsonValue::Object(map)
}

// ─── decode side: rehydrate binary tags after env.to_js_value ──────────

fn rehydrate_binaries<'env>(env: &'env Env, value: Unknown<'env>) -> Result<Unknown<'env>> {
    let ty = value.get_type()?;
    if ty != ValueType::Object {
        return Ok(value);
    }

    let env_raw = value.value().env;
    let raw = value.value().value;

    let mut is_arr = false;
    check_status!(
        unsafe { sys::napi_is_array(env_raw, raw, &raw mut is_arr) },
        "napi_is_array"
    )?;
    if is_arr {
        // Mutate the existing array in place: we own this value (it was
        // freshly produced by env.to_js_value) and no JS caller holds a
        // reference that could observe partial state.
        let mut arr: Array = unsafe { Array::from_napi_value(env_raw, raw) }?;
        for i in 0..arr.len() {
            let item: Option<Unknown> = arr.get::<Unknown>(i)?;
            if let Some(item) = item {
                let rehydrated = rehydrate_binaries(env, item)?;
                arr.set(i, rehydrated)?;
            }
        }
        return Ok(value);
    }

    let obj: Object = unsafe { Object::from_napi_value(env_raw, raw) }?;

    if let Some(rehydrated) = try_rehydrate_envelope(env, &obj)? {
        return Ok(rehydrated);
    }

    let mut new_obj = Object::new(env)?;
    for key in Object::keys(&obj)? {
        let v: Option<Unknown> = obj.get::<Unknown>(&key)?;
        if let Some(v) = v {
            new_obj.set(&key, rehydrate_binaries(env, v)?)?;
        }
    }
    new_obj.into_unknown(env)
}

fn try_rehydrate_envelope<'env>(
    env: &'env Env,
    obj: &Object<'env>,
) -> Result<Option<Unknown<'env>>> {
    // `kind` must be a string AND `bin` must be a JS array — otherwise
    // this is just a user object that happens to have similar keys.
    let Some(kind) = obj.get::<String>(TAG_KIND)? else {
        return Ok(None);
    };
    let Some(bin) = obj.get::<Unknown>(TAG_BIN)? else {
        return Ok(None);
    };
    let env_raw = bin.value().env;
    let bin_raw = bin.value().value;
    let mut is_arr = false;
    check_status!(
        unsafe { sys::napi_is_array(env_raw, bin_raw, &raw mut is_arr) },
        "napi_is_array (bin)"
    )?;
    if !is_arr {
        return Ok(None);
    }

    let arr: Array = unsafe { Array::from_napi_value(env_raw, bin_raw) }?;
    let mut bytes = Vec::with_capacity(arr.len() as usize);
    for i in 0..arr.len() {
        let n: u32 = arr.get::<u32>(i)?.unwrap_or(0);
        bytes.push(n as u8);
    }

    let out: Unknown = match kind.as_str() {
        "Buffer" => Buffer::from(bytes).into_unknown(env)?,
        "Uint8Array" => Uint8Array::from(bytes).into_unknown(env)?,
        "ArrayBuffer" => ArrayBuffer::from_data(env, bytes)?.into_unknown(env)?,
        _ => return Ok(None),
    };
    Ok(Some(out))
}

// ─── napi type predicates ───────────────────────────────────────────────

fn is_buffer(env_raw: sys::napi_env, raw: sys::napi_value) -> Result<bool> {
    let mut result = false;
    check_status!(
        unsafe { sys::napi_is_buffer(env_raw, raw, &raw mut result) },
        "napi_is_buffer"
    )?;
    Ok(result)
}

fn is_arraybuffer(env_raw: sys::napi_env, raw: sys::napi_value) -> Result<bool> {
    let mut result = false;
    check_status!(
        unsafe { sys::napi_is_arraybuffer(env_raw, raw, &raw mut result) },
        "napi_is_arraybuffer"
    )?;
    Ok(result)
}

fn is_typedarray(env_raw: sys::napi_env, raw: sys::napi_value) -> Result<bool> {
    let mut result = false;
    check_status!(
        unsafe { sys::napi_is_typedarray(env_raw, raw, &raw mut result) },
        "napi_is_typedarray"
    )?;
    Ok(result)
}

/// Return `value.constructor.name`, or `None` if it can't be determined.
/// Used to distinguish `Buffer` from `Uint8Array` (Node's `napi_is_buffer`
/// returns true for both).
fn constructor_name(env_raw: sys::napi_env, raw: sys::napi_value) -> Result<Option<String>> {
    let obj: Object = unsafe { Object::from_napi_value(env_raw, raw) }?;
    let Some(ctor): Option<Object> = obj.get("constructor")? else {
        return Ok(None);
    };
    let name: Option<String> = ctor.get("name")?;
    Ok(name)
}
