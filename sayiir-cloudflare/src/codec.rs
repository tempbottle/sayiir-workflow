//! Codec bridging JavaScript values and the Rust core's `Bytes`.
//!
//! Uses `JSON.stringify` / `JSON.parse` via `js_sys` for the JS ↔ Bytes
//! boundary, and simple byte-level string operations for assembling
//! structured JSON (branch envelopes, named results) from valid JSON
//! fragments — no `serde_json` needed.
//!
//! ## Binary types
//!
//! `JSON.stringify` is lossy for `ArrayBuffer` (returns `"{}"`) and
//! `Uint8Array` (returns `{"0":...,"1":...}`). The codec works around
//! this with a tagged-envelope round-trip:
//!
//! - **Encode**: a `stringify` replacer substitutes any `ArrayBuffer` or
//!   `Uint8Array` value with `{"$sayiir_bin": "<base64>",
//!   "$sayiir_kind": "ArrayBuffer" | "Uint8Array"}` before serialization.
//! - **Decode**: a `parse` reviver decodes those tagged envelopes back
//!   into real binary types.
//!
//! Base64 keeps the on-disk size to ~1.33× the raw bytes (versus ~5–7×
//! for a JSON array of numbers); for a workflow snapshot subject to D1's
//! ~1MB row size limit this matters.
//!
//! Other typed arrays (`Int32Array`, `Float64Array`, etc.) are not yet
//! handled — call `.buffer` on them to obtain an `ArrayBuffer` first.
//!
//! ## Fork/join branch results
//!
//! Branch results are encoded as a plain JSON object `{"name1":val1,...}`
//! by splicing together the already-valid JSON bytes of each branch output.

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use bytes::Bytes;
use js_sys::{Array, ArrayBuffer, JSON, JsString, Object, Reflect, Uint8Array};
use wasm_bindgen::prelude::wasm_bindgen;
use wasm_bindgen::{JsCast, JsError, JsValue};

use crate::error::to_js_error;

const TAG_BIN: &str = "$sayiir_bin";
const TAG_KIND: &str = "$sayiir_kind";

#[wasm_bindgen]
extern "C" {
    /// Mirror of `JSON.parse(text, reviver)` — `js_sys 0.3.98` only exposes
    /// the no-reviver and replacer-flavored variants of stringify, so we
    /// surface the reviver-flavored parse manually for codec rehydration.
    #[wasm_bindgen(catch, js_namespace = JSON, js_name = parse)]
    fn parse_with_reviver_func(
        text: &str,
        reviver: &mut dyn FnMut(JsString, JsValue) -> Result<JsValue, JsError>,
    ) -> Result<JsValue, JsValue>;
}

/// Encode a JavaScript value to JSON bytes.
///
/// `ArrayBuffer` and `Uint8Array` values inside `value` (at any depth) are
/// replaced with a tagged envelope so they survive the round-trip.
///
/// # Errors
///
/// Returns a JS error if `JSON.stringify` fails.
pub(crate) fn encode_js_value(value: &JsValue) -> Result<Bytes, JsValue> {
    let mut replacer = binary_replacer;
    let json_string = JSON::stringify_with_replacer_func(value, &mut replacer, None)
        .map_err(|_| to_js_error("Failed to stringify JS value"))?;
    let s: String = json_string.into();
    Ok(Bytes::from(s.into_bytes()))
}

/// Decode JSON bytes to a JavaScript value.
///
/// Tagged binary envelopes produced by [`encode_js_value`] are rehydrated
/// back into `ArrayBuffer` / `Uint8Array` instances.
///
/// # Errors
///
/// Returns a JS error if the bytes are not valid UTF-8 or valid JSON.
pub(crate) fn decode_to_js_value(bytes: &Bytes) -> Result<JsValue, JsValue> {
    let json_str = std::str::from_utf8(bytes)
        .map_err(|e| to_js_error(format!("Invalid UTF-8 in bytes: {e}")))?;
    // Common case: payload has no binary envelopes. Skip the reviver
    // walk (which `JSON.parse` would invoke for every node) and go
    // straight to the fast no-reviver path.
    if !json_str.contains("\"$sayiir_bin\"") {
        return JSON::parse(json_str).map_err(|_| to_js_error(format!("Invalid JSON: {json_str}")));
    }
    let mut reviver = binary_reviver;
    parse_with_reviver_func(json_str, &mut reviver)
        .map_err(|_| to_js_error(format!("Invalid JSON: {json_str}")))
}

/// Replacer hook for `JSON.stringify`: substitutes binary values with a
/// tagged envelope so they survive the JSON round-trip.
///
/// Returning `Some(value)` keeps the value (possibly substituted) in the
/// output; returning `None` would drop the entry, which we never want.
fn binary_replacer(_key: JsString, value: JsValue) -> Result<Option<JsValue>, JsError> {
    if let Some(envelope) = binary_envelope(&value)? {
        return Ok(Some(envelope));
    }
    Ok(Some(value))
}

/// Reviver hook for `JSON.parse`: rehydrates tagged envelopes back into
/// `ArrayBuffer` / `Uint8Array` instances. Other values pass through.
fn binary_reviver(_key: JsString, value: JsValue) -> Result<JsValue, JsError> {
    if let Some(bin) = try_rehydrate_envelope(&value)? {
        return Ok(bin);
    }
    Ok(value)
}

/// Build the tagged envelope for an `ArrayBuffer` or `Uint8Array`, or
/// `Ok(None)` if `value` isn't a supported binary type.
fn binary_envelope(value: &JsValue) -> Result<Option<JsValue>, JsError> {
    let (bytes_array, kind): (Uint8Array, &str) = if value.is_instance_of::<ArrayBuffer>() {
        (Uint8Array::new(value), "ArrayBuffer")
    } else if value.is_instance_of::<Uint8Array>() {
        let u8 = value.unchecked_ref::<Uint8Array>();
        (u8.clone(), "Uint8Array")
    } else {
        return Ok(None);
    };

    let encoded = BASE64.encode(bytes_array.to_vec());

    let env = Object::new();
    Reflect::set(
        &env,
        &JsValue::from_str(TAG_BIN),
        &JsValue::from_str(&encoded),
    )
    .map_err(|_| JsError::new("Failed to set $sayiir_bin"))?;
    Reflect::set(&env, &JsValue::from_str(TAG_KIND), &JsValue::from_str(kind))
        .map_err(|_| JsError::new("Failed to set $sayiir_kind"))?;
    Ok(Some(env.into()))
}

/// If `value` is a binary envelope, decode it back to the original binary
/// type. Returns `Ok(None)` for anything else.
fn try_rehydrate_envelope(value: &JsValue) -> Result<Option<JsValue>, JsError> {
    if !value.is_object() || Array::is_array(value) {
        return Ok(None);
    }
    let bin = Reflect::get(value, &JsValue::from_str(TAG_BIN))
        .map_err(|_| JsError::new("Reflect.get $sayiir_bin"))?;
    let kind = Reflect::get(value, &JsValue::from_str(TAG_KIND))
        .map_err(|_| JsError::new("Reflect.get $sayiir_kind"))?;
    let (Some(encoded), Some(kind_str)) = (bin.as_string(), kind.as_string()) else {
        return Ok(None);
    };
    let bytes = BASE64
        .decode(encoded.as_bytes())
        .map_err(|e| JsError::new(&format!("Invalid base64 in $sayiir_bin: {e}")))?;
    let len_u32 = u32::try_from(bytes.len())
        .map_err(|_| JsError::new("binary envelope exceeds u32::MAX bytes"))?;

    let u8 = Uint8Array::new_with_length(len_u32);
    u8.copy_from(&bytes);

    let out: JsValue = match kind_str.as_str() {
        "ArrayBuffer" => u8.buffer().into(),
        "Uint8Array" => u8.into(),
        _ => return Ok(None),
    };
    Ok(Some(out))
}

/// Decode a JSON-encoded string from bytes (e.g. `"my_key"` → `my_key`).
///
/// # Errors
///
/// Returns an error if the bytes are not a valid JSON string.
pub(crate) fn decode_json_string(bytes: &[u8]) -> Result<String, JsValue> {
    let js_val = decode_to_js_value(&Bytes::copy_from_slice(bytes))?;
    js_val
        .as_string()
        .ok_or_else(|| to_js_error("Expected JSON string value"))
}

/// Build a branch envelope: `{"branch":"key","result":...}`.
///
/// # Errors
///
/// Returns a JS error if decoding result bytes or encoding the envelope fails.
pub(crate) fn encode_branch_envelope(key: &str, result_bytes: &[u8]) -> Result<Bytes, JsValue> {
    let obj = js_sys::Object::new();
    js_sys::Reflect::set(&obj, &JsValue::from_str("branch"), &JsValue::from_str(key))?;
    let result_val = decode_to_js_value(&Bytes::copy_from_slice(result_bytes))?;
    js_sys::Reflect::set(&obj, &JsValue::from_str("result"), &result_val)?;
    encode_js_value(&obj.into())
}

/// Merge named branch results into a JSON object: `{"name1":val1,"name2":val2,...}`.
///
/// # Errors
///
/// Returns a JS error if decoding any branch bytes or encoding the result fails.
pub(crate) fn encode_named_results(results: &[(String, Bytes)]) -> Result<Bytes, JsValue> {
    let obj = js_sys::Object::new();
    for (name, bytes) in results {
        let val = decode_to_js_value(&Bytes::copy_from_slice(bytes))?;
        js_sys::Reflect::set(&obj, &JsValue::from_str(name), &val)?;
    }
    encode_js_value(&obj.into())
}

/// Decode a `LoopResult` JSON (`{"_loop":"done"|"again","value":...}`)
/// into `(tag, inner_bytes)`.
///
/// # Errors
///
/// Returns a JS error if the JSON is malformed or missing required fields.
pub(crate) fn decode_loop_result(bytes: &[u8]) -> Result<(String, Bytes), JsValue> {
    let js_val = decode_to_js_value(&Bytes::copy_from_slice(bytes))?;
    let tag = js_sys::Reflect::get(&js_val, &JsValue::from_str("_loop"))?
        .as_string()
        .ok_or_else(|| to_js_error("Missing or invalid '_loop' tag in LoopResult"))?;
    let value = js_sys::Reflect::get(&js_val, &JsValue::from_str("value"))?;
    let value_bytes = encode_js_value(&value)?;
    Ok((tag, value_bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use wasm_bindgen_test::*;

    #[wasm_bindgen_test]
    fn roundtrip_number() {
        let val = JsValue::from_f64(42.0);
        let bytes = encode_js_value(&val).unwrap();
        assert_eq!(&bytes[..], b"42");
        let decoded = decode_to_js_value(&bytes).unwrap();
        assert_eq!(decoded.as_f64(), Some(42.0));
    }

    #[wasm_bindgen_test]
    fn roundtrip_string() {
        let val = JsValue::from_str("hello world");
        let bytes = encode_js_value(&val).unwrap();
        assert_eq!(&bytes[..], br#""hello world""#);
        let decoded = decode_to_js_value(&bytes).unwrap();
        assert_eq!(decoded.as_string().as_deref(), Some("hello world"));
    }

    #[wasm_bindgen_test]
    fn roundtrip_object() {
        let obj = js_sys::Object::new();
        js_sys::Reflect::set(&obj, &JsValue::from_str("a"), &JsValue::from_f64(1.0)).unwrap();
        js_sys::Reflect::set(&obj, &JsValue::from_str("b"), &JsValue::from_str("two")).unwrap();

        let bytes = encode_js_value(&obj.into()).unwrap();
        let decoded = decode_to_js_value(&bytes).unwrap();

        let a = js_sys::Reflect::get(&decoded, &JsValue::from_str("a")).unwrap();
        let b = js_sys::Reflect::get(&decoded, &JsValue::from_str("b")).unwrap();
        assert_eq!(a.as_f64(), Some(1.0));
        assert_eq!(b.as_string().as_deref(), Some("two"));
    }

    #[wasm_bindgen_test]
    fn roundtrip_null() {
        let bytes = encode_js_value(&JsValue::NULL).unwrap();
        assert_eq!(&bytes[..], b"null");
        let decoded = decode_to_js_value(&bytes).unwrap();
        assert!(decoded.is_null());
    }

    #[wasm_bindgen_test]
    fn roundtrip_bool() {
        let bytes = encode_js_value(&JsValue::TRUE).unwrap();
        assert_eq!(&bytes[..], b"true");
        let decoded = decode_to_js_value(&bytes).unwrap();
        assert_eq!(decoded.as_bool(), Some(true));
    }

    #[wasm_bindgen_test]
    fn roundtrip_array() {
        let arr = js_sys::Array::new();
        arr.push(&JsValue::from_f64(1.0));
        arr.push(&JsValue::from_str("x"));

        let bytes = encode_js_value(&arr.into()).unwrap();
        let decoded = decode_to_js_value(&bytes).unwrap();

        let js_arr = js_sys::Array::from(&decoded);
        assert_eq!(js_arr.length(), 2);
        assert_eq!(js_arr.get(0).as_f64(), Some(1.0));
        assert_eq!(js_arr.get(1).as_string().as_deref(), Some("x"));
    }

    #[wasm_bindgen_test]
    fn decode_string_from_json_bytes() {
        let bytes = br#""my_key""#;
        assert_eq!(decode_json_string(bytes).unwrap(), "my_key");
    }

    #[wasm_bindgen_test]
    fn decode_string_rejects_number() {
        assert!(decode_json_string(b"42").is_err());
    }

    #[wasm_bindgen_test]
    fn branch_envelope_roundtrip() {
        let result_bytes = b"123";
        let envelope = encode_branch_envelope("my_branch", result_bytes).unwrap();

        let decoded = decode_to_js_value(&envelope).unwrap();
        let branch = js_sys::Reflect::get(&decoded, &JsValue::from_str("branch")).unwrap();
        let result = js_sys::Reflect::get(&decoded, &JsValue::from_str("result")).unwrap();

        assert_eq!(branch.as_string().as_deref(), Some("my_branch"));
        assert_eq!(result.as_f64(), Some(123.0));
    }

    #[wasm_bindgen_test]
    fn branch_envelope_with_object_result() {
        let inner = br#"{"x":1}"#;
        let envelope = encode_branch_envelope("b1", inner).unwrap();
        let decoded = decode_to_js_value(&envelope).unwrap();

        let result = js_sys::Reflect::get(&decoded, &JsValue::from_str("result")).unwrap();
        let x = js_sys::Reflect::get(&result, &JsValue::from_str("x")).unwrap();
        assert_eq!(x.as_f64(), Some(1.0));
    }

    #[wasm_bindgen_test]
    fn named_results_roundtrip() {
        let results = vec![
            ("alpha".to_string(), Bytes::from(r#""hello""#)),
            ("beta".to_string(), Bytes::from("42")),
        ];
        let encoded = encode_named_results(&results).unwrap();
        let decoded = decode_to_js_value(&encoded).unwrap();

        let alpha = js_sys::Reflect::get(&decoded, &JsValue::from_str("alpha")).unwrap();
        let beta = js_sys::Reflect::get(&decoded, &JsValue::from_str("beta")).unwrap();
        assert_eq!(alpha.as_string().as_deref(), Some("hello"));
        assert_eq!(beta.as_f64(), Some(42.0));
    }

    #[wasm_bindgen_test]
    fn named_results_empty() {
        let encoded = encode_named_results(&[]).unwrap();
        assert_eq!(&encoded[..], b"{}");
    }

    #[wasm_bindgen_test]
    fn loop_result_done() {
        let bytes = br#"{"_loop":"done","value":99}"#;
        let (tag, inner) = decode_loop_result(bytes).unwrap();
        assert_eq!(tag, "done");
        assert_eq!(&inner[..], b"99");
    }

    #[wasm_bindgen_test]
    fn loop_result_again_with_object() {
        let bytes = br#"{"_loop":"again","value":{"count":5}}"#;
        let (tag, inner) = decode_loop_result(bytes).unwrap();
        assert_eq!(tag, "again");

        let decoded = decode_to_js_value(&inner).unwrap();
        let count = js_sys::Reflect::get(&decoded, &JsValue::from_str("count")).unwrap();
        assert_eq!(count.as_f64(), Some(5.0));
    }

    #[wasm_bindgen_test]
    fn loop_result_missing_tag() {
        let bytes = br#"{"value":1}"#;
        assert!(decode_loop_result(bytes).is_err());
    }

    #[wasm_bindgen_test]
    fn arraybuffer_roundtrip() {
        let u8 = Uint8Array::new_with_length(4);
        u8.set_index(0, 0x00);
        u8.set_index(1, 0x7f);
        u8.set_index(2, 0xc0);
        u8.set_index(3, 0xff);
        let buf: JsValue = u8.buffer().into();

        let bytes = encode_js_value(&buf).unwrap();
        let s = std::str::from_utf8(&bytes).unwrap();
        assert!(s.contains("$sayiir_bin"));
        assert!(s.contains("ArrayBuffer"));

        let decoded = decode_to_js_value(&bytes).unwrap();
        assert!(decoded.is_instance_of::<ArrayBuffer>());
        let round = Uint8Array::new(&decoded);
        assert_eq!(round.length(), 4);
        assert_eq!(round.get_index(0), 0x00);
        assert_eq!(round.get_index(1), 0x7f);
        assert_eq!(round.get_index(2), 0xc0);
        assert_eq!(round.get_index(3), 0xff);
    }

    #[wasm_bindgen_test]
    fn uint8array_roundtrip() {
        let u8 = Uint8Array::new_with_length(3);
        u8.set_index(0, 1);
        u8.set_index(1, 2);
        u8.set_index(2, 3);
        let val: JsValue = u8.into();

        let bytes = encode_js_value(&val).unwrap();
        let decoded = decode_to_js_value(&bytes).unwrap();
        assert!(decoded.is_instance_of::<Uint8Array>());
        let round = decoded.unchecked_ref::<Uint8Array>();
        assert_eq!(round.length(), 3);
        assert_eq!(round.get_index(0), 1);
        assert_eq!(round.get_index(1), 2);
        assert_eq!(round.get_index(2), 3);
    }

    #[wasm_bindgen_test]
    fn nested_binary_in_object_roundtrip() {
        // { name: "doc", body: <Uint8Array [10, 20]> }
        let u8 = Uint8Array::new_with_length(2);
        u8.set_index(0, 10);
        u8.set_index(1, 20);
        let outer = js_sys::Object::new();
        Reflect::set(
            &outer,
            &JsValue::from_str("name"),
            &JsValue::from_str("doc"),
        )
        .unwrap();
        Reflect::set(&outer, &JsValue::from_str("body"), &u8.into()).unwrap();

        let bytes = encode_js_value(&outer.into()).unwrap();
        let decoded = decode_to_js_value(&bytes).unwrap();

        let name = Reflect::get(&decoded, &JsValue::from_str("name")).unwrap();
        let body = Reflect::get(&decoded, &JsValue::from_str("body")).unwrap();
        assert_eq!(name.as_string().as_deref(), Some("doc"));
        assert!(body.is_instance_of::<Uint8Array>());
        let body_u8 = body.unchecked_ref::<Uint8Array>();
        assert_eq!(body_u8.get_index(0), 10);
        assert_eq!(body_u8.get_index(1), 20);
    }

    #[wasm_bindgen_test]
    fn binary_in_array_roundtrip() {
        // [<Uint8Array [9]>, "next"]
        let u8 = Uint8Array::new_with_length(1);
        u8.set_index(0, 9);
        let arr = Array::new();
        arr.push(&u8.into());
        arr.push(&JsValue::from_str("next"));

        let bytes = encode_js_value(&arr.into()).unwrap();
        let decoded = decode_to_js_value(&bytes).unwrap();

        let out = Array::from(&decoded);
        let first = out.get(0);
        let second = out.get(1);
        assert!(first.is_instance_of::<Uint8Array>());
        assert_eq!(first.unchecked_ref::<Uint8Array>().get_index(0), 9);
        assert_eq!(second.as_string().as_deref(), Some("next"));
    }
}
