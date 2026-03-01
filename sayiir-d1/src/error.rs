//! Error mapping from `JsValue` to [`BackendError`].

use sayiir_persistence::BackendError;
use wasm_bindgen::JsValue;

/// Newtype for `JsValue` errors so we can convert into `BackendError`.
#[derive(Debug)]
pub(crate) struct D1Error(pub JsValue);

impl From<JsValue> for D1Error {
    fn from(e: JsValue) -> Self {
        Self(e)
    }
}

impl From<D1Error> for BackendError {
    fn from(e: D1Error) -> Self {
        let msg =
            e.0.as_string()
                .or_else(|| {
                    js_sys::Reflect::get(&e.0, &JsValue::from_str("message"))
                        .ok()
                        .and_then(|v| v.as_string())
                })
                .unwrap_or_else(|| format!("{:?}", e.0));
        BackendError::Backend(msg)
    }
}
