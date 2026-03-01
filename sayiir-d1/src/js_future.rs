//! Helper to make `JsFuture` (which is `!Send`) usable in trait methods
//! that require `impl Future + Send`.
//!
//! SAFETY: This is only compiled for `wasm32-unknown-unknown` which is
//! single-threaded. There is no actual cross-thread sending.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use wasm_bindgen::JsValue;
use wasm_bindgen_futures::JsFuture;

/// A wrapper around [`JsFuture`] that implements [`Send`].
pub(crate) struct SendJsFuture(JsFuture);

// SAFETY: WASM is single-threaded — no real cross-thread access occurs.
unsafe impl Send for SendJsFuture {}

impl Future for SendJsFuture {
    type Output = Result<JsValue, JsValue>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: JsFuture is Unpin
        Pin::new(&mut self.0).poll(cx)
    }
}

/// Extension trait to convert a `js_sys::Promise` into a [`SendJsFuture`].
pub(crate) trait JsFutureExt {
    /// Convert this promise into a `Send`-safe future.
    fn into_send_future(self) -> SendJsFuture;
}

impl JsFutureExt for js_sys::Promise {
    fn into_send_future(self) -> SendJsFuture {
        SendJsFuture(JsFuture::from(self))
    }
}
