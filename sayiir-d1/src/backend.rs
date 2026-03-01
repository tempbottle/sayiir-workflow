//! `D1Backend` struct, inline JSON codec, and constructors.

use bytes::Bytes;
use sayiir_core::codec::{self, Decoder, Encoder};
use sayiir_core::snapshot::WorkflowSnapshot;
use sayiir_persistence::BackendError;
use send_wrapper::SendWrapper;

use crate::bindings::D1Database;
use crate::error::D1Error;
use crate::js_future::JsFutureExt as _;
use crate::schema::MIGRATION_SQL;

// ---------------------------------------------------------------------------
// Inline JsonCodec (avoids depending on sayiir-runtime which pulls in tokio)
// ---------------------------------------------------------------------------

/// Minimal JSON codec for snapshot serialization.
#[derive(Debug, Clone, Default)]
pub struct JsonCodec;

impl Encoder for JsonCodec {}
impl Decoder for JsonCodec {}

impl codec::sealed::EncodeValue<WorkflowSnapshot> for JsonCodec {
    fn encode_value(
        &self,
        value: &WorkflowSnapshot,
    ) -> Result<Bytes, Box<dyn std::error::Error + Send + Sync>> {
        serde_json::to_vec(value)
            .map(Bytes::from)
            .map_err(|e| e.into())
    }
}

impl codec::sealed::DecodeValue<WorkflowSnapshot> for JsonCodec {
    fn decode_value(
        &self,
        bytes: Bytes,
    ) -> Result<WorkflowSnapshot, Box<dyn std::error::Error + Send + Sync>> {
        serde_json::from_slice(&bytes).map_err(|e| e.into())
    }
}

// ---------------------------------------------------------------------------
// D1Backend
// ---------------------------------------------------------------------------

/// Cloudflare D1 persistence backend for Sayiir workflows.
///
/// Uses JSON serialization for snapshot data stored as `BLOB` in D1 (SQLite).
/// This backend targets `wasm32-unknown-unknown` and communicates with D1
/// through `wasm-bindgen` FFI bindings.
///
/// # Example
///
/// ```rust,ignore
/// use sayiir_d1::D1Backend;
///
/// // `db` is a D1Database binding from the Worker env
/// let backend = D1Backend::new(db).await?;
/// ```
pub struct D1Backend {
    // `SendWrapper` provides `Send + Sync` for the `!Send` `D1Database`.
    // SAFETY is upheld by `SendWrapper`'s contract: access only from the
    // original thread — guaranteed on single-threaded WASM.
    db: SendWrapper<D1Database>,
}

impl D1Backend {
    /// Create a new D1 backend and run schema migrations.
    ///
    /// # Errors
    ///
    /// Returns an error if the migration DDL fails.
    pub async fn new(db: D1Database) -> Result<Self, BackendError> {
        db.run_raw(MIGRATION_SQL)
            .into_send_future()
            .await
            .map_err(D1Error)?;

        Ok(Self {
            db: SendWrapper::new(db),
        })
    }

    /// Get a reference to the underlying D1 database.
    pub(crate) fn db(&self) -> &D1Database {
        &self.db
    }

    /// Encode a snapshot to JSON bytes.
    pub(crate) fn encode(&self, snapshot: &WorkflowSnapshot) -> Result<Vec<u8>, BackendError> {
        let codec = JsonCodec;
        codec
            .encode(snapshot)
            .map(|b| b.to_vec())
            .map_err(|e| BackendError::Serialization(e.to_string()))
    }

    /// Decode a snapshot from JSON bytes.
    pub(crate) fn decode(&self, data: &[u8]) -> Result<WorkflowSnapshot, BackendError> {
        let codec = JsonCodec;
        codec
            .decode(Bytes::copy_from_slice(data))
            .map_err(|e| BackendError::Serialization(e.to_string()))
    }
}
