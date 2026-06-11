use bytes::Bytes;
use sayiir_core::codec::{CodecIdentity, Decoder, Encoder, sealed};
use sayiir_core::error::BoxError;
use sayiir_core::snapshot_format::CodecId;
use serde::{Deserialize, Serialize};

/// A codec that can serialize and deserialize values using `serde_json`.
///
/// This codec uses ``serde_json`` to serialize and deserialize values.
///
/// # Example
///
/// ```rust
/// use sayiir_runtime::serialization::JsonCodec;
/// use sayiir_core::codec::*;
///
/// let codec = JsonCodec;
/// let value = 42;
/// let encoded = codec.encode(&value).unwrap();
/// let decoded: i32 = codec.decode(encoded).unwrap();
/// assert_eq!(decoded, value);
/// ```
#[derive(Default, Clone, Copy)]
pub struct JsonCodec;

impl Encoder for JsonCodec {}

impl CodecIdentity for JsonCodec {
    fn codec_id(&self) -> CodecId {
        CodecId::Json
    }
}

impl<T> sealed::EncodeValue<T> for JsonCodec
where
    T: Serialize,
{
    fn encode_value(&self, value: &T) -> Result<Bytes, BoxError> {
        Ok(Bytes::from(serde_json::to_vec(value)?))
    }
}

impl Decoder for JsonCodec {}

impl<T> sealed::DecodeValue<T> for JsonCodec
where
    T: for<'de> Deserialize<'de>,
{
    fn decode_value(&self, bytes: Bytes) -> Result<T, BoxError> {
        Ok(serde_json::from_slice(&bytes)?)
    }
}

impl sayiir_core::codec::EnvelopeCodec for JsonCodec {
    fn decode_string(&self, bytes: &[u8]) -> Result<String, BoxError> {
        Ok(serde_json::from_slice(bytes)?)
    }

    fn encode_branch_envelope(&self, key: &str, result_bytes: &[u8]) -> Result<Bytes, BoxError> {
        #[derive(Serialize)]
        struct Envelope<'a> {
            branch: &'a str,
            result: &'a serde_json::value::RawValue,
        }
        // RawValue splices the already-encoded result without building a
        // Value tree (validate-only parse, no per-node allocation).
        // Invalid result bytes degrade to a null result, as before.
        let result = serde_json::from_slice::<&serde_json::value::RawValue>(result_bytes)
            .or_else(|_| serde_json::from_slice(b"null"))?;
        Ok(Bytes::from(serde_json::to_vec(&Envelope {
            branch: key,
            result,
        })?))
    }

    fn encode_named_results(&self, results: &[(String, Bytes)]) -> Result<Bytes, BoxError> {
        let nbr = sayiir_core::branch_results::NamedBranchResults::new(results.to_vec());
        Ok(Bytes::from(serde_json::to_vec(&nbr)?))
    }
}
