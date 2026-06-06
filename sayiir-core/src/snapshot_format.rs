//! Durable snapshot wire format: a self-describing, versioned envelope.
//!
//! Every durable backend ([`sayiir-postgres`], [`sayiir-d1`]) stores a workflow
//! snapshot as an opaque byte blob. Historically that blob was *raw* codec
//! output with no header, which meant the bytes could not be identified,
//! version-checked, or safely migrated: a struct-layout change silently broke
//! `rkyv` decode, and switching codecs corrupted in-flight instances with a
//! cryptic error.
//!
//! This module defines the frozen v1 envelope that wraps the codec payload:
//!
//! ```text
//! offset  bytes  field
//! 0..4      4    magic           = b"SYRS"   (SaYiiR Snapshot)
//! 4         1    format_version  = 1
//! 5         1    codec_id        (1 = JSON, 2 = rkyv)
//! 6..       N    payload         = codec output of the snapshot
//! ```
//!
//! The header is a fixed 6 bytes. `format_version` advances independently of the
//! magic, so a future incompatible layout bumps the version (and ships a
//! compatible read path) while keeping the magic stable for identification.
//!
//! All operations here are pure byte manipulation with no dependency on the
//! standard library beyond [`Vec`], keeping the format **wasm-safe** for the
//! Cloudflare Workers / D1 path.
//!
//! See `docs/FORMAT.md` for the normative specification and compatibility policy.
//!
//! [`sayiir-postgres`]: https://docs.rs/sayiir-postgres
//! [`sayiir-d1`]: https://docs.rs/sayiir-d1

use core::fmt;

/// Magic prefix identifying a Sayiir durable snapshot blob (`b"SYRS"`).
pub const SNAPSHOT_MAGIC: [u8; 4] = *b"SYRS";

/// Current durable snapshot format version.
///
/// Frozen at `1` for the 1.0 release line. A bump signals an envelope or
/// payload-layout change that older readers cannot decode; readers reject any
/// version they do not understand (see [`FormatError::UnsupportedVersion`]).
pub const SNAPSHOT_FORMAT_VERSION: u8 = 1;

/// Total length of the fixed envelope header (magic + version + codec id).
pub const HEADER_LEN: usize = SNAPSHOT_MAGIC.len() + 2;

/// Identifies which codec produced the framed payload.
///
/// Persisted as a single byte in the envelope so a reader can detect — and
/// reject with a clear error — a blob written by a different codec than the one
/// it is configured with, instead of silently mis-decoding the bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum CodecId {
    /// `serde_json` payload. The recommended durable default: tolerant to
    /// additive optional fields.
    Json = 1,
    /// `rkyv` zero-copy payload. Fast, but archives are fragile across
    /// struct-layout changes — see the compatibility policy in `docs/FORMAT.md`.
    Rkyv = 2,
}

impl CodecId {
    /// The wire byte for this codec.
    #[must_use]
    pub const fn as_u8(self) -> u8 {
        self as u8
    }

    /// Parse a codec id from its wire byte.
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::UnknownCodec`] if the byte does not correspond to
    /// a known codec.
    pub const fn from_u8(byte: u8) -> Result<Self, FormatError> {
        match byte {
            1 => Ok(Self::Json),
            2 => Ok(Self::Rkyv),
            other => Err(FormatError::UnknownCodec(other)),
        }
    }

    /// Human-readable codec name, for diagnostics.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Json => "json",
            Self::Rkyv => "rkyv",
        }
    }
}

/// A decoded snapshot envelope: header fields plus a borrowed view of the payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotFrame<'a> {
    /// Format version read from the header.
    pub format_version: u8,
    /// Codec that produced [`payload`](Self::payload).
    pub codec_id: CodecId,
    /// The raw codec payload (everything after the header).
    pub payload: &'a [u8],
}

/// Errors that can occur while parsing a durable snapshot envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FormatError {
    /// The blob does not begin with [`SNAPSHOT_MAGIC`]. For a 1.0+ reader this
    /// almost always means a pre-1.0 (headerless) snapshot.
    MissingMagic,
    /// The blob is shorter than the fixed header.
    Truncated,
    /// The format version is not understood by this reader.
    UnsupportedVersion(u8),
    /// The codec id byte does not map to a known codec.
    UnknownCodec(u8),
    /// The blob's codec does not match the codec the reader is configured with.
    CodecMismatch {
        /// Codec the reader was configured with.
        expected: CodecId,
        /// Codec recorded in the blob.
        found: CodecId,
    },
}

impl fmt::Display for FormatError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingMagic => write!(
                f,
                "not a Sayiir 1.0+ snapshot (missing magic header). This looks \
                 like a pre-1.0 snapshot, which the 1.0 wire format does not read: \
                 drain in-flight workflows on the old version before upgrading."
            ),
            Self::Truncated => write!(
                f,
                "snapshot blob is truncated (shorter than the {HEADER_LEN}-byte header)"
            ),
            Self::UnsupportedVersion(v) => write!(
                f,
                "unsupported snapshot format version {v} (this build understands \
                 version {SNAPSHOT_FORMAT_VERSION}); upgrade Sayiir to read it"
            ),
            Self::UnknownCodec(b) => write!(f, "unknown snapshot codec id: {b}"),
            Self::CodecMismatch { expected, found } => write!(
                f,
                "snapshot codec mismatch: blob was written with `{}` but this \
                 backend is configured with `{}`. Configure the matching codec, \
                 or drain and re-run these workflows under the new codec.",
                found.name(),
                expected.name()
            ),
        }
    }
}

impl std::error::Error for FormatError {}

/// Wrap a codec payload in the v1 envelope.
///
/// Produces `magic ++ [SNAPSHOT_FORMAT_VERSION] ++ [codec_id] ++ payload`.
#[must_use]
pub fn frame_snapshot(codec_id: CodecId, payload: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(HEADER_LEN + payload.len());
    buf.extend_from_slice(&SNAPSHOT_MAGIC);
    buf.push(SNAPSHOT_FORMAT_VERSION);
    buf.push(codec_id.as_u8());
    buf.extend_from_slice(payload);
    buf
}

/// Parse a v1 envelope, returning the header fields and a borrowed payload.
///
/// Validates the magic, format version, and codec id, but does **not** check the
/// codec against any configured codec — use [`unframe_snapshot_checked`] for that.
///
/// # Errors
///
/// - [`FormatError::Truncated`] if shorter than the header.
/// - [`FormatError::MissingMagic`] if the magic prefix is absent.
/// - [`FormatError::UnsupportedVersion`] if the version is not [`SNAPSHOT_FORMAT_VERSION`].
/// - [`FormatError::UnknownCodec`] if the codec byte is unrecognized.
pub fn unframe_snapshot(bytes: &[u8]) -> Result<SnapshotFrame<'_>, FormatError> {
    // Header layout: magic(4) | version(1) | codec_id(1) | payload..
    // A blob that doesn't start with the magic is treated as missing-magic
    // (almost always a pre-1.0 / legacy blob), even if it is shorter than the
    // magic itself. A blob with valid magic but cut short is `Truncated`.
    let after_magic = bytes
        .strip_prefix(&SNAPSHOT_MAGIC)
        .ok_or(FormatError::MissingMagic)?;
    let (&format_version, after_version) =
        after_magic.split_first().ok_or(FormatError::Truncated)?;
    if format_version != SNAPSHOT_FORMAT_VERSION {
        return Err(FormatError::UnsupportedVersion(format_version));
    }
    let (&codec_byte, payload) = after_version.split_first().ok_or(FormatError::Truncated)?;
    let codec_id = CodecId::from_u8(codec_byte)?;
    Ok(SnapshotFrame {
        format_version,
        codec_id,
        payload,
    })
}

/// Parse a v1 envelope and assert its codec matches `expected`.
///
/// # Errors
///
/// Everything [`unframe_snapshot`] can return, plus
/// [`FormatError::CodecMismatch`] if the blob's codec differs from `expected`.
pub fn unframe_snapshot_checked(
    bytes: &[u8],
    expected: CodecId,
) -> Result<SnapshotFrame<'_>, FormatError> {
    let frame = unframe_snapshot(bytes)?;
    if frame.codec_id != expected {
        return Err(FormatError::CodecMismatch {
            expected,
            found: frame.codec_id,
        });
    }
    Ok(frame)
}

/// Encode a [`WorkflowSnapshot`] with `codec` and wrap it in the v1 envelope.
///
/// This is the single durable-encode boundary shared by every backend: the
/// envelope's `codec_id` is taken from the codec itself, so a backend never
/// needs to know or hard-code which codec it uses.
///
/// # Errors
///
/// Returns the codec's serialization error if encoding fails.
pub fn encode_framed<C>(
    codec: &C,
    snapshot: &crate::snapshot::WorkflowSnapshot,
) -> Result<Vec<u8>, crate::error::BoxError>
where
    C: crate::codec::Encoder
        + crate::codec::CodecIdentity
        + crate::codec::sealed::EncodeValue<crate::snapshot::WorkflowSnapshot>,
{
    let payload = codec.encode(snapshot)?;
    Ok(frame_snapshot(codec.codec_id(), &payload))
}

/// Decode a [`WorkflowSnapshot`] from a v1-enveloped blob with `codec`.
///
/// The shared durable-decode boundary: validates the envelope and that its
/// `codec_id` matches `codec`, then decodes the payload. The
/// [`FormatError`] cases (missing magic, version, codec mismatch) surface with
/// their guidance intact.
///
/// # Errors
///
/// Returns a [`FormatError`] if the envelope is invalid or its codec differs
/// from `codec`, or the codec's deserialization error if decoding fails.
pub fn decode_framed<C>(
    codec: &C,
    data: &[u8],
) -> Result<crate::snapshot::WorkflowSnapshot, crate::error::BoxError>
where
    C: crate::codec::Decoder
        + crate::codec::CodecIdentity
        + crate::codec::sealed::DecodeValue<crate::snapshot::WorkflowSnapshot>,
{
    let frame = unframe_snapshot_checked(data, codec.codec_id())?;
    codec.decode(bytes::Bytes::copy_from_slice(frame.payload))
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
mod tests {
    use super::*;

    #[test]
    fn header_layout_is_frozen() {
        // This asserts the exact on-the-wire header. If this test fails, the
        // durable format changed — that MUST be accompanied by a format_version
        // bump and a migrator, never a silent edit.
        let framed = frame_snapshot(CodecId::Json, b"payload");
        assert_eq!(&framed[0..4], b"SYRS", "magic");
        assert_eq!(framed[4], 1, "format_version");
        assert_eq!(framed[5], 1, "codec_id json");
        assert_eq!(&framed[6..], b"payload", "payload follows header");
        assert_eq!(framed.len(), HEADER_LEN + 7);
    }

    #[test]
    fn rkyv_codec_id_byte() {
        let framed = frame_snapshot(CodecId::Rkyv, b"x");
        assert_eq!(framed[5], 2);
    }

    #[test]
    fn roundtrip() {
        for codec in [CodecId::Json, CodecId::Rkyv] {
            let payload = b"some opaque codec bytes \x00\x01\x02";
            let framed = frame_snapshot(codec, payload);
            let frame = unframe_snapshot(&framed).expect("parse");
            assert_eq!(frame.format_version, SNAPSHOT_FORMAT_VERSION);
            assert_eq!(frame.codec_id, codec);
            assert_eq!(frame.payload, payload);
        }
    }

    #[test]
    fn empty_payload_roundtrips() {
        let framed = frame_snapshot(CodecId::Json, b"");
        let frame = unframe_snapshot(&framed).expect("parse");
        assert_eq!(frame.payload, b"");
    }

    #[test]
    fn missing_magic_is_detected() {
        // A bare JSON blob (the legacy v0 shape) has no magic.
        let legacy = br#"{"instance_id":"wf-1"}"#;
        assert_eq!(unframe_snapshot(legacy), Err(FormatError::MissingMagic));
        // The legacy decode-error message points at the migration path.
        assert!(FormatError::MissingMagic.to_string().contains("pre-1.0"));
    }

    #[test]
    fn short_blob_without_magic_is_missing_magic_not_truncated() {
        // 3 bytes that are not the magic prefix.
        assert_eq!(unframe_snapshot(b"abc"), Err(FormatError::MissingMagic));
    }

    #[test]
    fn truncated_after_valid_magic() {
        // Valid magic but cut off before the codec id byte.
        let mut buf = SNAPSHOT_MAGIC.to_vec();
        buf.push(SNAPSHOT_FORMAT_VERSION); // missing codec id
        assert_eq!(unframe_snapshot(&buf), Err(FormatError::Truncated));
    }

    #[test]
    fn unsupported_version_is_rejected() {
        let mut framed = frame_snapshot(CodecId::Json, b"x");
        framed[4] = 2; // a future version
        assert_eq!(
            unframe_snapshot(&framed),
            Err(FormatError::UnsupportedVersion(2))
        );
    }

    #[test]
    fn unknown_codec_is_rejected() {
        let mut framed = frame_snapshot(CodecId::Json, b"x");
        framed[5] = 99;
        assert_eq!(
            unframe_snapshot(&framed),
            Err(FormatError::UnknownCodec(99))
        );
    }

    #[test]
    fn codec_mismatch_is_detected() {
        let framed = frame_snapshot(CodecId::Json, b"x");
        assert_eq!(
            unframe_snapshot_checked(&framed, CodecId::Rkyv),
            Err(FormatError::CodecMismatch {
                expected: CodecId::Rkyv,
                found: CodecId::Json,
            })
        );
        // Matching codec passes.
        assert!(unframe_snapshot_checked(&framed, CodecId::Json).is_ok());
    }

    #[test]
    fn codec_id_byte_roundtrip() {
        assert_eq!(CodecId::from_u8(1), Ok(CodecId::Json));
        assert_eq!(CodecId::from_u8(2), Ok(CodecId::Rkyv));
        assert_eq!(CodecId::from_u8(0), Err(FormatError::UnknownCodec(0)));
        assert_eq!(CodecId::Json.as_u8(), 1);
        assert_eq!(CodecId::Rkyv.as_u8(), 2);
    }
}

/// Golden-blob freeze tests.
///
/// These lock the **JSON wire bytes** of a representative [`WorkflowSnapshot`]
/// (the durable v1 payload under [`CodecId::Json`]). If a change to the snapshot
/// struct alters the serialized form, these tests fail — which is the point:
/// such a change is incompatible and MUST be accompanied by a
/// [`SNAPSHOT_FORMAT_VERSION`] bump, never a silent edit.
#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::print_stdout
)]
mod golden {
    use super::*;
    use crate::snapshot::{
        ExecutionPosition, TaskDeadline, TaskResult, TaskRetryState, WorkflowSnapshot,
        WorkflowSnapshotState,
    };
    use crate::task::RetryPolicy;
    use bytes::Bytes;
    use chrono::DateTime;
    use std::collections::HashMap;
    use std::time::Duration;

    /// Build a deterministic snapshot exercising the durable fields: an
    /// in-progress position, a completed task with output, a task deadline,
    /// retry state, a loop iteration counter, priority, and tags. All
    /// timestamps and hashes are fixed so the serialized bytes are stable.
    fn deterministic_snapshot() -> WorkflowSnapshot {
        let task = crate::TaskId::from_bytes([0x11; 32]);
        let dt = DateTime::from_timestamp(1_700_000_000, 0).unwrap();

        let mut completed = std::collections::HashMap::default();
        completed.insert(
            task,
            TaskResult {
                task_id: task,
                output: Bytes::from_static(b"\x01\x02\x03"),
            },
        );

        let mut task_retries = HashMap::new();
        task_retries.insert(
            crate::TaskId::from_bytes([0x22; 32]),
            TaskRetryState {
                attempts: 2,
                policy: RetryPolicy {
                    max_retries: 3,
                    initial_delay: Duration::from_millis(100),
                    backoff_multiplier: 2.0,
                    max_delay: Some(Duration::from_secs(10)),
                },
                last_error: "boom".to_string(),
                last_failed_worker: Some("worker-7".to_string()),
                next_retry_at: dt,
            },
        );

        let mut loop_iterations = HashMap::new();
        loop_iterations.insert(crate::TaskId::from_bytes([0x33; 32]), 4u32);

        WorkflowSnapshot {
            instance_id: std::sync::Arc::from("wf-golden-1"),
            definition_hash: crate::DefinitionHash::from_bytes([0x44; 32]),
            state: WorkflowSnapshotState::InProgress {
                position: ExecutionPosition::AtTask { task_id: task },
                completed_tasks: completed,
                last_completed_task_id: Some(task),
            },
            created_at: 1_700_000_000,
            updated_at: 1_700_000_500,
            initial_input: Some(Bytes::from_static(b"\"hi\"")),
            task_deadline: Some(TaskDeadline {
                task_id: task,
                deadline: dt,
                timeout_ms: 30_000,
            }),
            task_retries,
            loop_iterations,
            task_priority: Some(3),
            task_tags: vec!["gpu".to_string(), "cuda-12".to_string()],
            trace_parent: None,
            output_unflushed: false,
        }
    }

    /// The frozen v1/JSON envelope for [`deterministic_snapshot`], hex-encoded.
    ///
    /// Regenerate ONLY alongside a deliberate, documented format change (and a
    /// [`SNAPSHOT_FORMAT_VERSION`] bump) by running this module's
    /// `golden_blob_is_frozen` test with `--nocapture` and pasting the printed value.
    const GOLDEN_HEX: &str = "5359525301017b22696e7374616e63655f6964223a2277662d676f6c64656e2d31222c22646566696e6974696f6e5f68617368223a2234343434343434343434343434343434343434343434343434343434343434343434343434343434343434343434343434343434343434343434343434343434222c227374617465223a7b22496e50726f6772657373223a7b22706f736974696f6e223a7b2241745461736b223a7b227461736b5f6964223a2231313131313131313131313131313131313131313131313131313131313131313131313131313131313131313131313131313131313131313131313131313131227d7d2c22636f6d706c657465645f7461736b73223a7b2231313131313131313131313131313131313131313131313131313131313131313131313131313131313131313131313131313131313131313131313131313131223a7b227461736b5f6964223a2231313131313131313131313131313131313131313131313131313131313131313131313131313131313131313131313131313131313131313131313131313131222c226f7574707574223a5b312c322c335d7d7d2c226c6173745f636f6d706c657465645f7461736b5f6964223a2231313131313131313131313131313131313131313131313131313131313131313131313131313131313131313131313131313131313131313131313131313131227d7d2c22637265617465645f6174223a313730303030303030302c22757064617465645f6174223a313730303030303530302c22696e697469616c5f696e707574223a5b33342c3130342c3130352c33345d2c227461736b5f646561646c696e65223a7b227461736b5f6964223a2231313131313131313131313131313131313131313131313131313131313131313131313131313131313131313131313131313131313131313131313131313131222c22646561646c696e65223a22323032332d31312d31345432323a31333a32305a222c2274696d656f75745f6d73223a33303030307d2c227461736b5f72657472696573223a7b2232323232323232323232323232323232323232323232323232323232323232323232323232323232323232323232323232323232323232323232323232323232223a7b22617474656d707473223a322c22706f6c696379223a7b226d61785f72657472696573223a332c22696e697469616c5f64656c6179223a7b2273656373223a302c226e616e6f73223a3130303030303030307d2c226261636b6f66665f6d756c7469706c696572223a322e302c226d61785f64656c6179223a7b2273656373223a31302c226e616e6f73223a307d7d2c226c6173745f6572726f72223a22626f6f6d222c226c6173745f6661696c65645f776f726b6572223a22776f726b65722d37222c226e6578745f72657472795f6174223a22323032332d31312d31345432323a31333a32305a227d7d2c226c6f6f705f697465726174696f6e73223a7b2233333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333223a347d2c227461736b5f7072696f72697479223a332c227461736b5f74616773223a5b22677075222c22637564612d3132225d7d";

    #[test]
    fn golden_blob_is_frozen() {
        let snap = deterministic_snapshot();
        let payload = serde_json::to_vec(&snap).expect("serialize");
        let framed = frame_snapshot(CodecId::Json, &payload);
        let actual_hex = hex::encode(&framed);

        // Helpful on first run / intentional regeneration.
        println!("GOLDEN_HEX = \"{actual_hex}\"");

        let golden = hex::decode(GOLDEN_HEX).unwrap_or_default();
        assert_eq!(
            framed, golden,
            "durable snapshot wire format changed. If intentional, bump \
             SNAPSHOT_FORMAT_VERSION and regenerate GOLDEN_HEX from the printed value."
        );
    }

    #[test]
    fn golden_blob_decodes_back() {
        // Independent of GOLDEN_HEX: prove the envelope round-trips a rich snapshot.
        let snap = deterministic_snapshot();
        let payload = serde_json::to_vec(&snap).unwrap();
        let framed = frame_snapshot(CodecId::Json, &payload);

        let frame = unframe_snapshot_checked(&framed, CodecId::Json).expect("parse");
        let decoded: WorkflowSnapshot = serde_json::from_slice(frame.payload).expect("decode");

        assert_eq!(decoded.instance_id.as_ref(), "wf-golden-1");
        assert_eq!(decoded.task_priority, Some(3));
        assert_eq!(decoded.task_tags, vec!["gpu", "cuda-12"]);
        assert_eq!(decoded.created_at, 1_700_000_000);
        assert!(decoded.task_deadline.is_some());
        assert_eq!(decoded.task_retries.len(), 1);
        assert_eq!(decoded.loop_iterations.values().copied().next(), Some(4));

        // Re-encoding the decoded snapshot reproduces the exact bytes.
        let re = frame_snapshot(CodecId::Json, &serde_json::to_vec(&decoded).unwrap());
        assert_eq!(re, framed, "re-encode is not byte-stable");
    }
}
