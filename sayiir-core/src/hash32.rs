//! Fixed-length 32-byte identifiers backed by SHA-256.
//!
//! [`Hash32`] is a primitive value type — a `[u8; 32]` with cheap `Copy`,
//! constant-time-equivalent comparison (single SIMD-friendly memcmp), and a
//! single hash-map probe instead of the per-character hashing a `String`
//! incurs. It is the building block for the semantic id newtypes (currently
//! [`DefinitionHash`]; `WorkflowId` and `TaskId` will follow in Phase 2).
//!
//! Serde encodes a `Hash32` as a 64-character lowercase hex string on
//! human-readable formats (JSON, TOML) and as raw 32 bytes on binary formats
//! (bincode, rkyv-derived codecs). This keeps user-facing snapshot blobs and
//! API payloads readable while making over-the-wire transports compact.

use core::fmt;
use core::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest, Sha256};

/// A 32-byte fixed-length identifier, typically the output of SHA-256.
///
/// Stored inline as `[u8; 32]` — no heap allocation, no length prefix, `Copy`,
/// and trivially `Hash`/`Eq` (one memcmp). `Default` returns [`Hash32::ZERO`]
/// (all-zero bytes) so semantic newtypes wrapping it can be `Default` too.
#[derive(Clone, Copy, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Hash32([u8; 32]);

impl Hash32 {
    /// Zero hash — useful as a sentinel for "uninitialised". Not a valid
    /// SHA-256 output in practice.
    pub const ZERO: Self = Self([0u8; 32]);

    /// Construct from raw bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Borrow the underlying bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Consume into the underlying bytes.
    #[must_use]
    pub const fn into_bytes(self) -> [u8; 32] {
        self.0
    }

    /// Compute SHA-256 of the given input.
    #[must_use]
    pub fn sha256(input: impl AsRef<[u8]>) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(input.as_ref());
        Self::from_digest(hasher)
    }

    /// Finalise a hasher into a `Hash32`.
    #[must_use]
    pub fn from_digest(hasher: Sha256) -> Self {
        let out = hasher.finalize();
        let mut bytes = [0u8; 32];
        bytes.copy_from_slice(&out);
        Self(bytes)
    }

    /// Lowercase hex encoding (64 chars). Allocates.
    #[must_use]
    pub fn to_hex(&self) -> String {
        use fmt::Write as _;
        let mut s = String::with_capacity(64);
        for byte in &self.0 {
            let _ = write!(&mut s, "{byte:02x}");
        }
        s
    }

    /// Parse from a 64-character lowercase or uppercase hex string.
    ///
    /// # Errors
    ///
    /// Returns [`Hash32ParseError`] if the input is not 64 hex digits.
    pub fn from_hex(s: &str) -> Result<Self, Hash32ParseError> {
        let bytes_in = s.as_bytes();
        if bytes_in.len() != 64 {
            return Err(Hash32ParseError::WrongLength(bytes_in.len()));
        }
        let mut bytes = [0u8; 32];
        for (i, byte) in bytes.iter_mut().enumerate() {
            let lo = bytes_in.get(i * 2).copied().unwrap_or(0);
            let hi = bytes_in.get(i * 2 + 1).copied().unwrap_or(0);
            *byte = (decode_nibble(lo)? << 4) | decode_nibble(hi)?;
        }
        Ok(Self(bytes))
    }
}

#[inline]
fn decode_nibble(c: u8) -> Result<u8, Hash32ParseError> {
    match c {
        b'0'..=b'9' => Ok(c - b'0'),
        b'a'..=b'f' => Ok(c - b'a' + 10),
        b'A'..=b'F' => Ok(c - b'A' + 10),
        _ => Err(Hash32ParseError::InvalidChar(c)),
    }
}

/// Errors from parsing a hex-encoded [`Hash32`].
#[derive(Debug, thiserror::Error)]
pub enum Hash32ParseError {
    /// Input length was not exactly 64 hex characters.
    #[error("expected 64 hex characters, got {0}")]
    WrongLength(usize),
    /// Non-hex byte encountered.
    #[error("invalid hex character: {:?}", *.0 as char)]
    InvalidChar(u8),
}

impl fmt::Display for Hash32 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in &self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl fmt::Debug for Hash32 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl FromStr for Hash32 {
    type Err = Hash32ParseError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_hex(s)
    }
}

impl AsRef<[u8]> for Hash32 {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl Serialize for Hash32 {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            serializer.collect_str(self)
        } else {
            serializer.serialize_bytes(&self.0)
        }
    }
}

impl<'de> Deserialize<'de> for Hash32 {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        if deserializer.is_human_readable() {
            let s = <&str>::deserialize(deserializer)?;
            Self::from_hex(s).map_err(serde::de::Error::custom)
        } else {
            struct V;
            impl<'de> serde::de::Visitor<'de> for V {
                type Value = Hash32;
                fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                    f.write_str("32 raw bytes")
                }
                fn visit_bytes<E: serde::de::Error>(self, v: &[u8]) -> Result<Hash32, E> {
                    if v.len() != 32 {
                        return Err(E::invalid_length(v.len(), &self));
                    }
                    let mut bytes = [0u8; 32];
                    bytes.copy_from_slice(v);
                    Ok(Hash32(bytes))
                }
                fn visit_borrowed_bytes<E: serde::de::Error>(
                    self,
                    v: &'de [u8],
                ) -> Result<Hash32, E> {
                    self.visit_bytes(v)
                }
                fn visit_byte_buf<E: serde::de::Error>(self, v: Vec<u8>) -> Result<Hash32, E> {
                    self.visit_bytes(&v)
                }
                fn visit_seq<A: serde::de::SeqAccess<'de>>(
                    self,
                    mut seq: A,
                ) -> Result<Hash32, A::Error> {
                    let mut bytes = [0u8; 32];
                    for (i, byte) in bytes.iter_mut().enumerate() {
                        *byte = seq
                            .next_element()?
                            .ok_or_else(|| serde::de::Error::invalid_length(i, &"32 bytes"))?;
                    }
                    Ok(Hash32(bytes))
                }
            }
            deserializer.deserialize_bytes(V)
        }
    }
}

// ============================================================================
// Semantic newtypes — `DefinitionHash` and `TaskId`.
//
// Both are `Hash32`-backed but distinct at the type level so the compiler
// catches mixups. The macro keeps their surface in lock-step.
// ============================================================================

/// Define a `Hash32`-backed semantic newtype with the standard surface:
/// `Copy`/`Eq`/`Hash`, hex `Display`, hex `FromStr`, transparent serde,
/// and `From<&str>`/`From<String>` that **hash** the input via SHA-256
/// (not hex-parse). The hex round-trip path is `from_hex` / `to_hex`.
macro_rules! hash32_newtype {
    (
        $(#[$attr:meta])*
        $name:ident
    ) => {
        $(#[$attr])*
        #[derive(
            Clone, Copy, Default, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
        )]
        #[serde(transparent)]
        pub struct $name(Hash32);

        impl $name {
            #[doc = concat!("Construct a [`", stringify!($name), "`] from a [`Hash32`].")]
            #[must_use]
            pub const fn from_hash(hash: Hash32) -> Self { Self(hash) }

            #[doc = concat!("Construct a [`", stringify!($name), "`] from raw 32 bytes.")]
            #[must_use]
            pub const fn from_bytes(bytes: [u8; 32]) -> Self {
                Self(Hash32::from_bytes(bytes))
            }

            /// SHA-256-hash the given input and wrap the result.
            ///
            /// This is the canonical way to mint a fresh id from a
            /// human-readable name at construction time.
            #[must_use]
            pub fn sha256(input: impl AsRef<[u8]>) -> Self {
                Self(Hash32::sha256(input))
            }

            /// Borrow the underlying [`Hash32`].
            #[must_use]
            pub const fn as_hash(&self) -> &Hash32 { &self.0 }

            /// Borrow the raw 32 bytes.
            #[must_use]
            pub const fn as_bytes(&self) -> &[u8; 32] { self.0.as_bytes() }

            /// Lowercase hex encoding (64 chars).
            #[must_use]
            pub fn to_hex(&self) -> String { self.0.to_hex() }

            /// Parse from a 64-character hex string.
            ///
            /// # Errors
            ///
            /// See [`Hash32::from_hex`].
            pub fn from_hex(s: &str) -> Result<Self, Hash32ParseError> {
                Hash32::from_hex(s).map(Self)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt::Display::fmt(&self.0, f)
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, concat!(stringify!($name), "({})"), self.0)
            }
        }

        impl FromStr for $name {
            type Err = Hash32ParseError;
            fn from_str(s: &str) -> Result<Self, Self::Err> { Self::from_hex(s) }
        }

        impl From<Hash32> for $name {
            fn from(h: Hash32) -> Self { Self(h) }
        }

        impl From<$name> for Hash32 {
            fn from(h: $name) -> Self { h.0 }
        }

        // NOTE: `From<&str>` / `From<String>` SHA-256-hash the input. They do
        // **not** parse a hex hash — use `from_hex` for that. These impls keep
        // builder APIs and test fixtures (`"task-1".into()`) ergonomic.
        impl From<&str> for $name {
            fn from(s: &str) -> Self { Self::sha256(s.as_bytes()) }
        }

        impl From<String> for $name {
            fn from(s: String) -> Self { Self::sha256(s.as_bytes()) }
        }
    };
}

hash32_newtype! {
    /// SHA-256 fingerprint of a workflow's structural definition.
    ///
    /// Computed from the workflow's continuation tree (task IDs, retry
    /// policies, fork shapes, delays, signals, loops, child workflows). Used
    /// by the runtime to detect when a serialised snapshot was written
    /// against a different workflow definition than the one currently in
    /// memory.
    ///
    /// Compares in a single 32-byte memcmp instead of a 64-character string
    /// equality, and hashes to one `u64` instead of per-character siphash.
    DefinitionHash
}

hash32_newtype! {
    /// SHA-256 hash of a task's user-facing name (e.g. `"validate"`).
    ///
    /// Used as a precomputed lookup key alongside the human-readable id on
    /// hot data structures (e.g. [`TaskHint`](crate::snapshot::TaskHint)). The
    /// runtime stores the name (`String`) on
    /// [`ExecutionPosition`](crate::snapshot::ExecutionPosition) variants for
    /// readability of serialised snapshots; the `TaskId` is what comparisons
    /// and map probes use when callers have access to it.
    ///
    /// 32-byte memcmp + single-`u64` hash, same wins as
    /// [`DefinitionHash`].
    TaskId
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn hash32_round_trips_via_hex() {
        let h = Hash32::sha256(b"hello world");
        let hex = h.to_hex();
        assert_eq!(hex.len(), 64);
        assert_eq!(Hash32::from_hex(&hex).unwrap(), h);
    }

    #[test]
    fn from_hex_rejects_wrong_length() {
        assert!(matches!(
            Hash32::from_hex("abc"),
            Err(Hash32ParseError::WrongLength(3))
        ));
    }

    #[test]
    fn from_hex_rejects_non_hex_char() {
        let bad = "z".repeat(64);
        assert!(matches!(
            Hash32::from_hex(&bad),
            Err(Hash32ParseError::InvalidChar(b'z'))
        ));
    }

    #[test]
    fn from_hex_accepts_uppercase() {
        let lower = "abcd".repeat(16);
        let upper = "ABCD".repeat(16);
        assert_eq!(
            Hash32::from_hex(&lower).unwrap(),
            Hash32::from_hex(&upper).unwrap()
        );
    }

    #[test]
    fn json_round_trip_is_hex_string() {
        let h = Hash32::sha256(b"payload");
        let json = serde_json::to_string(&h).unwrap();
        assert_eq!(json.len(), 66);
        assert!(json.starts_with('"') && json.ends_with('"'));
        let parsed: Hash32 = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, h);
    }

    #[test]
    fn definition_hash_displays_as_hex() {
        let d = DefinitionHash::from_bytes([0xab; 32]);
        assert_eq!(format!("{d}"), "ab".repeat(32));
    }

    #[test]
    fn definition_hash_serde_transparent() {
        let d = DefinitionHash::from_hash(Hash32::sha256(b"wf"));
        let as_hash_json = serde_json::to_string(d.as_hash()).unwrap();
        let as_def_json = serde_json::to_string(&d).unwrap();
        assert_eq!(as_hash_json, as_def_json);
    }

    #[test]
    fn definition_hash_round_trips_via_hex() {
        let d = DefinitionHash::from_hash(Hash32::sha256(b"abc"));
        let parsed: DefinitionHash = d.to_hex().parse().unwrap();
        assert_eq!(parsed, d);
    }

    #[test]
    fn definition_hash_from_str_hashes_input() {
        let by_str: DefinitionHash = "wf-1".into();
        let by_sha = DefinitionHash::sha256(b"wf-1");
        assert_eq!(by_str, by_sha);
    }
}
