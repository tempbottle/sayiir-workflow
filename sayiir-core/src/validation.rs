//! Input validation primitives applied at workflow-API entry points.
//!
//! These bounds keep user-supplied identifiers within sensible limits before
//! they touch persistence: DB indexes, NOTIFY payloads, and log fields all
//! degrade if an instance id can be arbitrarily large.

/// Maximum byte length of a workflow `instance_id`.
///
/// Measured in bytes so the bound matches database storage exactly (Postgres
/// `TEXT` is unbounded but practical index/B-tree behaviour expects short
/// keys). 256 bytes comfortably covers UUIDs, ULIDs, ULID+prefix, and the
/// usual `"<service>-<correlation>"` style without invoking arbitrary user
/// data as a primary-key column.
pub const MAX_INSTANCE_ID_LEN: usize = 256;

/// Reasons [`validate_instance_id`] may reject an input.
#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum InvalidInstanceId {
    /// The supplied id was empty.
    #[error("instance_id must not be empty")]
    Empty,
    /// The supplied id exceeded [`MAX_INSTANCE_ID_LEN`] bytes.
    #[error("instance_id length {len} exceeds maximum {max} bytes")]
    TooLong {
        /// Length of the rejected id in bytes.
        len: usize,
        /// The enforced maximum (= [`MAX_INSTANCE_ID_LEN`]).
        max: usize,
    },
}

/// Validate a user-supplied workflow `instance_id` at the API boundary.
///
/// Run/resume entry points call this once before any DB I/O so a malformed id
/// fails fast and consistently — same error shape across single-process,
/// distributed, and FFI runners.
///
/// # Errors
///
/// - [`InvalidInstanceId::Empty`] if `instance_id` is the empty string.
/// - [`InvalidInstanceId::TooLong`] if `instance_id.len() > MAX_INSTANCE_ID_LEN`.
pub fn validate_instance_id(instance_id: &str) -> Result<(), InvalidInstanceId> {
    if instance_id.is_empty() {
        return Err(InvalidInstanceId::Empty);
    }
    if instance_id.len() > MAX_INSTANCE_ID_LEN {
        return Err(InvalidInstanceId::TooLong {
            len: instance_id.len(),
            max: MAX_INSTANCE_ID_LEN,
        });
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn accepts_typical_ids() {
        validate_instance_id("a").unwrap();
        validate_instance_id("order-12345").unwrap();
        validate_instance_id("01HQK4P9P0V7S6XQGZ9N3M2RF7").unwrap(); // ULID
    }

    #[test]
    fn rejects_empty() {
        assert_eq!(validate_instance_id(""), Err(InvalidInstanceId::Empty));
    }

    #[test]
    fn accepts_exactly_max() {
        let s = "x".repeat(MAX_INSTANCE_ID_LEN);
        validate_instance_id(&s).unwrap();
    }

    #[test]
    fn rejects_one_byte_too_long() {
        let s = "x".repeat(MAX_INSTANCE_ID_LEN + 1);
        let err = validate_instance_id(&s).unwrap_err();
        assert_eq!(
            err,
            InvalidInstanceId::TooLong {
                len: MAX_INSTANCE_ID_LEN + 1,
                max: MAX_INSTANCE_ID_LEN,
            }
        );
    }
}
