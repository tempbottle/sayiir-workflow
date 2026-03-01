//! Shared validation helpers for persistence backends.

use sayiir_core::snapshot::{SignalKind, SnapshotStatus};

use crate::BackendError;

/// Validate that a signal can be sent to a workflow in the given state.
///
/// Returns `Ok(())` if the signal is allowed, or a [`BackendError`] describing
/// why it cannot be delivered. Unknown status strings (forward-compatibility)
/// are treated as permissive.
pub fn validate_signal_allowed(status: &str, kind: SignalKind) -> Result<(), BackendError> {
    use std::str::FromStr;

    let Ok(status) = SnapshotStatus::from_str(status) else {
        // Unknown status from DB — be permissive (forward compatibility).
        return Ok(());
    };

    match kind {
        SignalKind::Cancel => match status {
            SnapshotStatus::Completed | SnapshotStatus::Failed => {
                Err(BackendError::CannotCancel(status.as_ref().to_string()))
            }
            _ => Ok(()),
        },
        SignalKind::Pause => match status {
            SnapshotStatus::Completed | SnapshotStatus::Failed | SnapshotStatus::Cancelled => {
                Err(BackendError::CannotPause(status.as_ref().to_string()))
            }
            _ => Ok(()),
        },
    }
}
