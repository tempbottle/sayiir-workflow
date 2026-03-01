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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancel_rejected_on_completed() {
        let err = validate_signal_allowed("Completed", SignalKind::Cancel).unwrap_err();
        assert!(matches!(err, BackendError::CannotCancel(_)));
    }

    #[test]
    fn cancel_rejected_on_failed() {
        let err = validate_signal_allowed("Failed", SignalKind::Cancel).unwrap_err();
        assert!(matches!(err, BackendError::CannotCancel(_)));
    }

    #[test]
    fn cancel_allowed_on_in_progress() {
        validate_signal_allowed("InProgress", SignalKind::Cancel).unwrap();
    }

    #[test]
    fn cancel_allowed_on_paused() {
        validate_signal_allowed("Paused", SignalKind::Cancel).unwrap();
    }

    #[test]
    fn cancel_allowed_on_cancelled() {
        // Idempotent: cancelling an already-cancelled workflow is fine.
        validate_signal_allowed("Cancelled", SignalKind::Cancel).unwrap();
    }

    #[test]
    fn pause_rejected_on_completed() {
        let err = validate_signal_allowed("Completed", SignalKind::Pause).unwrap_err();
        assert!(matches!(err, BackendError::CannotPause(_)));
    }

    #[test]
    fn pause_rejected_on_failed() {
        let err = validate_signal_allowed("Failed", SignalKind::Pause).unwrap_err();
        assert!(matches!(err, BackendError::CannotPause(_)));
    }

    #[test]
    fn pause_rejected_on_cancelled() {
        let err = validate_signal_allowed("Cancelled", SignalKind::Pause).unwrap_err();
        assert!(matches!(err, BackendError::CannotPause(_)));
    }

    #[test]
    fn pause_allowed_on_in_progress() {
        validate_signal_allowed("InProgress", SignalKind::Pause).unwrap();
    }

    #[test]
    fn pause_allowed_on_paused() {
        // Idempotent: pausing an already-paused workflow is fine.
        validate_signal_allowed("Paused", SignalKind::Pause).unwrap();
    }

    // Unknown statuses are treated as permissive (forward compatibility).
    #[test]
    fn unknown_status_allows_cancel() {
        validate_signal_allowed("SomeFutureStatus", SignalKind::Cancel).unwrap();
    }

    #[test]
    fn unknown_status_allows_pause() {
        validate_signal_allowed("SomeFutureStatus", SignalKind::Pause).unwrap();
    }
}
