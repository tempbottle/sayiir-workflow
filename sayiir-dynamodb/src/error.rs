//! Error mapping from AWS SDK to `BackendError`.

use sayiir_persistence::BackendError;

/// Newtype wrapper so we can implement `Into<BackendError>` without orphan rules.
pub(crate) struct DdbError(pub String);

impl From<DdbError> for BackendError {
    fn from(e: DdbError) -> Self {
        BackendError::Backend(e.0)
    }
}

/// Map an AWS SDK `SdkError` into a [`DdbError`].
pub(crate) fn sdk_err<E: std::fmt::Display>(e: E) -> DdbError {
    DdbError(e.to_string())
}
