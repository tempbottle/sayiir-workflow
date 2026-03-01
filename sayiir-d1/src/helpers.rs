//! D1-specific helpers.

use chrono::{DateTime, Utc};

/// Format a `DateTime<Utc>` as an ISO 8601 string for SQLite TEXT columns.
pub(crate) fn dt_to_sqlite(dt: Option<DateTime<Utc>>) -> Option<String> {
    dt.map(|d| d.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string())
}
