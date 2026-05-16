//! Embedded migration SQL for D1 / `SQLite` schema initialization.
//!
//! Migrations are versioned and tracked via `SQLite`'s `PRAGMA user_version`.
//! [`run_migrations`](crate::backend::SQLiteBackend::run_migrations) reads
//! the current version, applies every migration whose version is greater,
//! and bumps `user_version` to match. This makes fresh deploys and
//! existing databases converge to the same schema without the operator
//! having to track migration application manually — `Sayiir`'s snapshot
//! tables are engine-owned.

/// One versioned migration. `version` is the value `PRAGMA user_version`
/// gets bumped to after `sql` runs successfully.
pub(crate) struct Migration {
    pub version: u32,
    pub sql: &'static str,
}

/// Ordered list of every schema migration. Add new migrations to the end.
///
/// Each migration's `sql` is executed exactly once per database in the
/// order listed; a database whose `user_version` already matches or
/// exceeds the entry's `version` skips it.
pub(crate) const MIGRATIONS: &[Migration] = &[
    Migration {
        version: 1,
        sql: include_str!("../migrations/001_initial.sql"),
    },
    Migration {
        version: 2,
        sql: include_str!("../migrations/002_task_priority.sql"),
    },
    Migration {
        version: 3,
        sql: include_str!("../migrations/003_task_tags.sql"),
    },
    Migration {
        version: 4,
        sql: include_str!("../migrations/004_awaited_signal_name.sql"),
    },
];
