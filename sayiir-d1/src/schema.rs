//! Embedded migration SQL for D1 schema initialization.

/// `SQLite` DDL for the 4 workflow tables, run via `D1Database` raw SQL execution.
pub(crate) const MIGRATION_SQL: &str = include_str!("../migrations/001_initial.sql");
