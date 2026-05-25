//! PostgreSQL persistence backend for the Sayiir workflow engine.
//!
//! This crate provides [`PostgresBackend`], a production-grade implementation of
//! [`SnapshotStore`](sayiir_persistence::SnapshotStore),
//! [`SignalStore`](sayiir_persistence::SignalStore), and
//! [`TaskClaimStore`](sayiir_persistence::TaskClaimStore) backed by PostgreSQL via
//! [`sqlx`].
//!
//! # Features
//!
//! - **Codec-generic**: Serialise snapshots with any codec (JSON for debuggability,
//!   rkyv/bincode for speed). The data column is always `BYTEA`.
//! - **ACID transactions**: Composite signal operations (`check_and_cancel`,
//!   `check_and_pause`, `unpause`) use single Postgres transactions with
//!   `SELECT … FOR UPDATE` for true atomicity.
//! - **Snapshot history**: Every checkpoint is appended to an immutable history
//!   table for debugging, auditing, and future replay.
//! - **Observability-ready**: Indexed metadata columns (`status`, `current_task_id`,
//!   `completed_task_count`, `error`, timestamps) plus a denormalised
//!   `sayiir_workflow_tasks` table enable monitoring without deserialising blobs.
//! - **Distributed task claiming**: `TaskClaimStore` with TTL-based claims,
//!   expired-claim replacement, and soft worker bias.
//!
//! # PostgreSQL version support
//!
//! Minimum supported version: **PostgreSQL 13**. The schema uses
//! `INSERT … ON CONFLICT DO UPDATE` (9.5+) and `ALTER TABLE … ADD COLUMN IF NOT
//! EXISTS` (9.6+); 13 is the floor because it is the oldest major release still
//! receiving security patches. Integration tests run against both 13 and 17.
//!
//! # Quick start
//!
//! ```rust,no_run
//! use sayiir_postgres::PostgresBackend;
//! use sayiir_runtime::serialization::JsonCodec;
//! use sayiir_persistence::SnapshotStore;
//! use sayiir_core::snapshot::WorkflowSnapshot;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let backend = PostgresBackend::<JsonCodec>::connect("postgresql://localhost/sayiir").await?;
//!
//! let snapshot = WorkflowSnapshot::new("order-123", sayiir_core::DefinitionHash::from("hash-abc"));
//! backend.save_snapshot(&snapshot).await?;
//!
//! let loaded = backend.load_snapshot("order-123").await?;
//! # Ok(())
//! # }
//! ```

#![deny(missing_docs)]
#![deny(clippy::pedantic)]
#![deny(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::todo,
    clippy::unimplemented,
    clippy::dbg_macro,
    clippy::print_stdout,
    clippy::print_stderr
)]
// PostgreSQL is a proper noun, not inline code.
#![allow(clippy::doc_markdown)]

mod backend;
mod error;
mod history;
mod signal_store;
mod snapshot_store;
mod task_claim_store;
mod task_result_store;
mod wakeup;

pub use backend::{PoolOptions, PostgresBackend};
pub use wakeup::wakeup_drops_total;

/// Per-instance child tables — i.e. everything that holds rows keyed by
/// `instance_id` other than `sayiir_workflow_snapshots` itself. Source
/// of truth for both `delete_snapshot`'s cleanup loop and the
/// benchmark's `reset_sayiir_tables` truncate.
pub const WORKFLOW_CHILD_TABLES: &[&str] = &[
    "sayiir_workflow_snapshot_history",
    "sayiir_workflow_tasks",
    "sayiir_workflow_events",
    "sayiir_workflow_signals",
    "sayiir_workflow_claims",
];
