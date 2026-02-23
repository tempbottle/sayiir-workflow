//! Amazon DynamoDB persistence backend for the Sayiir workflow engine.
//!
//! This crate provides [`DynamoDbBackend`], a production-grade implementation of
//! [`SnapshotStore`](sayiir_persistence::SnapshotStore),
//! [`SignalStore`](sayiir_persistence::SignalStore), and
//! [`TaskClaimStore`](sayiir_persistence::TaskClaimStore) backed by Amazon DynamoDB.
//!
//! # Features
//!
//! - **Codec-generic**: Serialise snapshots with any codec (JSON for debuggability,
//!   rkyv/bincode for speed). The data attribute is always Binary.
//! - **Conditional writes**: Composite signal operations (`check_and_cancel`,
//!   `check_and_pause`, `unpause`) use `TransactWriteItems` with version conditions
//!   for optimistic-concurrency atomicity.
//! - **Snapshot history**: Every checkpoint is appended to an immutable history
//!   item for debugging, auditing, and future replay.
//! - **Serverless**: No database to manage — scales automatically with DynamoDB
//!   on-demand capacity.
//! - **Distributed task claiming**: `TaskClaimStore` with TTL-based claims and
//!   DynamoDB conditional writes for mutual exclusion.
//!
//! # Quick start
//!
//! ```rust,no_run
//! use sayiir_dynamodb::DynamoDbBackend;
//! use sayiir_runtime::serialization::JsonCodec;
//! use sayiir_persistence::SnapshotStore;
//! use sayiir_core::snapshot::WorkflowSnapshot;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
//! let backend = DynamoDbBackend::<JsonCodec>::new(&config, "myapp").await?;
//!
//! let snapshot = WorkflowSnapshot::new("order-123".to_string(), "hash-abc".to_string());
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
// DynamoDB is a proper noun, not inline code.
#![allow(clippy::doc_markdown)]

mod backend;
mod error;
mod helpers;
mod signal_store;
mod snapshot_store;
mod task_claim_store;

pub use backend::DynamoDbBackend;
