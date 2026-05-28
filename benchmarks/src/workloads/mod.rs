//! Workload scenarios. Each scenario owns its workflow definition and driver loop.
//!
//! Naming follows the durable-workflow benchmark vernacular: `linear`
//! is the universal N-step throughput shape (Restate / DBOS / Temporal
//! Omes), `fanout` is the scatter-gather pattern, `signal_driven`
//! exercises the wait-for-signal path, and `sleeping` is the
//! long-timer durable-park scenario. Add new shapes here.

pub mod fanout;
pub mod linear;
pub mod signal_driven;
pub mod sleeping;
