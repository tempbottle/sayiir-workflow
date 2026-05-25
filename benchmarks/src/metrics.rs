//! Driver-side metrics: Prometheus exporter + hdrhistogram + completion signalling.
//!
//! Scenarios route two events back to the driver via global channels:
//!
//! * [`record_pickup`] — emitted by the *first* task of every workflow so the
//!   driver can measure scheduler-pickup latency (submit → first task start).
//!   Together with completion this splits end-to-end into the two phases
//!   competitors report separately (Temporal's `StartWorkflow` vs
//!   `WorkflowEndToEnd`).
//! * [`record_completion`] — emitted by the *final* task so the driver can
//!   close out the per-workflow timer.

use std::net::SocketAddr;
use std::sync::OnceLock;
use std::time::Instant;

use anyhow::{Context, Result};
use metrics_exporter_prometheus::PrometheusBuilder;
use tokio::sync::mpsc::UnboundedSender;

/// Pickup event emitted by the first task of every scenario workflow.
///
/// The driver subtracts `submit_time` (stored locally per index) from
/// `at` to derive scheduler-pickup latency.
#[derive(Debug, Clone, Copy)]
pub struct Pickup {
    pub workflow_index: u64,
    pub at: Instant,
}

/// Completion event emitted by the final task of every scenario workflow.
#[derive(Debug, Clone, Copy)]
pub struct Completion {
    pub workflow_index: u64,
    pub at: Instant,
}

/// Global pickup channel sender, populated once at scenario startup.
pub static PICKUP_TX: OnceLock<UnboundedSender<Pickup>> = OnceLock::new();

/// Global completion channel sender, populated once at scenario startup.
pub static COMPLETION_TX: OnceLock<UnboundedSender<Completion>> = OnceLock::new();

/// Install the global Prometheus metrics exporter and start its scrape listener.
pub fn install_prometheus_exporter(addr: &str) -> Result<()> {
    let socket: SocketAddr = addr
        .parse()
        .with_context(|| format!("parsing metrics bind address {addr}"))?;
    PrometheusBuilder::new()
        .with_http_listener(socket)
        .install()
        .context("installing prometheus exporter")?;
    tracing::info!(%addr, "driver metrics endpoint listening");
    Ok(())
}

/// Record a pickup event from within the first task of a workflow.
/// Silently drops if the global channel hasn't been initialised.
pub fn record_pickup(workflow_index: u64) {
    if let Some(tx) = PICKUP_TX.get() {
        let _ = tx.send(Pickup {
            workflow_index,
            at: Instant::now(),
        });
    }
}

/// Record a completion event from within a workflow task. Silently drops if
/// the global channel hasn't been initialised (e.g. unit tests).
pub fn record_completion(workflow_index: u64) {
    if let Some(tx) = COMPLETION_TX.get() {
        let _ = tx.send(Completion {
            workflow_index,
            at: Instant::now(),
        });
    }
}
