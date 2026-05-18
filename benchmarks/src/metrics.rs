//! Driver-side metrics: Prometheus exporter + hdrhistogram + completion signalling.

use std::net::SocketAddr;
use std::sync::OnceLock;
use std::time::Instant;

use anyhow::{Context, Result};
use metrics_exporter_prometheus::PrometheusBuilder;
use tokio::sync::mpsc::UnboundedSender;

/// Completion event emitted by the final task of every scenario workflow.
#[derive(Debug, Clone, Copy)]
pub struct Completion {
    pub workflow_index: u64,
    pub at: Instant,
}

/// Global completion channel sender, populated once at scenario startup.
///
/// Workflow tasks reach this via the static so they can emit completion
/// events back to the driver without per-instance plumbing.
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
