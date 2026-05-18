//! Sleeping giants — long-timer durable parking + wake storm.
//!
//! Workflow: `AcceptId → ctx.sleep(N) → WakeTask → FinalEmit`
//!
//! Implementation lands in phase 6 of the design plan.

use anyhow::Result;

use crate::{CommonContext, SleepingGiantsArgs};

pub async fn run(_ctx: CommonContext, _args: SleepingGiantsArgs) -> Result<()> {
    anyhow::bail!("sleeping-giants scenario not implemented yet (phase 6)")
}
