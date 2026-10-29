//! The client-facing epoch and the single timestamp used by every host dataflow.

use super::*;

/// The host epoch: the server's clock, which `tick` advances.
pub type OuterTime = u64;

/// The time programs and published traces run at: coordinate 0 is the epoch, and coordinate
/// `j` is loop depth `j`. Root collections have only the epoch (empty at epoch zero).
pub type HostTime = crate::ir::Time;

/// Epoch `e` as a program time, with trailing zeros trimmed.
pub fn host_time(e: OuterTime) -> HostTime {
    HostTime::new(smallvec::smallvec![e])
}

/// Build a dataflow rooted directly at [`HostTime`], which refines `()`.
pub fn host_dataflow<R>(
    worker: &mut Worker,
    build: impl FnOnce(timely::dataflow::Scope<'_, HostTime>) -> R,
) -> R {
    worker.dataflow::<HostTime, _, _>(build)
}
