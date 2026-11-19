//! Scalar client times and the timestamp used by host dataflows.

use super::*;

/// A point on a scalar input or output.
pub type OuterTime = u64;

/// The time programs and published traces run at: the first `k` coordinates are the root,
/// followed by one per loop. Scalar inputs use `k = 1`. Trailing zeros are trimmed.
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
