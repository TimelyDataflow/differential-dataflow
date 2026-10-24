//! The server's two clocks: the host epoch `tick` advances, and the program time every
//! dataflow runs at, with `host_dataflow` to build a dataflow at it.

use super::*;

/// The host epoch: the server's clock, which `tick` advances.
pub type OuterTime = u64;

/// The time programs and published traces run at: an epoch, then one coordinate per enclosing
/// loop. A program's root collections, and so its published traces, are at `(e, [])`.
pub type HostTime = crate::ir::Time;

/// Epoch `e` as a program time, `(e, [])`.
pub fn host_time(e: OuterTime) -> HostTime {
    timely::order::Product::new(e, PointStamp::default())
}

/// Build a dataflow whose operators run at [`HostTime`]: `build` gets a scope at that time.
///
/// Timely roots a dataflow at a time that refines `()`, which `HostTime` does not, so the scope
/// sits one level in, under a root at the epoch. Nothing iterates there: only inputs, which a
/// root must hold, cross into it (see [`ServerInput`]), and every trace is built and read at
/// `HostTime`.
pub fn host_dataflow<R>(
    worker: &mut Worker,
    build: impl FnOnce(timely::dataflow::Scope<'_, OuterTime>, timely::dataflow::Scope<'_, HostTime>) -> R,
) -> R {
    worker.dataflow::<OuterTime, _, _>(|outer| outer.iterative::<PointStamp<OuterTime>, _, _>(|scope| build(outer, scope)))
}
