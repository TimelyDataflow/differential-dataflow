//! The server's two clocks: the host epoch `tick` advances, and the program time every
//! dataflow runs at, with `host_dataflow` to build a dataflow at it.

use super::*;

/// The host epoch: the server's clock, which `tick` advances.
pub type OuterTime = u64;

/// The time programs and published traces run at: an epoch, then one coordinate per enclosing
/// loop. A program's root collections, and so its published traces, are at `[e]`.
pub type HostTime = crate::ir::Time;

/// Epoch `e` as a program time, `[e]`.
pub fn host_time(e: OuterTime) -> HostTime {
    HostTime::new([e])
}

/// Build a dataflow whose operators run at [`HostTime`]: `build` gets a scope at that time.
///
/// The scope sits one level in, under a root at the epoch: timely's `new_input` wants a totally
/// ordered time, so the inputs live at the root and cross into it (see [`ServerInput`]), and
/// every trace is built and read at `HostTime`. Nothing iterates there.
pub fn host_dataflow<R>(
    worker: &mut Worker,
    build: impl FnOnce(timely::dataflow::Scope<'_, OuterTime>, timely::dataflow::Scope<'_, HostTime>) -> R,
) -> R {
    // Keep "Iterative" for comparisons across profiles and diagnostics, although nothing iterates here.
    worker.dataflow::<OuterTime, _, _>(|outer| outer.scoped::<HostTime, _, _>("Iterative", |scope| build(outer, scope)))
}
