//! The client-facing epoch and the single timestamp used by every host dataflow.

use super::*;

/// The host epoch: the server's clock, which `tick` advances.
pub type OuterTime = u64;

/// The time programs and published traces run at: the first `k` coordinates are the root,
/// followed by one per loop. The legacy epoch API uses `k = 1`. Trailing zeros are trimmed.
pub type HostTime = crate::ir::Time;

/// Epoch `e` as a program time, with trailing zeros trimmed.
pub fn host_time(e: OuterTime) -> HostTime {
    HostTime::new(smallvec::smallvec![e])
}

/// Pad trimmed zeros to the declared width; retain excess coordinates in invalid points.
pub(super) fn format_point(point: &HostTime, width: usize) -> String {
    let coordinates: Vec<_> = (0..width.max(point.len())).map(|i| {
        match point.get(i).copied().unwrap_or(0) {
            u64::MAX => "max".to_string(),
            coordinate => coordinate.to_string(),
        }
    }).collect();
    format!("[{}]", coordinates.join(","))
}

pub(super) fn format_frontier(frontier: &Antichain<HostTime>, width: usize) -> String {
    let mut points: Vec<_> = frontier.elements().iter().collect();
    points.sort();
    format!("{{{}}}", points.into_iter().map(|p| format_point(p, width)).collect::<Vec<_>>().join(","))
}

/// Build a dataflow rooted directly at [`HostTime`], which refines `()`.
pub fn host_dataflow<R>(
    worker: &mut Worker,
    build: impl FnOnce(timely::dataflow::Scope<'_, HostTime>) -> R,
) -> R {
    worker.dataflow::<HostTime, _, _>(build)
}
