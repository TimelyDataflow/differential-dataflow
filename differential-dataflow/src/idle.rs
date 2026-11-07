//! Opt-in scheduling for arrangement readers: join, half join, count_total and threshold_total.
//!
//! Set the bool worker configuration value `differential/idle_readers` to `true` to use
//! `IfCapability` frontier interest. The default is `false` (`Always`). Read once at construction;
//! this does not change arrangements or reduce outputs, including those built by convenience methods.
//! Sleeping readers delay compaction holds until they next run and can retain history longer.
//! A join with one closed input can retain the other trace until data arrives or both inputs close.

use timely::{dataflow::Scope, progress::{Timestamp, operate::FrontierInterest}};

/// Worker configuration key for opting into relaxed reader frontier interest (a `bool`).
pub const IDLE_READERS: &str = "differential/idle_readers";

/// Frontier interest for a reader, read once when its operator is built.
pub fn reader_interest<T: Timestamp>(scope: &Scope<'_, T>) -> FrontierInterest {
    if scope.worker().config().get::<bool>(IDLE_READERS).copied().unwrap_or(false) {
        FrontierInterest::IfCapability
    } else {
        FrontierInterest::Always
    }
}
