//! Timely dataflow operators specific to differential dataflow.
//!
//! Differential dataflow introduces a small number of specialized operators, designed to apply to
//! streams of *typed updates*, records of the form `(T, Delta)` indicating that the frequency of the
//! associated record of type `T` has changed.
//!
//! These operators have specialized implementations to make them work efficiently, but are in all
//! other ways compatible with timely dataflow. In fact, many operators are currently absent because
//! their timely dataflow analogues are sufficient (e.g. `map`, `filter`, `concat`).

pub use self::group::Group;
// pub use self::cogroup::CoGroupBy;
pub use self::consolidate::ConsolidateExt;
pub use self::iterate::IterateExt;
pub use self::join::Join;
pub use self::threshold::Threshold;
pub use self::arrange::{ArrangeByKey, ArrangeBySelf};

pub mod arrange;
pub mod threshold;
pub mod group;
// pub mod cogroup;
pub mod consolidate;
pub mod iterate;
pub mod join;
