//! Timely dataflow operators specific to differential dataflow.
//!
//! Differential dataflow introduces a small number of specialized operators, designed to apply to
//! streams of *typed updates*, records of the form `(T, i32)` indicating that the frequency of the
//! associated record of type `T` has changed.
//!
//! These operators have specialized implementations to make them work efficiently, but are in all
//! other ways compatible with timely dataflow. In fact, many operators are currently absent because
//! their timely dataflow analogues are sufficient (e.g. `map`, `filter`, `concat`).

pub use self::group_by::{GroupByExt, GroupExt, GroupUnsigned};
pub use self::consolidate::ConsolidateExt;
pub use self::iterate::IterateExt;
pub use self::join::JoinExt;

pub mod group_by;
pub mod consolidate;
pub mod iterate;
pub mod join;
