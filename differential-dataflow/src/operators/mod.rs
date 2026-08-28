//! Specialize differential dataflow operators.
//!
//! Differential dataflow introduces a small number of specialized operators on collections. These
//! operators have specialized implementations to make them work efficiently, and are in addition
//! to several operations defined directly on the `Collection` type (e.g. `map` and `filter`).

pub use self::iterate::Iterate;
pub use self::count::CountTotal;
pub use self::threshold::ThresholdTotal;

pub mod arrange;
pub mod cursor;
pub mod history;
pub mod int_proxy;
pub mod reduce;
pub mod iterate;
pub mod join;
pub mod count;
pub mod threshold;

pub use self::history::{EditList, ValueHistory};
