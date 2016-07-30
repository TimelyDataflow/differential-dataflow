//! A time-varying multiset of records.
//!
//! The core datastructure representing a dataset in differential dataflow is `collection::Trace`.
//! It represents a multiset that can be indexed by partially ordered times, and is stored in a
//! compressed incremental representation.

pub mod lookup;
pub mod trace;
pub mod compact;
pub mod trie;
pub mod count;
pub mod robin_hood;
pub mod basic;

pub use collection::lookup::Lookup;
pub use collection::trace::{Trace, TraceRef};
pub use collection::basic::{BasicTrace, Offset};