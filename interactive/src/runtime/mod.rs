//! The corgi rendering substrate's internals: the columnar container on dataflow
//! edges ([`container`]) and its wire format (`bytes`), the columnar chunk and
//! its arrangement plumbing ([`chunk`]), the key-hash shuffle ([`exchange`]),
//! and the join/half-join/reduce tactic bindings (`join`, `half_join`, `reduce`). The slots'
//! typing and compiling are the expression language's, in `crate::lambda`. The `Backend` impl wiring these into rendering is
//! `crate::backend::corgi`.

pub(crate) mod bytes;
pub mod chunk;
pub mod col_times;
pub mod container;
pub mod exchange;
pub mod feed;
pub mod frame;
pub(crate) mod half_join;
pub(crate) mod join;
pub(crate) mod reduce;
mod search;
