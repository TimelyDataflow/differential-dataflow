//! The corgi rendering substrate's internals: the columnar container on dataflow
//! edges ([`container`]) and its wire format ([`bytes`]), the columnar chunk and
//! its arrangement plumbing ([`chunk`]), the key-hash shuffle ([`exchange`]),
//! the DDIR-term compiler ([`logic`]), and the join/half-join/reduce tactic bindings ([`join`],
//! [`half_join`], [`reduce`]). The `Backend` impl wiring these into rendering is
//! [`crate::backend::corgi`].

pub mod bytes;
pub mod chunk;
pub mod col_times;
pub mod container;
pub mod exchange;
pub mod feed;
pub mod half_join;
pub mod join;
pub mod logic;
pub mod reduce;
mod search;
