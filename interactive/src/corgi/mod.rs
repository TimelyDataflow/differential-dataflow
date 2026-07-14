//! The corgi rendering substrate's internals: the columnar container on dataflow
//! edges ([`container`]), the columnar chunk and its arrangement plumbing ([`chunk`]),
//! the DDIR-term compiler ([`logic`]), and the join/reduce tactic bindings ([`join`],
//! [`reduce`]). The `Backend` impl wiring these into rendering is
//! [`crate::backend::corgi`].

pub mod chunk;
pub mod col_times;
pub mod container;
pub mod join;
pub mod logic;
pub mod reduce;
