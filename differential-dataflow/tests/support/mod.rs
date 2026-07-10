//! Shared test support for the int_proxy integration tests.
//!
//! Code here is deliberately OUT of the shipped crate: the seam is tested by being
//! consumed from test code as an external crate would consume it (DESIGN.md S5).

pub mod vec_backend;
pub mod identity_chunk;
