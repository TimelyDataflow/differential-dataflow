//! Public compatibility facade for the in-process DDIR runtime.
//!
//! The implementation lives in [`crate::runtime`]. Keeping this module name
//! preserves the `interactive::server::{Server, evaluate, ...}` API used by
//! the executable and integration tests while making the runtime boundary
//! explicit in the source tree.

pub use crate::runtime::*;
