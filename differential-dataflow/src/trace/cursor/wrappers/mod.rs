//! Cursor implementations for the trace wrappers.
//!
//! The wrappers themselves live in [`crate::trace::wrappers`] and are cursor-free: they present
//! wrapped traces and batches, and expose their time semantics over owned times. This module
//! supplies one way to read through those wrappers, by forwarding to the cursor of the wrapped
//! batch and applying the wrapper's time rule to each time it produces. Another read strategy
//! would supply its own module here, and reuse the same time rules.

pub mod enter;
