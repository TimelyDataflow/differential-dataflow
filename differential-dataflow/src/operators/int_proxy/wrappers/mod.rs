//! Backends that read wrapped batches.
//!
//! The trace wrappers change only the times a reader sees, never data or diffs. A batch entered
//! into a nested scope ([`BatchEnter`](crate::trace::wrappers::enter::BatchEnter)) reads each time as `TInner::to_inner(time)`. A batch of a
//! trace read as of a frontier ([`BatchFrontier`](crate::trace::wrappers::frontier::BatchFrontier)) reads each time advanced by `since`, and none at
//! or beyond `until`. A cursor applies these rules as it reads, but the proxy tactics read batches
//! through a backend, so each wrapper here is a backend. It hands its inner backend the batches
//! inside the wrapper, and reads the times the inner backend draws as the wrapper says. The tactic
//! sees an ordinary backend, whose times are already the operator's.
//!
//! Each wrapper peels one batch wrapper off one input, and they compose by nesting, outermost
//! wrapper outermost: a join whose second input is a trace entered into a scope and read as of a
//! frontier uses `BackendEnter::right(BackendFrontier::right(backend))`. Only reads are
//! translated. Outputs are the inner backend's, built at the operator's times, and pass through.

mod enter;
mod frontier;

pub use enter::BackendEnter;
pub use frontier::BackendFrontier;

/// The input a wrapper reads: a join's first input, or a reduce's input.
pub struct Left;
/// The input a wrapper reads: a join's second input.
pub struct Right;
