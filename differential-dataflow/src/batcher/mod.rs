//! Traits and implementations for forming batches from streams of updates.

use timely::progress::frontier::AntichainRef;

pub mod merge;

/// A type capable of accepting containers of updates, and carving them out by time as batches.
///
/// Updates are accepted as `C0`, the containers that arrive on the dataflow edge, and released as
/// `Output`, whatever the implementor means by a batch. The two need not agree: an implementor
/// staging updates in a form of its own can release that form directly, and one whose batch is a
/// sequence of chunks names a sequence as its output.
///
/// The implementor determines the meaning of extraction by a frontier; it is not required to be by
/// antichain partial order.
pub trait Batcher<C0> {
    /// The timestamps by which updates are carved out.
    type Time;
    /// The batches released by extraction.
    type Output;

    /// Takes the updates in `container`, leaving it in an undefined state.
    ///
    /// The implementor decides whether to claim the container's allocation or to drain it and
    /// leave the allocation with the caller, who is free to reuse the container either way.
    fn insert(&mut self, container: &mut C0);
    /// Extracts the updates `upper` unblocks as a batch, and lower bounds the times of those retained.
    ///
    /// What `upper` unblocks is the implementor's to decide. It can be based on the antichain up
    /// set, or it can be based on the total order of times (as used in delta join constructions).
    /// Absent a batch, `upper` unblocked no updates.
    ///
    /// The reported lower bound should accurately reflect the times of all accepted updates that
    /// have not been extracted. Over approximation can result in stalling dataflows, and under
    /// approximation is simply incorrect.
    fn extract<'a>(&'a mut self, upper: AntichainRef<'_, Self::Time>) -> (Option<Self::Output>, AntichainRef<'a, Self::Time>);
}
