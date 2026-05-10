//! Traits for paging chunks of merge-batcher state to and from backing storage.
//!
//! Modeled on timely's pager traits in
//! `timely-dataflow/communication/src/allocator/zero_copy/spill.rs`
//! (`SpillPolicy`, `BytesSpill`, `BytesFetch`), but parameterized over a chunk
//! type `C` rather than fixed to `timely::bytes::arc::Bytes`. For the columnar
//! batcher we expect `C = Updates<U>`; that wiring lives elsewhere — this file
//! only defines the trait shapes.

use std::collections::VecDeque;

/// A queue entry: either an in-memory chunk or a handle that can fetch one
/// (or several) from backing storage.
pub enum Entry<C> {
    /// In-memory chunk.
    Typed(C),
    /// Paged-out chunk(s); fetch via the handle.
    Paged(Box<dyn Fetch<C>>),
}

/// Decides which queue entries to spill out and which to keep resident.
///
/// Invoked at well-defined moments by the holder of the queue (e.g., after
/// pushing a new chunk). The implementation may rewrite entries in either
/// direction: convert `Typed` to `Paged` (spill out) or `Paged` to `Typed`
/// (fetch back).
pub trait SpillPolicy<C> {
    /// Optionally transform the queue.
    fn apply(&mut self, queue: &mut VecDeque<Entry<C>>);
}

/// Move in-memory chunks to backing storage, returning fetch handles.
///
/// The implementation should drain from `chunks` and push to `handles` as it
/// goes; on failure it may stop partway, leaving the lists in a consistent
/// state that will be retried in the future. If it cannot leave the lists in
/// a consistent state it should panic.
pub trait Spill<C> {
    /// Spill `chunks` to storage, producing one fetch handle per spilled group.
    fn spill(&mut self, chunks: &mut Vec<C>, handles: &mut Vec<Box<dyn Fetch<C>>>);
}

/// Handle to spilled chunk(s). Consume to retrieve them from storage.
pub trait Fetch<C> {
    /// Consume the handle and return the spilled chunks.
    ///
    /// On failure, the handle is returned so the caller can retry later.
    fn fetch(self: Box<Self>) -> Result<Vec<C>, Box<dyn Fetch<C>>>;
}
