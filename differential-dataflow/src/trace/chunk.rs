//! Sorted, consolidated runs of updates, and operators over sequences of them.
//!
//! A [`Chunk`] is a consolidated, sorted run of `(data, time, diff)` updates.
//! Chunks live in sequences (`Vec<Chunk>`) with no constraint on where the
//! breakpoints between them fall; each chunk holds at most [`Chunk::TARGET`]
//! updates. The trait deliberately exposes only batch-level operations — merge,
//! extract, advance — leaving the layout-aware work to the implementor. The
//! orchestration in this module (the binary merger) is generic over the layout
//! and concerns itself only with feeding chunks across calls.
//!
//! # Why chunks, and why one size
//!
//! A batch could be a single monolithic sorted run. We cut it into chunks because
//! the chunk is simultaneously the unit of four things, each of which wants a size
//! bound:
//!
//! * **Suspendable work.** The fueled merger does a chunk's-worth of work per step
//!   and checks fuel at the boundary, so chunk size bounds a step's latency.
//! * **Immutable sharing.** Chunks are `Rc`-shared; the merger reads its sources by
//!   *cloning* chunks (a refcount bump). The chunk is the finest granularity of sharing.
//! * **Allocation recycling.** Emptied input buffers are reused as output buffers;
//!   that only composes if buffers are roughly one size.
//! * **Indexing.** [`ChunkBatch`] indexes chunks by their first/last key, and the
//!   cursor binary-searches *over* chunks then gallops *within* one. The chunk
//!   count (≈ `len / TARGET`) sets the outer index size and search depth.
//!
//! So the size bound pulls two ways: an upper bound (latency, memory) says "not too
//! big," and a lower bound (per-chunk overhead, index bloat) says "not too
//! fragmented." Keeping chunks one size is what lets a single knob satisfy both.
//! The grading invariant ([`is_graded`]) encodes exactly this: every chunk is at
//! most `TARGET`, and every *adjacent pair* exceeds `TARGET` — i.e. no two
//! neighbours could be combined into one legal chunk. That makes `TARGET` both the
//! maximum size and the coalescing threshold (the invariant is self-similar), and
//! a graded sequence a *maximal packing*: as few chunks as the maximum allows.
//!
//! The intent is for a `Chunk` implementation to be each of
//!     1. the containers a `Collection` can transit.
//!     2. the containers a `MergeBatcher` can work with.
//!     3. the containers a `Batch` can be backed by.
//! It does this by exposing a small set of chunk-oriented primitives, which are
//! sufficient for harnesses for each of these tasks.

use timely::progress::Antichain;
use timely::progress::frontier::AntichainRef;
use crate::lattice::Lattice;
use crate::trace::{Batch, BatchReader, Description};
use crate::trace::cursor::Cursor;
use crate::trace::implementations::{BatchContainer, Layout, LayoutExt, WithLayout};

/// The key container of chunk `C`'s layout. Named via the `Layout` projection so
/// it unifies with the cursor's `Self::Key`, which also projects through `Layout`.
type KeyCon<C> = <<C as WithLayout>::Layout as Layout>::KeyContainer;
/// The val container of chunk `C`'s layout.
type ValCon<C> = <<C as WithLayout>::Layout as Layout>::ValContainer;

/// A partially consumed head and optional tail of chunks.
pub type ChunkFeed<C> = ((usize, C), Vec<C>);

/// Whether `chunks` satisfy the [`Chunk::TARGET`] grading invariant: every chunk
/// at most `TARGET`, and every adjacent pair summing to more than `TARGET` (so no
/// two neighbours could be combined into one legal chunk — a *maximal packing*).
///
/// This is the post-[`regrade`](Chunk::regrade) shape; useful as a test/debug check.
pub fn is_graded<C: Chunk>(chunks: &[C]) -> bool {
    chunks.iter().all(|c| c.len() <= C::TARGET)
        && chunks.windows(2).all(|w| w[0].len() + w[1].len() > C::TARGET)
}

/// A list of chunks that maintains the `C::regrade` structural invariant.
///
/// Producers `push` chunks in; each push runs `C::regrade`, which moves graded
/// runs into `data` and leaves anything not yet safe to emit in `todo`. `done`
/// flushes the remainder and yields the graded sequence.
pub struct ChunkList<C> {
    todo: Vec<C>,
    data: Vec<C>,
}

impl<C> Default for ChunkList<C> {
    fn default() -> Self { Self { todo: Vec::new(), data: Vec::new() } }
}

impl<C: Chunk> ChunkList<C> {
    /// Add a new chunk to the list, regrading as far as is safe.
    pub fn push(&mut self, chunk: C) {
        self.todo.push(chunk);
        C::regrade(&mut self.todo, false, &mut self.data);
    }
    /// Add several chunks.
    pub fn extend<I: IntoIterator<Item = C>>(&mut self, chunks: I) {
        for chunk in chunks { self.push(chunk); }
    }
    /// Finalize the list, flushing the remainder, and extract the graded sequence.
    pub fn done(mut self) -> Vec<C> {
        C::regrade(&mut self.todo, true, &mut self.data);
        assert!(self.todo.is_empty());
        self.data
    }
}

/// A consolidated, sorted sequence of `(data, time, diff)`.
///
/// Chunks exist in sequences, with no constraints on the breakpoints between
/// them. Each holds at most [`TARGET`](Chunk::TARGET) updates; a graded sequence
/// is a maximal packing at that size (see [`is_graded`] and the module docs).
///
/// `Clone` is expected to be cheap — a refcount bump on shared backing storage,
/// not a deep copy. The trace merger relies on this to read its (shared,
/// immutable) source batches by cloning chunks rather than consuming them, and
/// `prune` is likewise expected to be a range adjustment over shared storage.
///
/// A chunk *has* a [`Cursor`] over its own `(key, val, time, diff)` contents —
/// the chunk is its own cursor `Storage`, mirroring [`BatchReader`]. This is what
/// lets a batch cursor delegate downward: the batch indexes which chunk holds a
/// key (reusing the chunk's `KeyContainer` / `ValContainer` for boundaries) and
/// then reads through that chunk's cursor. As with `merge`, we do not
/// provide this; the opaque chunk implementor does.
///
/// # Implementor contract
///
/// The chunk-producing operations (`merge`, `extract`, `advance`, `regrade`) emit
/// into a [`ChunkList`], and implementors are expected to:
///
/// * **Respect the chain structure.** Emit *graded* chunks — sized to the
///   `regrade` invariant — rather than collapsing a run into one monolithic chunk
///   and leaning on `regrade` to re-split it. Building the right shape directly
///   avoids a redundant copy.
/// * **Bound output by input consumed.** Produce output chunks in proportion to
///   the input chunks consumed, never buffering an unbounded amount before
///   emitting. The fueled merger debits progress by the work it feeds across
///   suspensions; output that lags input arbitrarily breaks that accounting.
/// * **Recycle where possible.** Reuse the storage of chunks drained from the
///   input as the buffers for output, so allocations balance input against output
///   rather than allocating afresh per emitted chunk. `vec_chunk::extract` is the
///   worked example: it fills `TARGET`-sized buffers reclaimed from a stash of
///   emptied input `Vec`s.
///
/// [`BatchReader`]: crate::trace::BatchReader
pub trait Chunk: Sized + Clone + LayoutExt {

    /// The chunk size: both the maximum updates per chunk and the coalescing
    /// threshold.
    ///
    /// A *graded* sequence (the post-[`regrade`](Chunk::regrade) shape) has every
    /// chunk of length at most `TARGET`, and every adjacent pair summing to more
    /// than `TARGET` — so no two neighbours could be combined into one legal chunk.
    /// Equivalently, a maximal packing at size `TARGET`. [`is_graded`] checks
    /// exactly this. The value is the implementor's tuning knob: larger means fewer
    /// chunks (smaller index, less per-chunk overhead) but coarser merge-suspension
    /// granularity and a larger within-chunk seek.
    const TARGET: usize = 1024;

    /// A cursor navigating this chunk's contents; the chunk is its storage.
    ///
    /// The layout aliases are spelled out (mirroring [`BatchReader`]) so the
    /// cursor's `Key`/`Val`/`Time`/`Diff` and their containers are *definitionally*
    /// equal to the chunk's — without this the compiler won't connect the cursor's
    /// layout to the chunk's when reading through it.
    type Cursor:
        Cursor<Storage = Self> +
        WithLayout<Layout = Self::Layout> +
        for<'a> LayoutExt<
            Key<'a> = Self::Key<'a>,
            Val<'a> = Self::Val<'a>,
            ValOwn = Self::ValOwn,
            Time = Self::Time,
            TimeGat<'a> = Self::TimeGat<'a>,
            Diff = Self::Diff,
            DiffGat<'a> = Self::DiffGat<'a>,
            KeyContainer = Self::KeyContainer,
            ValContainer = Self::ValContainer,
            TimeContainer = Self::TimeContainer,
            DiffContainer = Self::DiffContainer,
        >;

    /// Acquire a cursor over this chunk.
    fn cursor(&self) -> Self::Cursor;

    /// The first and last `(key, val, time)` triples in the chunk.
    ///
    /// The chunk must be non-empty (batch chunks always are). Expected to be
    /// cheap — the chunk's endpoints, e.g. columnar indices `0` and `len - 1`,
    /// not a cursor walk. Indexing a batch's chunks rests on this: the last
    /// triples drive a binary search to a key or `(key, val)`, and comparing one
    /// chunk's last triple against the next chunk's first detects keys or
    /// `(key, val)` pairs that straddle the boundary — all without touching chunk
    /// contents. Returned by reference (no owned key type exists in the layout);
    /// the index materializes them into its own containers.
    fn bounds(&self) -> (
        (Self::Key<'_>, Self::Val<'_>, Self::TimeGat<'_>),
        (Self::Key<'_>, Self::Val<'_>, Self::TimeGat<'_>),
    );

    /// The number of updates in the chunk.
    ///
    /// Chunks are always non-empty (`len() > 0`): producers drop empties before
    /// they reach a chunk sequence, and [`ChunkBatch::new`] asserts the invariant.
    fn len(&self) -> usize;

    /// Remove some first few updates, returning the remainder.
    ///
    /// Implemented via a singleton `merge`: with one input there is no horizon to
    /// hold back, so the whole suffix `[prefix..]` is emitted. The remainder of a
    /// graded chunk is at most one graded chunk.
    fn prune(self, prefix: usize) -> Self {
        let mut buffer = ChunkList::default();
        Self::merge(&mut [(prefix, self)], &mut buffer);
        let mut data = buffer.done();
        assert_eq!(data.len(), 1);
        data.pop().unwrap()
    }

    /// Merges as much as possible from each of the input chunks.
    ///
    /// Input chunks come with a number of consumed prefix updates, which are not
    /// intended for merging. The chunks are only able to merge through updates
    /// that would be present in all inputs, generally up to the least last
    /// `(key, val, time)` triple across the inputs. On return, the consumed
    /// prefix of at least one input has advanced to that input's length, marking
    /// it drained and signalling the caller to refill that slot.
    fn merge(chunks: &mut [(usize, Self)], out: &mut ChunkList<Self>);

    /// Partition chunks into updates greater or equal `frontier` (`keep`) or not (`ship`).
    ///
    /// The lower envelope of the times routed to `keep` is folded into
    /// `residual`, so the caller learns the frontier of data it still holds
    /// without a second pass over the chunks.
    fn extract(
        chunks: &mut Vec<Self>,
        frontier: &Antichain<Self::Time>,
        residual: &mut Antichain<Self::Time>,
        keep: &mut ChunkList<Self>,
        ship: &mut ChunkList<Self>,
    );

    /// Advance times in input chunks by `frontier` and push consolidated result out.
    ///
    /// To be certainly consolidated, all `(key, val)` updates must be present in
    /// the input, or `done` must be set. A run of chunks may fail to be emitted if
    /// they all share the same `(key, val)` and the implementor cannot be sure no
    /// future times for the pair are yet to arrive.
    fn advance(
        feed: &mut ChunkFeed<Self>,
        frontier: &Antichain<Self::Time>,
        done: bool,
        out: &mut ChunkList<Self>,
    );

    /// Reshapes a sequence of consolidated chunks into a maximal packing: each at
    /// most [`TARGET`](Chunk::TARGET), and any two adjacent chunks summing past
    /// `TARGET` (so no neighbours could be combined). See [`is_graded`].
    ///
    /// The implementor should guard against emitting sequences of chunks that violate
    /// the invariant, until the set `done` indicates that the queues is complete.
    /// The implementor is allowed to push back at `queue` if it needs, but should
    /// not corrupt the order of chunks and updates.
    fn regrade(
        queue: &mut Vec<Self>,
        done: bool,
        out: &mut Vec<Self>,
    );

}

/// Merge two sorted chains of chunks into one sorted chain.
///
/// Presents the heads of `chain1` and `chain2` to [`Chunk::merge`], each
/// tagged with the prefix already consumed. After each call at least one head has
/// been drained to its length; that slot is refilled from its chain. When either
/// chain is exhausted, the partially-consumed remainder of the other is pruned of
/// its consumed prefix and the rest of that chain is appended verbatim.
pub fn merge_chains<C: Chunk>(
    chain1: Vec<C>,
    chain2: Vec<C>,
    out: &mut ChunkList<C>,
) {
    let mut iter1 = chain1.into_iter();
    let mut iter2 = chain2.into_iter();

    // Current head of each chain, tagged with its consumed prefix; `None` once
    // that chain's iterator is exhausted.
    let mut head1 = iter1.next().map(|c| (0, c));
    let mut head2 = iter2.next().map(|c| (0, c));

    while head1.is_some() && head2.is_some() {
        let mut window = [head1.take().unwrap(), head2.take().unwrap()];
        C::merge(&mut window, out);
        let [(p1, c1), (p2, c2)] = window;
        // Refill whichever side(s) drained to length; keep partially-consumed ones.
        head1 = if p1 >= c1.len() { iter1.next().map(|c| (0, c)) } else { Some((p1, c1)) };
        head2 = if p2 >= c2.len() { iter2.next().map(|c| (0, c)) } else { Some((p2, c2)) };
    }

    // One chain is exhausted; flush the partially-consumed remainder of the other,
    // then its untouched tail.
    for head in [head1, head2] {
        if let Some((consumed, chunk)) = head {
            // A retained head always has `consumed < len` (a fully-consumed one
            // would have been refilled), so the pruned remainder is non-empty.
            let chunk = if consumed > 0 { chunk.prune(consumed) } else { chunk };
            out.push(chunk);
        }
    }
    out.extend(iter1);
    out.extend(iter2);
}

/// Drives [`Chunk::advance`] over a growing queue of chunks.
///
/// Compaction may need to see several chunks before it can emit a consolidated
/// output chunk, because a `(key, val)` run can span chunk boundaries. The
/// implementor owns the `(next, tail)` representation and rotates it itself: it
/// can consume across chunks by amounts the driver cannot see, so the driver
/// never promotes from `tail` into `next`. The driver only appends incoming
/// chunks to `tail` and calls `advance`; a final [`Self::finish`] sets `done` to
/// flush whatever was being withheld.
pub struct AdvanceQueue<C: Chunk> {
    /// The chunks awaiting advancement, as a head (with consumed prefix) and tail;
    /// the implementor owns rotation between them.
    feed: ChunkFeed<C>,
    /// Frontier to advance times by during compaction.
    frontier: Antichain<C::Time>,
}

impl<C: Chunk + Default> AdvanceQueue<C> {
    /// A compactor that advances times by `frontier`.
    pub fn new(frontier: Antichain<C::Time>) -> Self {
        Self { feed: ((0, C::default()), Vec::new()), frontier }
    }
    /// Append a completed merge's chunks and advance as far as is certain.
    pub fn push<I: IntoIterator<Item = C>>(&mut self, chunks: I, out: &mut ChunkList<C>) {
        self.feed.1.extend(chunks);
        C::advance(&mut self.feed, &self.frontier, false, out);
    }
    /// Flush all remaining updates; no further chunks will be pushed.
    pub fn finish(mut self, out: &mut ChunkList<C>) {
        C::advance(&mut self.feed, &self.frontier, true, out);
    }
}

/// A merge-batcher [`Merger`](crate::trace::implementations::merge_batcher::Merger)
/// over chains of [`Chunk`]s.
///
/// `merge` runs the binary merger; `extract` splits by the seal frontier using
/// [`Chunk::extract`]. The batcher consolidates equal `(data, time)` updates
/// but does *not* advance times — time advancement is advance's job, handled
/// later in the trace.
pub struct ChunkMerger<C> {
    _marker: std::marker::PhantomData<C>,
}

impl<C> Default for ChunkMerger<C> {
    fn default() -> Self { Self { _marker: std::marker::PhantomData } }
}

impl<C> crate::trace::implementations::merge_batcher::Merger for ChunkMerger<C>
where
    C: Chunk + Default + 'static,
    C::Time: Clone + timely::PartialOrder + 'static,
{
    type Chunk = C;
    type Time = C::Time;

    fn merge(
        &mut self,
        list1: Vec<C>,
        list2: Vec<C>,
        output: &mut Vec<C>,
        _stash: &mut Vec<C>,
    ) {
        // The merge-batcher's chains are plain `Vec`s; grade through a `ChunkList`.
        let mut graded = ChunkList::default();
        merge_chains(list1, list2, &mut graded);
        output.extend(graded.done());
    }

    fn extract(
        &mut self,
        mut merged: Vec<C>,
        upper: AntichainRef<C::Time>,
        frontier: &mut Antichain<C::Time>,
        ship: &mut Vec<C>,
        kept: &mut Vec<C>,
        _stash: &mut Vec<C>,
    ) {
        // `extract` keeps updates greater-or-equal `upper` and ships the rest,
        // folding the lower envelope of kept times into `frontier`.
        let upper = upper.to_owned();
        let (mut keep, mut shipped) = (ChunkList::default(), ChunkList::default());
        C::extract(&mut merged, &upper, frontier, &mut keep, &mut shipped);
        kept.extend(keep.done());
        ship.extend(shipped.done());
    }

    fn account(chunk: &C) -> (usize, usize, usize, usize) { (chunk.len(), 0, 0, 0) }
}

/// The merge batcher for chunks of type `C`, merging pre-chunked `C` runs.
///
/// The batcher accepts already-formed `C` chunks via `PushInto` and merges them
/// through [`ChunkMerger`]; it holds no chunker. The `Input → C` bridge lives at the
/// `arrange_core` callsite, which supplies the chunker (e.g. [`ContainerChunker<C>`]
/// for same-shape input, where `C` satisfies the batcher-side container traits
/// `SizableContainer`, `Consolidate`, `Container`, `PushInto<Input::Item>`).
///
/// [`ContainerChunker<C>`]: crate::trace::implementations::chunker::ContainerChunker
pub type ChunkBatcher<C> = crate::trace::implementations::merge_batcher::MergeBatcher<ChunkMerger<C>>;

/// A spine of `Rc`-shared [`ChunkBatch`]es of type `C`: the trace type for `arrange`.
pub type ChunkSpine<C> = crate::trace::implementations::spine_fueled::Spine<std::rc::Rc<ChunkBatch<C>>>;

/// A reference-counted [`ChunkBatch`] builder over chunks of type `C`.
pub type ChunkRcBuilder<C> = crate::trace::rc_blanket_impls::RcBuilder<ChunkBuilder<C>>;

/// A batch is just an ordered sequence of [`Chunk`]s plus its time description.
///
/// The chunks are sorted and consolidated, with chunk boundaries arbitrary; the
/// concatenation of their contents is the batch.
///
/// This is a full [`Batch`](crate::trace::Batch): [`ChunkBatchCursor`] reads
/// across the chunks (delegating to each chunk's own cursor and continuing past
/// boundaries), [`ChunkBatchMerger`] performs the resumable merge-and-advance,
/// and [`ChunkBuilder`] collects pre-sorted chunks. All of those are below.
pub struct ChunkBatch<C: Chunk> {
    /// Ordered, consolidated chunks; their concatenation is the batch.
    pub chunks: Vec<C>,
    /// The lower, upper, and since frontiers of the batch.
    pub description: Description<C::Time>,
    /// Per-chunk first and last key, and first and last val, parallel to `chunks`.
    first_keys: KeyCon<C>,
    last_keys: KeyCon<C>,
    first_vals: ValCon<C>,
    last_vals: ValCon<C>,
}

impl<C: Chunk> ChunkBatch<C> {
    /// Assemble a batch from ordered chunks, building the per-chunk index.
    pub fn new(chunks: Vec<C>, description: Description<C::Time>) -> Self {
        let n = chunks.len();
        let mut first_keys = <KeyCon<C>>::with_capacity(n);
        let mut last_keys = <KeyCon<C>>::with_capacity(n);
        let mut first_vals = <ValCon<C>>::with_capacity(n);
        let mut last_vals = <ValCon<C>>::with_capacity(n);
        for chunk in &chunks {
            assert!(chunk.len() > 0, "ChunkBatch chunks must be non-empty");
            let ((fk, fv, _), (lk, lv, _)) = chunk.bounds();
            first_keys.push_ref(fk);
            last_keys.push_ref(lk);
            first_vals.push_ref(fv);
            last_vals.push_ref(lv);
        }
        ChunkBatch { chunks, description, first_keys, last_keys, first_vals, last_vals }
    }
}

impl<C: Chunk> WithLayout for ChunkBatch<C> {
    type Layout = C::Layout;
}

/// A cursor over a [`ChunkBatch`], merging the per-chunk cursors.
///
/// Chunk breakpoints are unconstrained, so a single key — or `(key, val)` — may
/// straddle consecutive chunks. But the chunks are one globally-sorted sequence
/// merely cut at arbitrary points, so the operation is *concatenation*, never a
/// merge: across a boundary a key's vals concatenate and a `(key, val)`'s times
/// concatenate. The cursor exploits this. It holds the chunk currently being read
/// and a cursor into it; it seeks by binary-searching the per-chunk index on
/// `ChunkBatch`, and at boundaries it *continues* into the next chunk rather than
/// merging — using the index to detect when a key or `(key, val)` spills forward,
/// without touching chunk contents.
pub struct ChunkBatchCursor<C: Chunk> {
    /// First chunk of the current key's run; where `rewind_vals` returns to.
    key_chunk: usize,
    /// Chunk currently being read; `>= key_chunk`, within the current key's span.
    chunk: usize,
    /// Cursor into `chunk`; `None` once `chunk` is past the last chunk.
    inner: Option<C::Cursor>,
}

impl<C: Chunk> WithLayout for ChunkBatchCursor<C> {
    type Layout = C::Layout;
}

impl<C: Chunk> ChunkBatchCursor<C> {
    /// Move the active chunk to `c`, opening a fresh inner cursor at its start.
    fn goto(&mut self, c: usize, storage: &ChunkBatch<C>) {
        self.chunk = c;
        self.inner = storage.chunks.get(c).map(C::cursor);
    }
}

impl<C: Chunk> Cursor for ChunkBatchCursor<C> {
    type Storage = ChunkBatch<C>;

    fn key_valid(&self, s: &Self::Storage) -> bool { self.chunk < s.chunks.len() && self.inner.as_ref().is_some_and(|i| i.key_valid(&s.chunks[self.chunk])) }
    fn val_valid(&self, s: &Self::Storage) -> bool { self.chunk < s.chunks.len() && self.inner.as_ref().is_some_and(|i| i.val_valid(&s.chunks[self.chunk])) }
    fn key<'a>(&self, s: &'a Self::Storage) -> Self::Key<'a> { self.inner.as_ref().unwrap().key(&s.chunks[self.chunk]) }
    fn val<'a>(&self, s: &'a Self::Storage) -> Self::Val<'a> { self.inner.as_ref().unwrap().val(&s.chunks[self.chunk]) }
    fn get_key<'a>(&self, s: &'a Self::Storage) -> Option<Self::Key<'a>> { if self.key_valid(s) { Some(self.key(s)) } else { None } }
    fn get_val<'a>(&self, s: &'a Self::Storage) -> Option<Self::Val<'a>> { if self.val_valid(s) { Some(self.val(s)) } else { None } }

    fn map_times<L: FnMut(Self::TimeGat<'_>, Self::DiffGat<'_>)>(&mut self, s: &Self::Storage, mut logic: L) {
        if !self.val_valid(s) { return; }
        let (k, v) = (self.key(s), self.val(s));
        self.inner.as_mut().unwrap().map_times(&s.chunks[self.chunk], &mut logic);
        // Follow the (key, val) forward across boundaries while it spills.
        let mut c = self.chunk;
        while c + 1 < s.chunks.len()
            && s.last_keys.index(c) == k && s.first_keys.index(c + 1) == k
            && s.last_vals.index(c) == v && s.first_vals.index(c + 1) == v
        {
            c += 1;
            s.chunks[c].cursor().map_times(&s.chunks[c], &mut logic);
        }
    }

    fn step_key(&mut self, s: &Self::Storage) {
        if !self.key_valid(s) { return; }
        let n = s.chunks.len();
        let k = self.key(s);
        // Advance to the last chunk the key spans.
        while self.chunk + 1 < n && s.last_keys.index(self.chunk) == k && s.first_keys.index(self.chunk + 1) == k {
            self.goto(self.chunk + 1, s);
        }
        // Step past the key within its last chunk.
        {
            let inner = self.inner.as_mut().unwrap();
            inner.seek_key(&s.chunks[self.chunk], k);
            inner.step_key(&s.chunks[self.chunk]);
        }
        // If that exhausted the chunk, the next key (if any) starts the next chunk.
        if !self.inner.as_ref().unwrap().key_valid(&s.chunks[self.chunk]) && self.chunk + 1 < n {
            self.goto(self.chunk + 1, s);
        }
        self.key_chunk = self.chunk;
    }

    fn seek_key(&mut self, s: &Self::Storage, key: Self::Key<'_>) {
        let n = s.chunks.len();
        // First chunk whose last key is `>= key`: where `key`'s run begins.
        let c = s.last_keys.advance(0, n, |x| {
            <KeyCon<C> as BatchContainer>::reborrow(x).lt(&<KeyCon<C> as BatchContainer>::reborrow(key))
        });
        self.goto(c, s);
        self.key_chunk = c;
        if c < n { self.inner.as_mut().unwrap().seek_key(&s.chunks[c], key); }
    }

    fn step_val(&mut self, s: &Self::Storage) {
        if !self.val_valid(s) { return; }
        let n = s.chunks.len();
        let (k, v) = (self.key(s), self.val(s));
        // Advance to the last chunk the (key, val) spans.
        while self.chunk + 1 < n
            && s.last_keys.index(self.chunk) == k && s.first_keys.index(self.chunk + 1) == k
            && s.last_vals.index(self.chunk) == v && s.first_vals.index(self.chunk + 1) == v
        {
            self.goto(self.chunk + 1, s);
        }
        // Step past the (key, val) within that chunk.
        self.inner.as_mut().unwrap().step_val(&s.chunks[self.chunk]);
        // If the key's vals are exhausted here but the key spills, roll forward.
        if !self.inner.as_ref().unwrap().val_valid(&s.chunks[self.chunk])
            && self.chunk + 1 < n && s.last_keys.index(self.chunk) == k && s.first_keys.index(self.chunk + 1) == k
        {
            self.goto(self.chunk + 1, s);
            self.inner.as_mut().unwrap().seek_key(&s.chunks[self.chunk], k);
        }
    }

    fn seek_val(&mut self, s: &Self::Storage, val: Self::Val<'_>) {
        if !self.key_valid(s) { return; }
        let n = s.chunks.len();
        let k = self.key(s);
        loop {
            self.inner.as_mut().unwrap().seek_val(&s.chunks[self.chunk], val);
            if self.inner.as_ref().unwrap().val_valid(&s.chunks[self.chunk]) { return; }
            // Key's vals exhausted in this chunk; if the key spills, retry in the next.
            if self.chunk + 1 < n && s.last_keys.index(self.chunk) == k && s.first_keys.index(self.chunk + 1) == k {
                self.goto(self.chunk + 1, s);
                self.inner.as_mut().unwrap().seek_key(&s.chunks[self.chunk], k);
            } else {
                return;
            }
        }
    }

    fn rewind_keys(&mut self, s: &Self::Storage) {
        self.key_chunk = 0;
        self.goto(0, s);
    }

    fn rewind_vals(&mut self, s: &Self::Storage) {
        if !self.key_valid(s) { return; }
        let k = self.key(s);
        let kc = self.key_chunk;
        self.goto(kc, s);
        self.inner.as_mut().unwrap().seek_key(&s.chunks[kc], k);
    }
}

impl<C: Chunk> BatchReader for ChunkBatch<C> {
    type Cursor = ChunkBatchCursor<C>;
    fn cursor(&self) -> Self::Cursor {
        ChunkBatchCursor { key_chunk: 0, chunk: 0, inner: self.chunks.first().map(C::cursor) }
    }
    fn len(&self) -> usize { self.chunks.iter().map(C::len).sum() }
    fn description(&self) -> &Description<Self::Time> { &self.description }
}

impl<C: Chunk + Default + 'static> Batch for ChunkBatch<C>
where
    C::Time: timely::progress::Timestamp + Lattice + Ord,
{
    type Merger = ChunkBatchMerger<C>;

    fn empty(lower: Antichain<Self::Time>, upper: Antichain<Self::Time>) -> Self {
        use timely::progress::Timestamp;
        let since = Antichain::from_elem(Self::Time::minimum());
        ChunkBatch::new(Vec::new(), Description::new(lower, upper, since))
    }
}

/// Live state of the binary merge: an index into each (shared, immutable) source
/// chain marking the next chunk to clone, and the current head of each (a cloned
/// chunk tagged with its consumed prefix). A head is `None` once its chain is
/// exhausted; the merge proper runs while both are `Some`. The indices are the
/// "cursor positions": the same sources arrive on each `work` call, so they are
/// stable across suspensions.
struct MergeState<C> {
    idx1: usize,
    idx2: usize,
    head1: Option<(usize, C)>,
    head2: Option<(usize, C)>,
}

/// Clone the chunk at `*idx` (if any), advancing `*idx`, tagged with prefix `0`.
fn clone_chunk<C: Clone>(chunks: &[C], idx: &mut usize) -> Option<(usize, C)> {
    let chunk = chunks.get(*idx)?.clone();
    *idx += 1;
    Some((0, chunk))
}

/// A merge of two [`ChunkBatch`]es in progress.
///
/// This is the [`ChunkBatch`] merger, wired in as its
/// [`Batch::Merger`](crate::trace::Batch::Merger), and has that trait's
/// `new` / `work` / `done` shape.
///
/// The merge is *resumable*: `work` drains one [`Chunk::merge`]'s-worth of
/// updates per step, feeding the output into a live [`AdvanceQueue`], and stops once
/// `fuel` is exhausted, retaining the iterators, heads, and advancer for the
/// next call. Fuel is debited by the (consolidated) updates fed into the advancer;
/// summed over all steps this is the total *output*, not the input scanned —
/// matching how the trace's other mergers account (cf. `ord_neu`, which debits the
/// consolidated updates it stages). Compaction's final flush (`done = true`) rides
/// along uncounted, bounded by the data withheld during streaming.
pub struct ChunkBatchMerger<C: Chunk> {
    /// Compaction frontier supplied at construction.
    frontier: Antichain<C::Time>,
    /// Result frontiers, retained for the output description.
    lower: Antichain<C::Time>,
    upper: Antichain<C::Time>,
    /// Merged-and-advanced chunks, grown by `work`.
    result: ChunkList<C>,
    /// Live merge state; `None` before the first `work` and after merging completes.
    state: Option<MergeState<C>>,
    /// Live advancer; `Some` until its final flush, then `None`.
    advancer: Option<AdvanceQueue<C>>,
    /// Whether the inputs have been moved into `state` yet.
    initialized: bool,
}

impl<C> crate::trace::Merger<ChunkBatch<C>> for ChunkBatchMerger<C>
where
    C: Chunk + Default + 'static,
    C::Time: timely::progress::Timestamp + Lattice + Ord + 'static,
{
    /// Begin merging `source1` and `source2`, advancing to `frontier`.
    fn new(source1: &ChunkBatch<C>, source2: &ChunkBatch<C>, frontier: AntichainRef<C::Time>) -> Self {
        let lower = source1.description.lower().meet(source2.description.lower());
        let upper = source1.description.upper().join(source2.description.upper());
        Self {
            frontier: frontier.to_owned(),
            lower,
            upper,
            result: ChunkList::default(),
            state: None,
            advancer: None,
            initialized: false,
        }
    }

    /// Advance the merge by up to `fuel` updates, suspending when it runs out.
    ///
    /// The sources are read by *cloning* chunks (a cheap refcount bump, per the
    /// [`Chunk`] contract), never consumed or mutated, so they remain shared and
    /// immutable. The same `source1`/`source2` must be supplied on every call.
    fn work(&mut self, source1: &ChunkBatch<C>, source2: &ChunkBatch<C>, fuel: &mut isize) {
        if !self.initialized {
            let mut idx1 = 0;
            let mut idx2 = 0;
            let head1 = clone_chunk(&source1.chunks, &mut idx1);
            let head2 = clone_chunk(&source2.chunks, &mut idx2);
            self.state = Some(MergeState { idx1, idx2, head1, head2 });
            self.advancer = Some(AdvanceQueue::new(self.frontier.clone()));
            self.initialized = true;
        }

        while *fuel > 0 {
            let state = match &mut self.state { Some(s) => s, None => break };
            let advancer = self.advancer.as_mut().unwrap();

            if state.head1.is_some() && state.head2.is_some() {
                // One merge step: present both heads, refill whichever drains.
                let mut window = [state.head1.take().unwrap(), state.head2.take().unwrap()];
                let mut merged = ChunkList::default();
                C::merge(&mut window, &mut merged);
                let [(p1, c1), (p2, c2)] = window;
                state.head1 = if p1 >= c1.len() { clone_chunk(&source1.chunks, &mut state.idx1) } else { Some((p1, c1)) };
                state.head2 = if p2 >= c2.len() { clone_chunk(&source2.chunks, &mut state.idx2) } else { Some((p2, c2)) };
                let chunks = merged.done();
                let work: usize = chunks.iter().map(C::len).sum();
                advancer.push(chunks, &mut self.result);
                *fuel -= work as isize;
            } else if let Some((consumed, chunk)) = state.head1.take().or_else(|| state.head2.take()) {
                // One chain exhausted; flush the partially-consumed head of the
                // other. It was retained with `consumed < len`, so the pruned
                // remainder is non-empty.
                let chunk = if consumed > 0 { chunk.prune(consumed) } else { chunk };
                let work = chunk.len();
                advancer.push(std::iter::once(chunk), &mut self.result);
                *fuel -= work as isize;
            } else if let Some((_, chunk)) = clone_chunk(&source1.chunks, &mut state.idx1).or_else(|| clone_chunk(&source2.chunks, &mut state.idx2)) {
                // Flush the untouched tail of the surviving chain, one chunk per step.
                let work = chunk.len();
                advancer.push(std::iter::once(chunk), &mut self.result);
                *fuel -= work as isize;
            } else {
                // Both chains fully fed; flush withheld advancement and retire.
                self.state = None;
                if let Some(advancer) = self.advancer.take() {
                    advancer.finish(&mut self.result);
                }
                break;
            }
        }
    }

    /// Extract the merged batch over `[lower, upper)` advanced to the frontier.
    ///
    /// Only valid once `work` has driven the merge to completion (left `fuel`
    /// positive), as the [`trace::Merger`](crate::trace::Merger) contract requires.
    fn done(self) -> ChunkBatch<C> {
        let description = Description::new(self.lower, self.upper, self.frontier);
        ChunkBatch::new(self.result.done(), description)
    }
}

/// A [`Builder`](crate::trace::Builder) that collects pre-sorted chunks into a
/// [`ChunkBatch`].
///
/// The builder assumes its inputs arrive already sorted and consolidated (as the
/// `Builder` contract requires), so it does no merging: each pushed chunk is an
/// ordered run, appended in order. They accumulate in a [`ChunkList`], which
/// regrades them to the size invariant as they arrive — so a batch built here is
/// graded like one produced by the merger, rather than inheriting whatever chunk
/// sizes the caller happened to push.
pub struct ChunkBuilder<C: Chunk> {
    chunks: ChunkList<C>,
}

impl<C> crate::trace::Builder for ChunkBuilder<C>
where
    C: Chunk + Default + 'static,
    C::Time: timely::progress::Timestamp,
{
    type Input = C;
    type Time = C::Time;
    type Output = ChunkBatch<C>;

    fn with_capacity(_keys: usize, _vals: usize, _upds: usize) -> Self {
        Self { chunks: ChunkList::default() }
    }

    fn push(&mut self, chunk: &mut C) {
        let chunk = std::mem::take(chunk);
        if chunk.len() > 0 { self.chunks.push(chunk); }
    }

    fn done(self, description: Description<C::Time>) -> ChunkBatch<C> {
        ChunkBatch::new(self.chunks.done(), description)
    }

    fn seal(chain: &mut Vec<C>, description: Description<C::Time>) -> ChunkBatch<C> {
        // The chain is sorted and consolidated but not necessarily graded; regrade
        // it. Already-sized chunks pass through as cheap `Rc` moves, so a chain that
        // arrives graded (as the batcher's does) pays only an O(#chunks) walk.
        let mut chunks = ChunkList::default();
        chunks.extend(std::mem::take(chain));
        ChunkBatch::new(chunks.done(), description)
    }
}

pub mod vec_chunk {
    //! A worked [`Chunk`] implementation: `Vec<((K, V), T, R)>` behind an `Rc`.
    //!
    //! This is the reference example — a next implementor (e.g. columnar) follows
    //! its *shape*, not its layout. It shows the two integration points any chunk
    //! type satisfies, and how leaning on the parent module's generic harnesses
    //! keeps the code terse:
    //!
    //! * **Batcher side.** The merge batcher's `ContainerChunker` builds chunks, so
    //!   the type implements timely's container traits (`Accountable`,
    //!   `SizableContainer`, `Consolidate`, `PushInto`). Here they delegate to the
    //!   inner `Vec` via `Rc::make_mut` — free while a chunk is being built
    //!   (refcount 1), and it never copies a *shared* chunk because batches are
    //!   immutable once built.
    //! * **Trace side.** [`Chunk`] (merge / extract / advance / prune / bounds)
    //!   plus a cursor. Key lookups are logarithmic by galloping search (`seek_*`),
    //!   independent of chunk size; stepping stays linear (short hops).
    //!
    //! `Clone` is a refcount bump, so the trace merger shares source chunks instead
    //! of copying them.

    use std::marker::PhantomData;
    use std::rc::Rc;

    use timely::Accountable;
    use timely::container::{PushInto, SizableContainer};
    use timely::progress::{Antichain, Timestamp};

    use crate::consolidation::Consolidate;
    use crate::difference::Semigroup;
    use crate::lattice::Lattice;
    use crate::trace::cursor::Cursor;
    use crate::trace::implementations::{Vector, WithLayout};

    use super::{Chunk, ChunkFeed, ChunkList};

    /// The chunk size: both the maximum updates per chunk and the coalescing
    /// threshold (see [`Chunk::TARGET`]). Chosen for the reference impl; exposed as
    /// the associated const below, and used internally for buffer sizing.
    const TARGET: usize = 1024;

    /// A sorted, consolidated run of `((key, val), time, diff)`, shared via `Rc`.
    pub struct VecChunk<K, V, T, R>(Rc<Vec<((K, V), T, R)>>);

    impl<K, V, T, R> Clone for VecChunk<K, V, T, R> {
        fn clone(&self) -> Self { VecChunk(Rc::clone(&self.0)) }
    }
    impl<K, V, T, R> Default for VecChunk<K, V, T, R> {
        fn default() -> Self { VecChunk(Rc::new(Vec::new())) }
    }

    /// The trace type for `arrange`: a spine of `Rc`-shared chunk batches.
    pub type ChunkSpine<K, V, T, R> = super::ChunkSpine<VecChunk<K, V, T, R>>;
    /// Merge batcher over `VecChunk`s. Unordered `Vec<((K, V), T, R)>` input is
    /// consolidated into sorted `VecChunk`s by a `ContainerChunker<VecChunk>` supplied
    /// at the `arrange_core` callsite (it drives the container-trait impls below); the
    /// batcher itself only merges the resulting chunks.
    pub type ChunkBatcher<K, V, T, R> = super::ChunkBatcher<VecChunk<K, V, T, R>>;
    /// Reference-counted batch builder.
    pub type ChunkRcBuilder<K, V, T, R> = super::ChunkRcBuilder<VecChunk<K, V, T, R>>;

    // --- batcher side: timely container traits, delegating to the inner `Vec` ---

    impl<K: 'static, V: 'static, T: 'static, R: 'static> Accountable for VecChunk<K, V, T, R> {
        fn record_count(&self) -> i64 { self.0.len() as i64 }
    }

    impl<K, V, T, R> SizableContainer for VecChunk<K, V, T, R>
    where K: Clone+'static, V: Clone+'static, T: Clone+'static, R: Clone+'static {
        fn at_capacity(&self) -> bool { self.0.at_capacity() }
        fn ensure_capacity(&mut self, _stash: &mut Option<Self>) {
            Rc::make_mut(&mut self.0).ensure_capacity(&mut None);
        }
    }

    impl<K, V, T, R> Consolidate for VecChunk<K, V, T, R>
    where K: Ord+Clone+'static, V: Ord+Clone+'static, T: Ord+Clone+'static, R: Semigroup+'static {
        fn len(&self) -> usize { self.0.len() }
        fn clear(&mut self) { Rc::make_mut(&mut self.0).clear() }
        fn consolidate_into(&mut self, target: &mut Self) {
            Rc::make_mut(&mut self.0).consolidate_into(Rc::make_mut(&mut target.0));
        }
    }

    impl<K, V, T, R> PushInto<((K, V), T, R)> for VecChunk<K, V, T, R>
    where K: Clone+'static, V: Clone+'static, T: Clone+'static, R: Clone+'static {
        fn push_into(&mut self, item: ((K, V), T, R)) { Rc::make_mut(&mut self.0).push(item); }
    }

    // --- trace side: a logarithmic cursor and the `Chunk` operations ---

    /// First index `>= start` at which `pred` turns false, by galloping (exponential)
    /// search. `pred` must hold for a prefix then not — i.e. `|u| u < target`.
    /// O(log distance), so O(1) for short hops and logarithmic for long ones.
    fn gallop<U>(s: &[U], start: usize, pred: impl Fn(&U) -> bool) -> usize {
        let mut pos = start;
        if pos < s.len() && pred(&s[pos]) {
            let mut step = 1;
            while pos + step < s.len() && pred(&s[pos + step]) { pos += step; step <<= 1; }
            step >>= 1;
            while step > 0 {
                if pos + step < s.len() && pred(&s[pos + step]) { pos += step; }
                step >>= 1;
            }
            pos += 1;
        }
        pos
    }

    /// A cursor over a [`VecChunk`], tracking the current key and `(key, val)`
    /// group starts as indices into the flat vector.
    pub struct VecChunkCursor<K, V, T, R> {
        key_pos: usize,
        val_pos: usize,
        phantom: PhantomData<(K, V, T, R)>,
    }

    impl<K, V, T, R> WithLayout for VecChunk<K, V, T, R>
    where K: Ord+Clone+'static, V: Ord+Clone+'static, T: Lattice+Timestamp, R: Ord+Semigroup+'static {
        type Layout = Vector<((K, V), T, R)>;
    }

    impl<K, V, T, R> WithLayout for VecChunkCursor<K, V, T, R>
    where K: Ord+Clone+'static, V: Ord+Clone+'static, T: Lattice+Timestamp, R: Ord+Semigroup+'static {
        type Layout = Vector<((K, V), T, R)>;
    }

    impl<K, V, T, R> Cursor for VecChunkCursor<K, V, T, R>
    where K: Ord+Clone+'static, V: Ord+Clone+'static, T: Lattice+Timestamp, R: Ord+Semigroup+'static {
        type Storage = VecChunk<K, V, T, R>;

        fn key_valid(&self, s: &Self::Storage) -> bool { self.key_pos < s.0.len() }
        fn val_valid(&self, s: &Self::Storage) -> bool {
            self.key_pos < s.0.len() && self.val_pos < s.0.len() && s.0[self.val_pos].0.0 == s.0[self.key_pos].0.0
        }
        fn key<'a>(&self, s: &'a Self::Storage) -> &'a K { &s.0[self.key_pos].0.0 }
        fn val<'a>(&self, s: &'a Self::Storage) -> &'a V { &s.0[self.val_pos].0.1 }
        fn get_key<'a>(&self, s: &'a Self::Storage) -> Option<&'a K> {
            if self.key_valid(s) { Some(self.key(s)) } else { None }
        }
        fn get_val<'a>(&self, s: &'a Self::Storage) -> Option<&'a V> {
            if self.val_valid(s) { Some(self.val(s)) } else { None }
        }
        fn map_times<L: FnMut(&T, &R)>(&mut self, s: &Self::Storage, mut logic: L) {
            if !self.val_valid(s) { return; }
            let kv = &s.0[self.val_pos].0;
            let mut i = self.val_pos;
            while i < s.0.len() && &s.0[i].0 == kv {
                logic(&s.0[i].1, &s.0[i].2);
                i += 1;
            }
        }
        fn step_key(&mut self, s: &Self::Storage) {
            // Linear: stepping is a short hop to the next group; an inlined scan
            // beats a gallop call for the common small-group case.
            if self.key_pos >= s.0.len() { return; }
            let key = s.0[self.key_pos].0.0.clone();
            let mut i = self.key_pos;
            while i < s.0.len() && s.0[i].0.0 == key { i += 1; }
            self.key_pos = i;
            self.val_pos = i;
        }
        fn seek_key(&mut self, s: &Self::Storage, key: &K) {
            // Logarithmic: O(log distance), independent of chunk size.
            self.key_pos = gallop(&s.0, self.key_pos, |u| &u.0.0 < key);
            self.val_pos = self.key_pos;
        }
        fn step_val(&mut self, s: &Self::Storage) {
            if !self.val_valid(s) { return; }
            let kv = s.0[self.val_pos].0.clone();
            let mut i = self.val_pos;
            while i < s.0.len() && s.0[i].0 == kv { i += 1; }
            self.val_pos = i;
        }
        fn seek_val(&mut self, s: &Self::Storage, val: &V) {
            if !self.key_valid(s) { return; }
            let key = s.0[self.key_pos].0.0.clone();
            self.val_pos = gallop(&s.0, self.val_pos, |u| (&u.0.0, &u.0.1) < (&key, val));
        }
        fn rewind_keys(&mut self, _s: &Self::Storage) { self.key_pos = 0; self.val_pos = 0; }
        fn rewind_vals(&mut self, _s: &Self::Storage) { self.val_pos = self.key_pos; }
    }

    /// Take the `Vec` out of a chunk, copying only if the `Rc` is shared.
    fn take<K: Clone, V: Clone, T: Clone, R: Clone>(chunk: VecChunk<K, V, T, R>) -> Vec<((K, V), T, R)> {
        Rc::try_unwrap(chunk.0).unwrap_or_else(|rc| (*rc).clone())
    }

    impl<K, V, T, R> Chunk for VecChunk<K, V, T, R>
    where K: Ord+Clone+'static, V: Ord+Clone+'static, T: Lattice+Timestamp, R: Ord+Semigroup+'static {
        type Cursor = VecChunkCursor<K, V, T, R>;

        const TARGET: usize = TARGET;

        fn cursor(&self) -> Self::Cursor {
            VecChunkCursor { key_pos: 0, val_pos: 0, phantom: PhantomData }
        }

        fn bounds(&self) -> ((&K, &V, &T), (&K, &V, &T)) {
            let s = &self.0[..];
            let (f, l) = (&s[0], &s[s.len() - 1]);
            ((&f.0.0, &f.0.1, &f.1), (&l.0.0, &l.0.1, &l.1))
        }

        fn len(&self) -> usize { self.0.len() }

        fn prune(self, prefix: usize) -> Self {
            let mut v = take(self);
            v.drain(..prefix);
            VecChunk(Rc::new(v))
        }

        fn merge(chunks: &mut [(usize, Self)], out: &mut ChunkList<Self>) {
            let mut consumed: Vec<usize> = chunks.iter().map(|(c, _)| *c).collect();
            {
                let inputs: Vec<&[_]> = chunks.iter().map(|(_, ch)| &ch.0[..]).collect();
                merge_buf(&inputs, &mut consumed, out);
            }
            for (i, (c, _)) in chunks.iter_mut().enumerate() { *c = consumed[i]; }
        }

        fn extract(
            chunks: &mut Vec<Self>,
            frontier: &Antichain<T>,
            residual: &mut Antichain<T>,
            keep: &mut ChunkList<Self>,
            ship: &mut ChunkList<Self>,
        ) {
            // Fill `TARGET`-sized buffers directly, so the chunks pushed are already
            // graded and `regrade` passes them through as `Rc` moves rather than
            // re-splitting (and re-copying) a monolithic chunk. Emptied input `Vec`s
            // are recycled as the next buffers, so allocations balance input against
            // output instead of one fresh buffer per emitted chunk.
            let mut stash: Vec<Vec<((K, V), T, R)>> = Vec::new();
            let take_buf = |stash: &mut Vec<_>| stash.pop().unwrap_or_default();
            let (mut k, mut s) = (take_buf(&mut stash), take_buf(&mut stash));
            for chunk in chunks.drain(..) {
                let mut v = take(chunk);
                for u in v.drain(..) {
                    if frontier.borrow().less_equal(&u.1) {
                        residual.insert_ref(&u.1);
                        k.push(u);
                        if k.len() >= TARGET { keep.push(VecChunk(Rc::new(std::mem::replace(&mut k, take_buf(&mut stash))))); }
                    } else {
                        s.push(u);
                        if s.len() >= TARGET { ship.push(VecChunk(Rc::new(std::mem::replace(&mut s, take_buf(&mut stash))))); }
                    }
                }
                stash.push(v);
            }
            if !k.is_empty() { keep.push(VecChunk(Rc::new(k))); }
            if !s.is_empty() { ship.push(VecChunk(Rc::new(s))); }
        }

        fn advance(
            feed: &mut ChunkFeed<Self>,
            frontier: &Antichain<T>,
            done: bool,
            out: &mut ChunkList<Self>,
        ) {
            // Advance and consolidate every *complete* `(key, val)` group eagerly,
            // so its updates can be released as soon as the input proves no later
            // time for the pair can arrive. A group is contiguous in the sorted
            // chain, so the only one that might continue in a future push is the
            // last; unless `done`, we process up to its start and withhold the rest
            // as the head for the next call.
            let mut stash: Vec<Vec<((K, V), T, R)>> = Vec::new();
            let (consumed, ch) = &mut feed.0;
            // Build the working buffer by *reusing the head's storage* and appending
            // the tail (recycling each emptied tail `Vec`). Reusing the head is what
            // keeps a withheld group from being recopied across calls: it just
            // accumulates in place, so a `(key, val)` larger than the working set
            // costs O(total) over the run rather than O(total²).
            let mut buf = take(std::mem::take(ch));
            if *consumed > 0 { buf.drain(..*consumed); *consumed = 0; }
            for chunk in feed.1.drain(..) {
                let mut v = take(chunk);
                buf.append(&mut v);
                stash.push(v);
            }
            if buf.is_empty() { return; }

            // If every available update shares one `(key, val)`, no group is provably
            // complete — the next push may extend it — so make no progress unless
            // `done`: retain the accumulated buffer as the head and return. This is
            // the giant-key case; comparing only the first and last pair detects it
            // without scanning, and reusing the head above makes the retention free.
            if !done && buf[0].0 == buf[buf.len() - 1].0 {
                *ch = VecChunk(Rc::new(buf));
                return;
            }

            // Otherwise at least the first group is complete. Withhold the last group
            // (a single `(key, val)`) as the next head unless the input is complete.
            let end = if done { buf.len() } else {
                let last_kv = buf[buf.len() - 1].0.clone();
                let mut start = buf.len();
                while start > 0 && buf[start - 1].0 == last_kv { start -= 1; }
                start
            };
            if end < buf.len() {
                let tail = buf.split_off(end);
                *ch = VecChunk(Rc::new(tail));
            }
            // Advance + consolidate each group into `TARGET`-sized output chunks,
            // filling buffers reclaimed from the recycled tail `Vec`s.
            let mut result = stash.pop().unwrap_or_default();
            let mut i = 0;
            while i < buf.len() {
                let mut j = i;
                while j < buf.len() && buf[j].0 == buf[i].0 { j += 1; }
                for u in &mut buf[i..j] { u.1.advance_by(frontier.borrow()); }
                // Advancing is monotone w.r.t. the lattice but not the
                // representation's total order, so re-sort the group by time.
                buf[i..j].sort_by(|a, b| a.1.cmp(&b.1));
                let mut k = i;
                while k < j {
                    let kv = buf[k].0.clone();
                    let t = buf[k].1.clone();
                    let mut diff = buf[k].2.clone();
                    k += 1;
                    while k < j && buf[k].1 == t { diff.plus_equals(&buf[k].2); k += 1; }
                    if !diff.is_zero() {
                        result.push((kv, t, diff));
                        if result.len() >= TARGET { out.push(VecChunk(Rc::new(std::mem::replace(&mut result, stash.pop().unwrap_or_default())))); }
                    }
                }
                i = j;
            }
            if !result.is_empty() { out.push(VecChunk(Rc::new(result))); }
        }

        fn regrade(queue: &mut Vec<Self>, done: bool, out: &mut Vec<Self>) {
            // Maximal packing: emit chunks as large as possible up to `TARGET`,
            // never splitting a pair that could combine into one legal (`<= TARGET`)
            // chunk. A chunk of exactly `TARGET` is maximal — it cannot grow — so it
            // passes straight through as an `Rc` move; only sub-`TARGET` chunks are
            // copied, and only to coalesce with a neighbour. Producers fill to
            // `TARGET`, so in steady state every chunk passes through and only the
            // occasional trailing partial is coalesced.
            //
            // `carry` is the (sub-`TARGET`) chunk under construction. It is flushed
            // once it reaches `TARGET`, carried back onto `queue` between calls, or
            // emitted on `done`. Whenever `carry` is non-empty its left neighbour in
            // `out` is a `TARGET` chunk (or `carry` is `out`'s first chunk), so
            // emitting `carry` against a neighbour it cannot merge with — their sum
            // exceeds `TARGET` — keeps the packing maximal on both sides.
            let mut carry: Vec<((K, V), T, R)> = Vec::new();
            for chunk in queue.drain(..) {
                if carry.is_empty() {
                    absorb(chunk, &mut carry, out);
                } else if carry.len() + chunk.0.len() <= TARGET {
                    // Combines into one legal chunk; coalesce in place.
                    carry.extend(take(chunk));
                    if carry.len() == TARGET {
                        out.push(VecChunk(Rc::new(std::mem::take(&mut carry))));
                    }
                } else {
                    // Cannot combine without exceeding `TARGET`; `carry` is maximal
                    // against this neighbour, so emit it and absorb the chunk afresh.
                    out.push(VecChunk(Rc::new(std::mem::take(&mut carry))));
                    absorb(chunk, &mut carry, out);
                }
            }
            if !carry.is_empty() {
                let chunk = VecChunk(Rc::new(carry));
                if done { out.push(chunk); } else { queue.push(chunk); }
            }
        }
    }

    /// Emit maximal `TARGET`-sized chunks off the front of `carry`, leaving the
    /// sub-`TARGET` tail behind.
    fn peel<K: Clone, V: Clone, T: Clone, R: Clone>(
        carry: &mut Vec<((K, V), T, R)>,
        out: &mut Vec<VecChunk<K, V, T, R>>,
    ) {
        let mut start = 0;
        while carry.len() - start >= TARGET {
            out.push(VecChunk(Rc::new(carry[start..start + TARGET].to_vec())));
            start += TARGET;
        }
        carry.drain(..start);
    }

    /// Absorb a chunk when nothing is carried: pass a `TARGET` chunk through as an
    /// `Rc` move, hold a smaller one in `carry`, or split a larger one (peeling off
    /// `TARGET` pieces and carrying the remainder). `carry` must be empty on entry.
    fn absorb<K: Clone, V: Clone, T: Clone, R: Clone>(
        chunk: VecChunk<K, V, T, R>,
        carry: &mut Vec<((K, V), T, R)>,
        out: &mut Vec<VecChunk<K, V, T, R>>,
    ) {
        use std::cmp::Ordering::{Equal, Greater, Less};
        match chunk.0.len().cmp(&TARGET) {
            Equal => out.push(chunk),
            Less => *carry = take(chunk),
            Greater => { *carry = take(chunk); peel(carry, out); }
        }
    }

    /// K-way merge of in-range prefixes of sorted, consolidated inputs, emitting
    /// graded chunks directly into `out`.
    ///
    /// `inputs[i][consumed[i]..]` is the unconsumed, sorted suffix of input `i`.
    /// Merges through the least last `((key, val), time)` across inputs (nothing
    /// interleaves below it), consolidating triples shared across inputs, and
    /// advances each `consumed[i]` past what it merged. Output is filled into
    /// `TARGET`-sized buffers and pushed as it fills, so the run arrives *graded*
    /// rather than as one monolithic chunk that `regrade` would re-split (and
    /// re-copy) — mirroring `extract`. Sizing buffers to `TARGET` also avoids the
    /// over-reservation a single up-front `with_capacity(total)` would incur.
    fn merge_buf<K, V, T, R>(
        inputs: &[&[((K, V), T, R)]],
        consumed: &mut [usize],
        out: &mut ChunkList<VecChunk<K, V, T, R>>,
    )
    where K: Ord+Clone+'static, V: Ord+Clone+'static, T: Lattice+Timestamp, R: Ord+Semigroup+'static {
        let Some(horizon) = inputs.iter().enumerate()
            .filter(|(i, s)| consumed[*i] < s.len())
            .map(|(_, s)| { let u = &s[s.len() - 1]; (u.0.clone(), u.1.clone()) })
            .min()
        else { return; };

        let in_range = |i: usize, p: usize| {
            p < inputs[i].len() && (&inputs[i][p].0, &inputs[i][p].1) <= (&horizon.0, &horizon.1)
        };

        let mut result: Vec<((K, V), T, R)> = Vec::with_capacity(TARGET);
        loop {
            let mut best: Option<usize> = None;
            for i in 0..inputs.len() {
                if in_range(i, consumed[i]) && best.is_none_or(|b| {
                    let (bi, bb) = (&inputs[i][consumed[i]], &inputs[b][consumed[b]]);
                    (&bi.0, &bi.1) < (&bb.0, &bb.1)
                }) {
                    best = Some(i);
                }
            }
            let Some(b) = best else { break; };
            let kv = inputs[b][consumed[b]].0.clone();
            let t = inputs[b][consumed[b]].1.clone();
            let mut diff: Option<R> = None;
            for i in 0..inputs.len() {
                if in_range(i, consumed[i]) && inputs[i][consumed[i]].0 == kv && inputs[i][consumed[i]].1 == t {
                    match &mut diff {
                        None => diff = Some(inputs[i][consumed[i]].2.clone()),
                        Some(d) => d.plus_equals(&inputs[i][consumed[i]].2),
                    }
                    consumed[i] += 1;
                }
            }
            if let Some(diff) = diff {
                if !diff.is_zero() {
                    result.push((kv, t, diff));
                    if result.len() >= TARGET {
                        out.push(VecChunk(Rc::new(std::mem::replace(&mut result, Vec::with_capacity(TARGET)))));
                    }
                }
            }
        }
        if !result.is_empty() { out.push(VecChunk(Rc::new(result))); }
    }

    #[cfg(test)]
    mod test {
        use super::VecChunk;
        use crate::trace::chunk::merge_chains;
        use std::rc::Rc;

        fn chunk(updates: Vec<((u64, u64), u64, i64)>) -> VecChunk<u64, u64, u64, i64> {
            VecChunk(Rc::new(updates))
        }

        // `extract` must partition by frontier, fold the kept frontier into
        // `residual`, and emit graded chunks directly — without leaning on a regrade
        // re-split.
        #[test]
        fn extract_partitions_and_grades() {
            use super::{Chunk, TARGET};
            use crate::trace::chunk::{is_graded, ChunkList};
            use timely::progress::Antichain;

            // 4·TARGET updates spread over many input chunks; even times ship
            // (< frontier), odd times keep (>= frontier), so both sides straddle.
            let n = 4 * TARGET as u64;
            let input: Vec<_> = (0..n)
                .map(|i| chunk(vec![((i, 0), i % 2, 1)]))
                .collect();
            let mut chunks = input;
            let frontier = Antichain::from_elem(1u64);
            let mut residual = Antichain::new();
            let (mut keep, mut ship) = (ChunkList::default(), ChunkList::default());
            VecChunk::extract(&mut chunks, &frontier, &mut residual, &mut keep, &mut ship);
            let (keep, ship) = (keep.done(), ship.done());

            // Kept times are exactly {1}; that is the residual frontier.
            assert_eq!(residual, Antichain::from_elem(1u64));
            // Both sides emerge graded directly from `extract`.
            assert!(is_graded(&keep), "ungraded keep: {:?}", keep.iter().map(Chunk::len).collect::<Vec<_>>());
            assert!(is_graded(&ship), "ungraded ship: {:?}", ship.iter().map(Chunk::len).collect::<Vec<_>>());
            // Nothing lost: half the updates each way.
            assert_eq!(keep.iter().map(Chunk::len).sum::<usize>(), n as usize / 2);
            assert_eq!(ship.iter().map(Chunk::len).sum::<usize>(), n as usize / 2);
        }

        // `advance` advances and consolidates complete `(key, val)` groups eagerly,
        // withholding only the (possibly-growing) last group when not `done`.
        #[test]
        fn advance_emits_complete_groups_eagerly() {
            use super::Chunk;
            use crate::trace::chunk::ChunkList;
            use timely::progress::Antichain;

            let frontier = Antichain::from_elem(5u64);
            // Group (0,0) is complete within this chunk; group (1,0) might still grow.
            let c0 = chunk(vec![((0, 0), 0, 1), ((0, 0), 1, 1), ((1, 0), 0, 1)]);
            let mut feed = ((0usize, VecChunk::default()), vec![c0]);
            let mut out = ChunkList::default();
            VecChunk::advance(&mut feed, &frontier, false, &mut out);

            // The trailing group (1,0) is withheld as the head for the next call.
            assert_eq!(Chunk::len(&feed.0.1), 1);
            assert!(feed.1.is_empty());
            // Group (0,0)'s times {0,1} advanced to 5 and consolidated, emitted now.
            let emitted: Vec<_> = out.done().into_iter().flat_map(|c| (*c.0).clone()).collect();
            assert_eq!(emitted, vec![((0, 0), 5, 2)]);
        }

        // Streaming the input one chunk at a time must yield exactly what a single
        // all-at-once flush does — the resumable path is just the one-shot path cut
        // at group boundaries.
        #[test]
        fn advance_resumable_matches_oneshot() {
            use crate::trace::chunk::{AdvanceQueue, ChunkList};
            use timely::progress::Antichain;

            let frontier = Antichain::from_elem(3u64);
            // Groups span chunk boundaries and carry several times each.
            let input = || vec![
                chunk(vec![((0, 0), 0, 1), ((0, 0), 1, 1), ((1, 0), 0, 1)]),
                chunk(vec![((1, 0), 5, 1), ((1, 1), 0, 1), ((2, 0), 0, 1)]),
                chunk(vec![((2, 0), 2, 1), ((2, 0), 9, 1)]),
            ];
            let flat = |v: Vec<VecChunk<u64, u64, u64, i64>>|
                v.into_iter().flat_map(|c| (*c.0).clone()).collect::<Vec<_>>();

            let oneshot = {
                let mut q = AdvanceQueue::new(frontier.clone());
                let mut out = ChunkList::default();
                q.push(input(), &mut out);
                q.finish(&mut out);
                flat(out.done())
            };
            let incremental = {
                let mut q = AdvanceQueue::new(frontier.clone());
                let mut out = ChunkList::default();
                for c in input() { q.push(std::iter::once(c), &mut out); }
                q.finish(&mut out);
                flat(out.done())
            };
            assert_eq!(oneshot, incremental);
            // Times are advanced: nothing below the frontier survives.
            for u in &oneshot { assert!(u.1 >= 3); }
        }

        // A single `(key, val)` whose updates span every pushed chunk: `advance`
        // can make no progress until `done`, accumulating in the head in place.
        // It must still produce the right advanced+consolidated result at the end.
        #[test]
        fn advance_single_key_spanning_pushes() {
            use crate::trace::chunk::{AdvanceQueue, ChunkList};
            use timely::progress::Antichain;

            let frontier = Antichain::from_elem(100u64);
            let n = 50u64;
            let make = || (0..n).map(|t| chunk(vec![((7u64, 0u64), t, 1i64)])).collect::<Vec<_>>();
            let flat = |v: Vec<VecChunk<u64, u64, u64, i64>>|
                v.into_iter().flat_map(|c| (*c.0).clone()).collect::<Vec<_>>();

            let mut q = AdvanceQueue::new(frontier);
            let mut out = ChunkList::default();
            for c in make() { q.push(std::iter::once(c), &mut out); }
            q.finish(&mut out);
            // All times advance to 100 and consolidate to one update of diff `n`.
            assert_eq!(flat(out.done()), vec![((7u64, 0u64), 100u64, n as i64)]);
        }

        #[test]
        fn merge_chains_consolidates() {
            let a = chunk(vec![((0, 0), 0, 1), ((1, 0), 0, 1)]);
            let b = chunk(vec![((0, 0), 0, 1), ((2, 0), 0, 1)]);
            let mut out = crate::trace::chunk::ChunkList::default();
            merge_chains(vec![a], vec![b], &mut out);
            let merged: Vec<_> = out.done().into_iter().flat_map(|c| (*c.0).clone()).collect();
            assert_eq!(merged, vec![((0, 0), 0, 2), ((1, 0), 0, 1), ((2, 0), 0, 1)]);
        }

        // Merging runs larger than `TARGET` must emit a *graded* sequence directly
        // (each chunk `<= TARGET`, adjacent pairs summing past `TARGET`), not one
        // monolithic chunk, while reproducing the consolidated sorted contents.
        #[test]
        fn merge_emits_graded_chunks() {
            use super::{Chunk, TARGET};
            use crate::trace::chunk::{ChunkList, is_graded, merge_chains};

            // Two interleaving single-chunk chains: evens and odds over `0..4·TARGET`.
            let n = 4 * TARGET as u64;
            let evens = chunk((0..n).step_by(2).map(|k| ((k, 0), 0, 1)).collect());
            let odds = chunk((0..n).step_by(2).map(|k| ((k + 1, 0), 0, 1)).collect());

            let mut out = ChunkList::default();
            merge_chains(vec![evens], vec![odds], &mut out);
            let chunks = out.done();

            assert!(is_graded(&chunks), "merge output not graded: {:?}",
                chunks.iter().map(Chunk::len).collect::<Vec<_>>());
            // Contents are exactly the sorted keys `0..4·TARGET`, each once.
            let merged: Vec<_> = chunks.into_iter().flat_map(|c| (*c.0).clone()).collect();
            let want: Vec<_> = (0..n).map(|k| ((k, 0u64), 0u64, 1i64)).collect();
            assert_eq!(merged, want);
        }

        // `regrade` must produce a *maximal packing*: adjacent sub-`TARGET` chunks
        // that could combine into one legal chunk are coalesced (the prior rule left
        // any pair summing past `TARGET/2` alone), full chunks pass through, and
        // contents are preserved exactly.
        #[test]
        fn regrade_maximal_packing() {
            use super::{Chunk, TARGET};
            use crate::trace::chunk::{is_graded, ChunkList};

            // A mix of small and full chunks with distinct, increasing keys (so the
            // concatenation is sorted and nothing consolidates away).
            let t = TARGET;
            let sizes = [t / 3, t / 3, t / 3, t, t / 2, t / 2, t, 1, t - 1];
            let total: usize = sizes.iter().sum();
            let mut key = 0u64;
            let mut list = ChunkList::default();
            for &s in &sizes {
                let updates: Vec<_> = (0..s).map(|_| { let k = key; key += 1; ((k, 0u64), 0u64, 1i64) }).collect();
                list.push(chunk(updates));
            }
            let chunks = list.done();

            assert!(is_graded(&chunks), "not graded: {:?}",
                chunks.iter().map(Chunk::len).collect::<Vec<_>>());
            // Nothing lost, and the keys stay strictly sorted across the new breaks.
            let got: Vec<_> = chunks.into_iter().flat_map(|c| (*c.0).clone()).collect();
            assert_eq!(got.len(), total);
            assert!(got.windows(2).all(|w| w[0].0.0 < w[1].0.0));
        }

        // The indexed cursor must reconstruct the same grouped updates as a flat
        // reference, even when a key — and a `(key, val)`'s times — straddle a
        // chunk boundary.
        #[test]
        fn cursor_handles_straddle() {
            use crate::trace::cursor::Cursor;
            use crate::trace::{BatchReader, Description};
            use crate::trace::chunk::ChunkBatch;
            use timely::progress::Antichain;

            let chunks = vec![
                chunk(vec![((0, 0), 0, 1), ((1, 0), 0, 1), ((1, 1), 0, 1)]),
                chunk(vec![((1, 1), 1, 1), ((1, 2), 0, 1)]),
                chunk(vec![((2, 0), 0, 1)]),
            ];
            let desc = Description::new(
                Antichain::from_elem(0u64),
                Antichain::from_elem(2u64),
                Antichain::from_elem(0u64),
            );
            let batch = ChunkBatch::new(chunks, desc);

            let mut cursor = batch.cursor();
            let got = cursor.to_vec(&batch, |k| *k, |v| *v);
            let want = vec![
                ((0u64, 0u64), vec![(0u64, 1i64)]),
                ((1, 0), vec![(0, 1)]),
                ((1, 1), vec![(0, 1), (1, 1)]),
                ((1, 2), vec![(0, 1)]),
                ((2, 0), vec![(0, 1)]),
            ];
            assert_eq!(got, want);
        }

        // Isolated: gallop vs linear forward-seek over one big chunk, for sparse to
        // dense probe sets. Run: cargo test seek_microbench -- --ignored --nocapture
        #[test]
        #[ignore]
        fn seek_microbench() {
            use std::time::Instant;
            use std::hint::black_box;
            use super::gallop;
            let n = 1_000_000u64;
            let data: Vec<((u64, ()), u64, isize)> = (0..n).map(|k| ((3 * k, ()), 0u64, 1isize)).collect();
            for probes in [100u64, 10_000, 1_000_000] {
                let targets: Vec<u64> = (0..probes).map(|i| 3 * (i * n / probes)).collect();
                let best = |f: &dyn Fn() -> u64| {
                    let mut b = std::time::Duration::MAX;
                    for _ in 0..5 { let t = Instant::now(); black_box(f()); b = b.min(t.elapsed()); }
                    b
                };
                let data = black_box(&data[..]);
                let g = best(&|| {
                    let (mut pos, mut acc) = (0usize, 0u64);
                    for &tgt in &targets { pos = gallop(data, pos, |u| u.0.0 < tgt); acc += pos as u64; }
                    acc
                });
                let l = best(&|| {
                    let (mut pos, mut acc) = (0usize, 0u64);
                    for &tgt in &targets { while pos < data.len() && data[pos].0.0 < tgt { pos += 1; } acc += pos as u64; }
                    acc
                });
                eprintln!("probes={probes:>7}: gallop={g:>12?}  linear={l:>12?}");
            }
        }
    }
}

pub mod col_chunk {
    //! A columnar [`Chunk`] mirroring the `ord_neu` layout: separate key / val /
    //! time / diff columns linked by offset lists, with the singleton update
    //! optimization (an empty `[offs[i], offs[i+1])` range reuses the prior single
    //! `(time, diff)`).
    //!
    //! Where [`vec_chunk`](super::vec_chunk) stores a flat `Vec<((K,V),T,R)>` —
    //! repeating each key on every update — this stores each distinct key once, each
    //! `(key, val)` once, and shares a single `(time, diff)` across vals when they
    //! coincide. It reuses the very containers `ord_neu` is built from
    //! ([`Vals`], [`Upds`], [`UpdsBuilder`], [`OffsetList`](crate::trace::implementations::OffsetList)),
    //! so the layout and the singleton encoding are shared code, not a re-derivation.
    //!
    //! The chunk-producing operations still emit *graded* runs (cut to
    //! [`Chunk::TARGET`]) rather than one monolithic batch, preserving proportional
    //! merging.
    //!
    //! # Status (v1)
    //!
    //! The full [`Chunk`] trait is implemented and tested (merge / extract / advance /
    //! regrade / prune / cursor), so `ColChunk` works as a [`ChunkBatch`] backing — a
    //! spine and reference-counted builder are aliased below. Known limitations, all
    //! noted at their sites and worth review:
    //!
    //! * **Decompress/recompress.** `merge`/`extract`/`advance` read inputs into an
    //!   owned `(key, val, time, diff)` stream and rebuild via [`Builder`]. This is
    //!   proportional and graded, but does not yet exploit the columnar layout to copy
    //!   runs of distinct keys by range (as `ord_neu`'s merger does); it also
    //!   materializes owned keys/vals during a merge.
    //! * **`consumed` ↔ singleton.** The merge `consumed` prefix is a *logical* update
    //!   count, which under the singleton encoding does not map `O(1)` to a column
    //!   position, so a retained head re-walks its prefix each call.
    //! * **Cut granularity.** Chunks are cut only at val boundaries, so a single
    //!   `(key, val)` exceeding `TARGET` yields one over-sized chunk; mid-val splitting
    //!   is unimplemented.
    //! * **Merge-batcher integration.** Wired via [`ColChunkBatcher`] / [`ColChunker`]:
    //!   rather than make the consolidated trie double as the merge batcher's unsorted
    //!   accumulation buffer (which `vec_chunk` can, being flat, but a trie can't), the
    //!   chunker keeps a `Vec` scratch and *builds* graded chunks from each sorted run.
    //!   The `Chunk` trait stays minimal — the accumulation traits live on the scratch,
    //!   not the chunk. `ColChunk` arranges end-to-end (`Collection → MergeBatcher →
    //!   Batch → Merger`); see the `chunks` example's `colchunk` mode.

    use std::marker::PhantomData;
    use std::rc::Rc;

    use std::collections::VecDeque;

    use timely::Accountable;
    use timely::container::{ContainerBuilder, PushInto, SizableContainer};
    use timely::progress::Antichain;

    use crate::consolidation::Consolidate;
    use crate::difference::{IsZero, Semigroup};
    use crate::lattice::Lattice;
    use crate::trace::cursor::Cursor;
    use crate::trace::implementations::{BatchContainer, Layout, WithLayout};
    use crate::trace::implementations::layout::Time;
    use crate::trace::implementations::chunker::ContainerChunker;
    use crate::trace::implementations::merge_batcher::MergeBatcher;
    use crate::trace::implementations::ord_neu::layers::{UpdsBuilder, Upds, Vals};
    use crate::trace::implementations::ord_neu::val_batch::OrdValStorage;

    use super::{Chunk, ChunkFeed, ChunkList, ChunkMerger};

    /// The chunk size: maximum updates per chunk and the coalescing threshold.
    const TARGET: usize = 1024;

    /// Columnar storage plus the *logical* update count, which exceeds the number
    /// of stored `(time, diff)` pairs by the number of singleton reuses.
    struct Inner<L: Layout> {
        storage: OrdValStorage<L>,
        updates: usize,
    }

    /// A sorted, consolidated columnar run of `((key, val), time, diff)`, shared via `Rc`.
    pub struct ColChunk<L: Layout>(Rc<Inner<L>>);

    impl<L: Layout> Clone for ColChunk<L> {
        fn clone(&self) -> Self { ColChunk(Rc::clone(&self.0)) }
    }

    impl<L: Layout> Default for ColChunk<L> {
        fn default() -> Self {
            ColChunk(Rc::new(Inner {
                storage: OrdValStorage {
                    keys: <L::KeyContainer as BatchContainer>::with_capacity(0),
                    vals: Vals::default(),
                    upds: Upds::default(),
                },
                updates: 0,
            }))
        }
    }

    impl<L: Layout> WithLayout for ColChunk<L> {
        type Layout = L;
    }

    // --- cursor: mirrors `ord_neu`'s `OrdValCursor`, over `ColChunk` storage ---

    /// A cursor over a [`ColChunk`], tracking absolute key and val positions.
    pub struct ColChunkCursor<L: Layout> {
        key_cursor: usize,
        val_cursor: usize,
        phantom: PhantomData<L>,
    }

    impl<L: Layout> WithLayout for ColChunkCursor<L> {
        type Layout = L;
    }

    impl<L: Layout> Cursor for ColChunkCursor<L> {
        type Storage = ColChunk<L>;

        fn get_key<'a>(&self, s: &'a Self::Storage) -> Option<Self::Key<'a>> { s.0.storage.keys.get(self.key_cursor) }
        fn get_val<'a>(&self, s: &'a Self::Storage) -> Option<Self::Val<'a>> { if self.val_valid(s) { Some(self.val(s)) } else { None } }

        fn key<'a>(&self, s: &'a Self::Storage) -> Self::Key<'a> { s.0.storage.keys.index(self.key_cursor) }
        fn val<'a>(&self, s: &'a Self::Storage) -> Self::Val<'a> { s.0.storage.vals.get_abs(self.val_cursor) }

        fn map_times<L2: FnMut(Self::TimeGat<'_>, Self::DiffGat<'_>)>(&mut self, s: &Self::Storage, mut logic: L2) {
            let (lower, upper) = s.0.storage.upds.bounds(self.val_cursor);
            for index in lower .. upper {
                let (time, diff) = s.0.storage.upds.get_abs(index);
                logic(time, diff);
            }
        }

        fn key_valid(&self, s: &Self::Storage) -> bool { self.key_cursor < s.0.storage.keys.len() }
        fn val_valid(&self, s: &Self::Storage) -> bool { self.val_cursor < s.0.storage.vals.bounds(self.key_cursor).1 }

        fn step_key(&mut self, s: &Self::Storage) {
            self.key_cursor += 1;
            if self.key_valid(s) { self.rewind_vals(s); }
            else { self.key_cursor = s.0.storage.keys.len(); }
        }
        fn seek_key(&mut self, s: &Self::Storage, key: Self::Key<'_>) {
            self.key_cursor += s.0.storage.keys.advance(self.key_cursor, s.0.storage.keys.len(), |x| {
                <L::KeyContainer as BatchContainer>::reborrow(x).lt(&<L::KeyContainer as BatchContainer>::reborrow(key))
            });
            if self.key_valid(s) { self.rewind_vals(s); }
        }
        fn step_val(&mut self, s: &Self::Storage) {
            self.val_cursor += 1;
            if !self.val_valid(s) { self.val_cursor = s.0.storage.vals.bounds(self.key_cursor).1; }
        }
        fn seek_val(&mut self, s: &Self::Storage, val: Self::Val<'_>) {
            self.val_cursor += s.0.storage.vals.vals.advance(self.val_cursor, s.0.storage.vals.bounds(self.key_cursor).1, |x| {
                <L::ValContainer as BatchContainer>::reborrow(x).lt(&<L::ValContainer as BatchContainer>::reborrow(val))
            });
        }
        fn rewind_keys(&mut self, s: &Self::Storage) {
            self.key_cursor = 0;
            if self.key_valid(s) { self.rewind_vals(s); }
        }
        fn rewind_vals(&mut self, s: &Self::Storage) {
            self.val_cursor = s.0.storage.vals.bounds(self.key_cursor).0;
        }
    }

    // --- trace side: the `Chunk` operations ---

    impl<L: Layout> Chunk for ColChunk<L>
    where
        // Implied by `Layout` but not elaborated at direct call sites; state them so
        // `advance` can advance times and consolidate diffs.
        <L::TimeContainer as BatchContainer>::Owned: Lattice,
        <L::DiffContainer as BatchContainer>::Owned: Semigroup,
    {
        type Cursor = ColChunkCursor<L>;

        const TARGET: usize = TARGET;

        fn cursor(&self) -> Self::Cursor {
            ColChunkCursor { key_cursor: 0, val_cursor: 0, phantom: PhantomData }
        }

        fn bounds(&self) -> (
            (Self::Key<'_>, Self::Val<'_>, Self::TimeGat<'_>),
            (Self::Key<'_>, Self::Val<'_>, Self::TimeGat<'_>),
        ) {
            let s = &self.0.storage;
            let nk = s.keys.len();
            let nv = s.vals.len();
            // First (key, val, time): first key, its first val, that val's first time.
            // The first val is never a singleton (the encoding needs a prior update).
            let fk = s.keys.index(0);
            let fv = s.vals.get_abs(0);
            let ft = s.upds.times.index(s.upds.bounds(0).0);
            // Last (key, val, time): last key, last val, that val's last time.
            let lk = s.keys.index(nk - 1);
            let lv = s.vals.get_abs(nv - 1);
            let lt = s.upds.times.index(s.upds.bounds(nv - 1).1 - 1);
            ((fk, fv, ft), (lk, lv, lt))
        }

        fn len(&self) -> usize { self.0.updates }

        fn prune(self, prefix: usize) -> Self {
            // The suffix `[prefix..]` is already sorted + consolidated; rebuild it as
            // a single chunk (never cutting), mirroring `vec_chunk::prune`.
            let mut updates: Vec<OwnedUpdate<L>> = Vec::new();
            collect_in_range(&self.0.storage, prefix, None, &mut updates);
            build_single(updates)
        }

        fn merge(chunks: &mut [(usize, Self)], out: &mut ChunkList<Self>) {
            // Horizon: least last `(key, val, time)` across in-range inputs; nothing
            // strictly below it can interleave with as-yet-unmerged updates.
            let horizon = chunks.iter()
                .filter(|(consumed, ch)| *consumed < ch.len())
                .map(|(_, ch)| {
                    let (_, (lk, lv, lt)) = ch.bounds();
                    (
                        <L::KeyContainer as BatchContainer>::into_owned(lk),
                        <L::ValContainer as BatchContainer>::into_owned(lv),
                        <L::TimeContainer as BatchContainer>::into_owned(lt),
                    )
                })
                .min();
            let Some(horizon) = horizon else { return; };

            // Decompress each input's in-range prefix, advancing its consumed count.
            // The input achieving the horizon drains fully (all its updates are
            // `<= horizon`), satisfying the merge contract.
            let mut collected: Vec<OwnedUpdate<L>> = Vec::new();
            for (consumed, ch) in chunks.iter_mut() {
                let added = collect_in_range(&ch.0.storage, *consumed, Some(&horizon), &mut collected);
                *consumed += added;
            }
            // Sort + sum equal `(key, val, time)` across inputs, dropping cancellations.
            crate::consolidation::consolidate(&mut collected);
            // Re-pack into graded chunks (the builder re-applies the singleton opt).
            let mut builder = Builder::new();
            for ((k, v, t), d) in collected { builder.push(k, v, t, d); }
            for c in builder.finish() { out.push(c); }
        }

        fn extract(
            chunks: &mut Vec<Self>,
            frontier: &Antichain<Time<L>>,
            residual: &mut Antichain<Time<L>>,
            keep: &mut ChunkList<Self>,
            ship: &mut ChunkList<Self>,
        ) {
            // Route each update to `keep` (time `>=` frontier) or `ship` (otherwise),
            // folding kept times into `residual`. Iterating sorted input and appending
            // in order keeps each side sorted, so the builders emit graded chunks.
            let mut keep_b = Builder::new();
            let mut ship_b = Builder::new();
            for chunk in chunks.drain(..) {
                let mut updates: Vec<OwnedUpdate<L>> = Vec::new();
                collect_in_range(&chunk.0.storage, 0, None, &mut updates);
                for ((k, v, t), d) in updates {
                    if frontier.borrow().less_equal(&t) {
                        residual.insert_ref(&t);
                        keep_b.push(k, v, t, d);
                    } else {
                        ship_b.push(k, v, t, d);
                    }
                }
            }
            for c in keep_b.finish() { keep.push(c); }
            for c in ship_b.finish() { ship.push(c); }
        }

        fn advance(
            feed: &mut ChunkFeed<Self>,
            frontier: &Antichain<Time<L>>,
            done: bool,
            out: &mut ChunkList<Self>,
        ) {
            // Decompress the withheld head (past its consumed prefix) and all freshly
            // pushed tail chunks into one sorted owned buffer. The feed is a single
            // sorted run, so the head precedes the tail.
            let (consumed, head) = &mut feed.0;
            let mut buf: Vec<OwnedUpdate<L>> = Vec::new();
            collect_in_range(&head.0.storage, *consumed, None, &mut buf);
            *consumed = 0;
            *head = ColChunk::default();
            for chunk in feed.1.drain(..) {
                collect_in_range(&chunk.0.storage, 0, None, &mut buf);
            }
            if buf.is_empty() { return; }

            // The `(key, val)` of an update.
            let kv = |u: &OwnedUpdate<L>| (u.0.0.clone(), u.0.1.clone());

            // If every update shares one `(key, val)` and we are not done, no group is
            // provably complete (a later push may extend it); withhold everything as
            // the head for the next call. (v1: re-packs the head each call, so a key
            // spanning many pushes is quadratic — noted for review.)
            if !done && kv(&buf[0]) == kv(&buf[buf.len() - 1]) {
                *head = build_single(buf);
                return;
            }

            // Otherwise withhold the trailing (single `(key, val)`) group unless done.
            let end = if done { buf.len() } else {
                let last = kv(&buf[buf.len() - 1]);
                let mut start = buf.len();
                while start > 0 && kv(&buf[start - 1]) == last { start -= 1; }
                start
            };
            if end < buf.len() {
                let tail = buf.split_off(end);
                *head = build_single(tail);
            }

            // Advance + consolidate each complete group into graded output chunks.
            let mut builder = Builder::new();
            let mut i = 0;
            while i < buf.len() {
                let mut j = i;
                let group = kv(&buf[i]);
                while j < buf.len() && kv(&buf[j]) == group { j += 1; }
                for u in &mut buf[i..j] { u.0.2.advance_by(frontier.borrow()); }
                // Advancing is monotone w.r.t. the lattice but not the total order;
                // re-sort the group by time, then consolidate equal times.
                buf[i..j].sort_by(|a, b| a.0.2.cmp(&b.0.2));
                let mut k = i;
                while k < j {
                    let (kk, vv, t) = (buf[k].0.0.clone(), buf[k].0.1.clone(), buf[k].0.2.clone());
                    let mut diff = buf[k].1.clone();
                    k += 1;
                    while k < j && buf[k].0.2 == t { diff.plus_equals(&buf[k].1); k += 1; }
                    if !diff.is_zero() {
                        builder.push(kk, vv, t, diff);
                    }
                }
                i = j;
            }
            for c in builder.finish() { out.push(c); }
        }

        fn regrade(queue: &mut Vec<Self>, done: bool, out: &mut Vec<Self>) {
            // Maximal packing over columnar chunks. A chunk already at least `TARGET`
            // is passed through by `Rc` move; smaller chunks are decompressed into a
            // `Builder` that re-emits `TARGET`-sized runs (and re-consolidates /
            // re-applies the singleton optimization). The builder's trailing partial
            // is carried back onto `queue` between calls, or emitted on `done`.
            //
            // (v1 note: a chunk *larger* than `TARGET` — only producible by a single
            // `(key, val)` whose updates exceed `TARGET` — is passed through rather
            // than split, since columnar mid-val splitting is not yet implemented.)
            let mut builder = Builder::new();
            for chunk in queue.drain(..) {
                if !builder.has_pending() && chunk.len() >= TARGET {
                    out.push(chunk);
                } else if chunk.len() >= TARGET {
                    // Flush the carried partial before the larger chunk (order!).
                    if let Some(c) = builder.finish_pending() { out.push(c); }
                    out.push(chunk);
                } else {
                    feed_chunk(&mut builder, &chunk);
                    out.append(&mut builder.drain_done());
                }
            }
            out.append(&mut builder.drain_done());
            if let Some(c) = builder.finish_pending() {
                if done { out.push(c); } else { queue.push(c); }
            }
        }
    }

    /// Owned key / val / time / diff for layout `L`.
    type KOwned<L> = <<L as Layout>::KeyContainer as BatchContainer>::Owned;
    type VOwned<L> = <<L as Layout>::ValContainer as BatchContainer>::Owned;
    type TOwned<L> = <<L as Layout>::TimeContainer as BatchContainer>::Owned;
    type DOwned<L> = <<L as Layout>::DiffContainer as BatchContainer>::Owned;
    /// An owned update grouped for consolidation: `((key, val, time), diff)`.
    type OwnedUpdate<L> = ((KOwned<L>, VOwned<L>, TOwned<L>), DOwned<L>);

    /// Push every update of `chunk` into `builder`, in order. Decompresses the
    /// columnar layout to an owned `(key, val, time, diff)` stream; used by `regrade`
    /// to re-pack small chunks.
    fn feed_chunk<L: Layout>(builder: &mut Builder<L>, chunk: &ColChunk<L>) {
        let mut updates: Vec<OwnedUpdate<L>> = Vec::new();
        collect_in_range(&chunk.0.storage, 0, None, &mut updates);
        for ((k, v, t), d) in updates { builder.push(k, v, t, d); }
    }

    /// Append owned updates of `s`, in sorted `(key, val, time)` order, starting at
    /// logical index `skip`, while `(key, val, time) <= horizon` (when `Some`).
    /// Returns the number appended. Used by the producing ops to decompress the
    /// in-range portion of a chunk for re-merging.
    ///
    /// `skip` is walked linearly; under the singleton encoding a logical update index
    /// does not map O(1) to a column position, so a retained head re-walks its
    /// consumed prefix each call (a perf wrinkle worth revisiting — see notes).
    fn collect_in_range<L: Layout>(
        s: &OrdValStorage<L>,
        skip: usize,
        horizon: Option<&(KOwned<L>, VOwned<L>, TOwned<L>)>,
        out: &mut Vec<OwnedUpdate<L>>,
    ) -> usize {
        let mut seen = 0;
        let mut count = 0;
        for ki in 0..s.keys.len() {
            let key = s.keys.index(ki);
            let (vlo, vhi) = s.vals.bounds(ki);
            for vi in vlo..vhi {
                let val = s.vals.get_abs(vi);
                let (ulo, uhi) = s.upds.bounds(vi);
                for ui in ulo..uhi {
                    if seen < skip { seen += 1; continue; }
                    let (t, d) = s.upds.get_abs(ui);
                    let triple = (
                        <L::KeyContainer as BatchContainer>::into_owned(key),
                        <L::ValContainer as BatchContainer>::into_owned(val),
                        <L::TimeContainer as BatchContainer>::into_owned(t),
                    );
                    if let Some(h) = horizon {
                        if &triple > h { return count; }
                    }
                    out.push((triple, <L::DiffContainer as BatchContainer>::into_owned(d)));
                    count += 1;
                    seen += 1;
                }
            }
        }
        count
    }

    /// An empty columnar storage (offset lists seeded with their leading `0`).
    fn empty_storage<L: Layout>() -> OrdValStorage<L> {
        OrdValStorage {
            keys: <L::KeyContainer as BatchContainer>::with_capacity(0),
            vals: Vals::default(),
            upds: Upds::default(),
        }
    }

    /// Build a single (un-cut) chunk from a sorted, consolidated owned update stream.
    /// Used to repackage withheld/pruned suffixes; returns an empty chunk if there
    /// are no updates.
    fn build_single<L: Layout>(updates: Vec<OwnedUpdate<L>>) -> ColChunk<L> {
        let mut builder = Builder::with_target(usize::MAX);
        for ((k, v, t), d) in updates { builder.push(k, v, t, d); }
        builder.finish().pop().unwrap_or_default()
    }

    /// Builds graded [`ColChunk`]s from a sorted, consolidated `(key, val, time, diff)`
    /// stream, reusing `ord_neu`'s columnar machinery (and its singleton optimization
    /// via [`UpdsBuilder::seal`]). Cuts a chunk once its logical update count reaches
    /// `TARGET`, at a val boundary, so output arrives graded rather than monolithic.
    ///
    /// Completed chunks accumulate in `done`; the in-progress chunk lives in
    /// `result`/`staging`. (v1: cuts only at val boundaries, so a single `(key, val)`
    /// with more than `target` updates yields one over-sized chunk.)
    struct Builder<L: Layout> {
        result: OrdValStorage<L>,
        staging: UpdsBuilder<L::TimeContainer, L::DiffContainer>,
        done: Vec<ColChunk<L>>,
        /// Cut the in-progress chunk once its update count reaches this.
        target: usize,
        /// Last-pushed key / val, for boundary detection without re-reading columns.
        cur_key: Option<KOwned<L>>,
        cur_val: Option<VOwned<L>>,
    }

    impl<L: Layout> Builder<L> {
        /// A builder cutting at `TARGET`.
        fn new() -> Self { Self::with_target(TARGET) }
        /// A builder cutting once the in-progress chunk reaches `target` updates;
        /// `usize::MAX` never cuts (one chunk).
        fn with_target(target: usize) -> Self {
            Self {
                result: empty_storage(),
                staging: UpdsBuilder::default(),
                done: Vec::new(),
                target,
                cur_key: None,
                cur_val: None,
            }
        }

        /// Whether the in-progress chunk holds any updates.
        fn has_pending(&self) -> bool { !self.result.keys.is_empty() }

        /// Take the completed chunks accumulated so far.
        fn drain_done(&mut self) -> Vec<ColChunk<L>> { std::mem::take(&mut self.done) }

        /// Push one update; assumes pushes arrive in sorted `(key, val, time)` order.
        fn push(&mut self, key: KOwned<L>, val: VOwned<L>, time: TOwned<L>, diff: DOwned<L>) {
            let same_key = self.cur_key.as_ref() == Some(&key);
            let same_val = same_key && self.cur_val.as_ref() == Some(&val);
            if same_val {
                self.staging.push(time, diff);
                return;
            }
            // Crossing a val boundary: seal the prior val, and maybe cut a chunk.
            if self.has_pending() {
                self.staging.seal(&mut self.result.upds);
                if self.staging.total() >= self.target {
                    self.cut();
                    self.open(key, val, time, diff);
                    return;
                }
            }
            if same_key {
                // New val under the same key.
                self.staging.push(time, diff);
                self.result.vals.vals.push_own(&val);
                self.cur_val = Some(val);
            } else {
                // New key (or the very first update).
                if self.has_pending() {
                    self.result.vals.offs.push_ref(self.result.vals.vals.len());
                }
                self.staging.push(time, diff);
                self.result.vals.vals.push_own(&val);
                self.result.keys.push_own(&key);
                self.cur_key = Some(key);
                self.cur_val = Some(val);
            }
        }

        /// Open a fresh chunk's first `(key, val)`; `result` must be empty.
        fn open(&mut self, key: KOwned<L>, val: VOwned<L>, time: TOwned<L>, diff: DOwned<L>) {
            self.result.vals.vals.push_own(&val);
            self.result.keys.push_own(&key);
            self.staging.push(time, diff);
            self.cur_key = Some(key);
            self.cur_val = Some(val);
        }

        /// Finalize the in-progress chunk into `done`. The prior val must already be
        /// sealed (as it is at the `push` cut point).
        fn cut(&mut self) {
            self.result.vals.offs.push_ref(self.result.vals.vals.len());
            let updates = self.staging.total();
            let storage = std::mem::replace(&mut self.result, empty_storage());
            self.staging = UpdsBuilder::default();
            self.cur_key = None;
            self.cur_val = None;
            self.done.push(ColChunk(Rc::new(Inner { storage, updates })));
        }

        /// Seal and finalize the in-progress chunk (if any), returning it.
        fn finish_pending(&mut self) -> Option<ColChunk<L>> {
            if !self.has_pending() { return None; }
            self.staging.seal(&mut self.result.upds);
            self.result.vals.offs.push_ref(self.result.vals.vals.len());
            let updates = self.staging.total();
            let storage = std::mem::replace(&mut self.result, empty_storage());
            self.staging = UpdsBuilder::default();
            self.cur_key = None;
            self.cur_val = None;
            Some(ColChunk(Rc::new(Inner { storage, updates })))
        }

        /// Finish the build, returning all chunks (completed plus the final partial).
        fn finish(mut self) -> Vec<ColChunk<L>> {
            if let Some(c) = self.finish_pending() { self.done.push(c); }
            self.done
        }
    }

    /// A spine of `Rc`-shared columnar [`ChunkBatch`]es: the `arrange` trace type.
    pub type ColChunkSpine<L> = super::ChunkSpine<ColChunk<L>>;
    /// A reference-counted builder of columnar [`ChunkBatch`]es.
    pub type ColChunkRcBuilder<L> = super::ChunkRcBuilder<ColChunk<L>>;

    // --- batcher side: a `Vec`-scratch chunker that *builds* columnar chunks ---
    //
    // A consolidated columnar trie can't serve as the merge batcher's unsorted
    // accumulation buffer, and the `Chunk` trait deliberately does not require it to.
    // Instead this "build-from-input" chunker keeps a flat `Vec` scratch — which the
    // stock `ContainerChunker` accumulates and consolidates — and transcodes each
    // sorted run into graded `ColChunk`s via [`Builder`]. The `Vec` scratch carries
    // the `SizableContainer`/`Consolidate`/`PushInto` burden; the chunk stays a
    // minimal, consolidated `Chunk`. (This is the seam: `Input → Chunk` happens here,
    // and only here.)

    /// Updates as the dataflow delivers them, before consolidation.
    type ColInput<L> = Vec<((KOwned<L>, VOwned<L>), TOwned<L>, DOwned<L>)>;

    /// A [`ContainerBuilder`] that consolidates input in a `Vec` scratch and emits
    /// graded [`ColChunk`]s built via [`Builder`].
    pub struct ColChunker<L: Layout> {
        scratch: ContainerChunker<ColInput<L>>,
        ready: VecDeque<ColChunk<L>>,
        output: ColChunk<L>,
    }

    impl<L: Layout> Default for ColChunker<L> {
        fn default() -> Self {
            Self { scratch: ContainerChunker::default(), ready: VecDeque::new(), output: ColChunk::default() }
        }
    }

    impl<L: Layout> ColChunker<L> {
        /// Transcode one consolidated, sorted `Vec` run into graded `ColChunk`s.
        fn transcode(&mut self, mut run: ColInput<L>) {
            let mut builder = Builder::new();
            for ((k, v), t, d) in run.drain(..) { builder.push(k, v, t, d); }
            self.ready.extend(builder.finish());
        }
        fn pop(&mut self) -> Option<&mut ColChunk<L>> {
            if let Some(c) = self.ready.pop_front() {
                self.output = c;
                Some(&mut self.output)
            } else {
                None
            }
        }
    }

    impl<L: Layout> ContainerBuilder for ColChunker<L>
    where ColInput<L>: SizableContainer + Consolidate {
        type Container = ColChunk<L>;
        fn extract(&mut self) -> Option<&mut ColChunk<L>> {
            if self.ready.is_empty() {
                if let Some(run) = self.scratch.extract() {
                    let run = std::mem::take(run);
                    self.transcode(run);
                }
            }
            self.pop()
        }
        fn finish(&mut self) -> Option<&mut ColChunk<L>> {
            if self.ready.is_empty() {
                if let Some(run) = self.scratch.finish() {
                    let run = std::mem::take(run);
                    self.transcode(run);
                }
            }
            self.pop()
        }
    }

    impl<'a, L: Layout, Input> PushInto<&'a mut Input> for ColChunker<L>
    where ContainerChunker<ColInput<L>>: PushInto<&'a mut Input> {
        fn push_into(&mut self, input: &'a mut Input) { self.scratch.push_into(input); }
    }

    impl<L: Layout> Accountable for ColChunk<L> {
        fn record_count(&self) -> i64 { self.0.updates as i64 }
    }

    /// Merge batcher for columnar chunks: merges `ColChunk<L>` chains via [`ChunkMerger`].
    /// The `Input → ColChunk` bridge is [`ColChunker`], now supplied at the `arrange_core`
    /// callsite (it consolidates `Input` in a `Vec` scratch and transcodes to `ColChunk`).
    pub type ColChunkBatcher<L> = MergeBatcher<ChunkMerger<ColChunk<L>>>;

    #[cfg(test)]
    mod test {
        use super::{Builder, ColChunk};
        use crate::trace::cursor::Cursor;
        use crate::trace::{BatchReader, Description};
        use crate::trace::chunk::{is_graded, ChunkBatch, ChunkList};
        use crate::trace::implementations::Vector;
        use timely::progress::Antichain;

        type CC = ColChunk<Vector<((u64, u64), u64, i64)>>;

        /// Build graded columnar chunks from a sorted, consolidated update stream.
        fn build(updates: Vec<((u64, u64), u64, i64)>) -> Vec<CC> {
            let mut b = Builder::new();
            for ((k, v), t, d) in &updates { b.push(*k, *v, *t, *d); }
            b.finish()
        }

        /// Flatten chunks back to a `(key, val, time, diff)` stream.
        fn dump(chunks: &[CC]) -> Vec<((u64, u64), u64, i64)> {
            let mut out = Vec::new();
            for c in chunks {
                let mut v = Vec::new();
                super::collect_in_range(&c.0.storage, 0, None, &mut v);
                for ((k, vv, t), d) in v { out.push(((k, vv), t, d)); }
            }
            out
        }

        // A columnar chunk built from a sorted stream must read back, through the
        // batch cursor, exactly the grouped updates — including a key and a
        // `(key, val)` that straddle chunk boundaries.
        #[test]
        fn cursor_round_trips() {
            let updates = vec![
                ((0u64, 0u64), 0u64, 1i64),
                ((1, 0), 0, 1),
                ((1, 1), 0, 1),
                ((1, 1), 1, 1),
                ((1, 2), 0, 1),
                ((2, 0), 0, 1),
            ];
            let chunks = build(updates.clone());
            assert!(!chunks.is_empty());
            let desc = Description::new(
                Antichain::from_elem(0u64),
                Antichain::from_elem(2u64),
                Antichain::from_elem(0u64),
            );
            let batch = ChunkBatch::new(chunks, desc);
            let mut cursor = batch.cursor();
            let got = cursor.to_vec(&batch, |k| *k, |v| *v);
            let want = vec![
                ((0u64, 0u64), vec![(0u64, 1i64)]),
                ((1, 0), vec![(0, 1)]),
                ((1, 1), vec![(0, 1), (1, 1)]),
                ((1, 2), vec![(0, 1)]),
                ((2, 0), vec![(0, 1)]),
            ];
            assert_eq!(got, want);
        }

        // Building a run larger than `TARGET` yields multiple graded chunks (cut at
        // val boundaries), and the singleton optimization keeps storage compact.
        #[test]
        fn build_cuts_and_grades() {
            use super::TARGET;
            // Distinct keys, each a single val with one update at the same (time,diff)
            // — the singleton case. 3·TARGET keys ⇒ several chunks.
            let n = 3 * TARGET as u64;
            let updates: Vec<_> = (0..n).map(|k| ((k, 0u64), 0u64, 1i64)).collect();
            let chunks = build(updates);
            assert!(chunks.len() >= 3, "expected several chunks, got {}", chunks.len());
            let total: usize = chunks.iter().map(|c| c.0.updates).sum();
            assert_eq!(total, n as usize);
            // Every chunk but the last is at least TARGET (cut on reaching it).
            for c in &chunks[..chunks.len() - 1] {
                assert!(c.0.updates >= TARGET);
            }
        }

        // `merge_chains` over columnar chunks must consolidate equal `(k,v,t)` across
        // chains and reproduce the sorted, summed contents.
        #[test]
        fn merge_chains_consolidates() {
            use crate::trace::chunk::merge_chains;
            let a = build(vec![((0, 0), 0, 1), ((1, 0), 0, 1)]);
            let b = build(vec![((0, 0), 0, 1), ((2, 0), 0, 1)]);
            let mut out = ChunkList::default();
            merge_chains(a, b, &mut out);
            assert_eq!(dump(&out.done()), vec![((0, 0), 0, 2), ((1, 0), 0, 1), ((2, 0), 0, 1)]);
        }

        // Merging two large interleaving chains yields a graded sequence with exactly
        // the merged contents.
        #[test]
        fn merge_chains_grades() {
            use super::TARGET;
            use crate::trace::chunk::{is_graded, merge_chains};
            let n = 4 * TARGET as u64;
            let evens = build((0..n).step_by(2).map(|k| ((k, 0), 0, 1)).collect());
            let odds = build((0..n).step_by(2).map(|k| ((k + 1, 0), 0, 1)).collect());
            let mut out = ChunkList::default();
            merge_chains(evens, odds, &mut out);
            let chunks = out.done();
            assert!(is_graded(&chunks), "ungraded: {:?}",
                chunks.iter().map(super::super::Chunk::len).collect::<Vec<_>>());
            let want: Vec<_> = (0..n).map(|k| ((k, 0u64), 0u64, 1i64)).collect();
            assert_eq!(dump(&chunks), want);
        }

        // `regrade` (via `ChunkList`) coalesces small adjacent chunks while
        // preserving sorted contents.
        #[test]
        fn regrade_coalesces() {
            // Many tiny single-update chunks with distinct increasing keys.
            let n = 5 * super::TARGET as u64;
            let mut list = ChunkList::default();
            for k in 0..n {
                let mut c = build(vec![((k, 0u64), 0u64, 1i64)]);
                assert_eq!(c.len(), 1);
                list.push(c.pop().unwrap());
            }
            let chunks = list.done();
            // Coalesced to roughly n/TARGET chunks, all sorted and accounted for.
            let total: usize = chunks.iter().map(super::super::Chunk::len).sum();
            assert_eq!(total, n as usize);
            assert!(is_graded(&chunks), "ungraded: {:?}",
                chunks.iter().map(super::super::Chunk::len).collect::<Vec<_>>());
        }

        // `extract` partitions by frontier (kept `>=`, shipped `<`), folds the kept
        // frontier into `residual`, and emits graded chunks on both sides.
        #[test]
        fn extract_partitions() {
            use super::super::Chunk;
            let n = 4 * super::TARGET as u64;
            // Distinct keys; even keys at time 0 (ship), odd keys at time 1 (keep).
            let mut chunks = build((0..n).map(|i| ((i, 0), i % 2, 1)).collect());
            let frontier = Antichain::from_elem(1u64);
            let mut residual = Antichain::new();
            let (mut keep, mut ship) = (ChunkList::default(), ChunkList::default());
            CC::extract(&mut chunks, &frontier, &mut residual, &mut keep, &mut ship);
            let (keep, ship) = (keep.done(), ship.done());
            assert_eq!(residual, Antichain::from_elem(1u64));
            assert!(is_graded(&keep) && is_graded(&ship));
            let (kd, sd) = (dump(&keep), dump(&ship));
            assert_eq!(kd.len() + sd.len(), n as usize);
            assert!(kd.iter().all(|u| u.1 == 1));
            assert!(sd.iter().all(|u| u.1 == 0));
        }

        // Resumable `advance` (chunk-at-a-time) must match a single all-at-once flush,
        // with every time advanced to the frontier.
        #[test]
        fn advance_resumable_matches_oneshot() {
            use crate::trace::chunk::AdvanceQueue;
            let frontier = Antichain::from_elem(3u64);
            // One sorted run, split across three pushes; groups straddle the splits.
            let input = || vec![
                vec![((0u64, 0u64), 0u64, 1i64), ((0, 0), 1, 1), ((1, 0), 0, 1)],
                vec![((1, 0), 5, 1), ((1, 1), 0, 1), ((2, 0), 0, 1)],
                vec![((2, 0), 2, 1), ((2, 0), 9, 1)],
            ];
            let oneshot = {
                let mut q = AdvanceQueue::new(frontier.clone());
                let mut out = ChunkList::default();
                for g in input() { q.push(build(g), &mut out); }
                q.finish(&mut out);
                dump(&out.done())
            };
            let incremental = {
                let mut q = AdvanceQueue::new(frontier.clone());
                let mut out = ChunkList::default();
                for g in input() {
                    for c in build(g) { q.push(std::iter::once(c), &mut out); }
                }
                q.finish(&mut out);
                dump(&out.done())
            };
            assert_eq!(oneshot, incremental);
            for u in &oneshot { assert!(u.1 >= 3); }
        }

        // End-to-end: merge two columnar batches through the real `ChunkBatchMerger`
        // (which drives merge + advance), then read the result through the batch
        // cursor. Validates the whole columnar pipeline.
        #[test]
        fn batch_merge_end_to_end() {
            use crate::trace::cursor::Cursor;
            use crate::trace::{BatchReader, Description, Merger};
            use crate::trace::chunk::{ChunkBatch, ChunkBatchMerger};

            let desc = |lo: u64, up: u64| Description::new(
                Antichain::from_elem(lo),
                Antichain::from_elem(up),
                Antichain::from_elem(0u64),
            );
            let b1 = ChunkBatch::new(build(vec![((0, 0), 0, 1), ((1, 0), 1, 1)]), desc(0, 1));
            let b2 = ChunkBatch::new(build(vec![((0, 0), 1, 1), ((2, 0), 0, 1)]), desc(1, 2));

            let frontier = Antichain::from_elem(0u64); // since == minimum: no advance
            let mut m = ChunkBatchMerger::new(&b1, &b2, frontier.borrow());
            let mut fuel = isize::MAX;
            m.work(&b1, &b2, &mut fuel);
            let merged = m.done();

            let mut cursor = merged.cursor();
            let got = cursor.to_vec(&merged, |k| *k, |v| *v);
            let want = vec![
                ((0u64, 0u64), vec![(0u64, 1i64), (1, 1)]),
                ((1, 0), vec![(1, 1)]),
                ((2, 0), vec![(0, 1)]),
            ];
            assert_eq!(got, want);
        }
    }
}
