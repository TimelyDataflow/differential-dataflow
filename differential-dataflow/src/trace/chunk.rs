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

use std::collections::VecDeque;

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

/// Whether `chunks` satisfy the [`Chunk::TARGET`] grading invariant: every chunk
/// at most `TARGET`, and every adjacent pair summing to more than `TARGET` (so no
/// two neighbours could be combined into one legal chunk — a *maximal packing*).
///
/// This is the post-[`regrade`](Chunk::regrade) shape; useful as a test/debug check.
pub fn is_graded<C: Chunk>(chunks: &[C]) -> bool {
    chunks.iter().all(|c| c.len() <= C::TARGET)
        && chunks.windows(2).all(|w| w[0].len() + w[1].len() > C::TARGET)
}

/// Regrade `input` to completion into a fresh graded `Vec` (see [`Chunk::regrade`]).
///
/// A convenience for the one-shot callers (batch sealing, the batcher's merge and
/// extract) that have a whole sequence in hand and want it graded; the streaming
/// callers drive [`Chunk::regrade`] directly across ticks.
pub fn regrade_all<C: Chunk>(input: impl IntoIterator<Item = C>) -> Vec<C> {
    let mut input: VecDeque<C> = input.into_iter().collect();
    let mut out = VecDeque::new();
    C::regrade(&mut input, true, &mut out);
    debug_assert!(input.is_empty());
    out.into()
}

/// A consolidated, sorted sequence of `(data, time, diff)`.
///
/// Chunks exist in sequences, with no constraints on the breakpoints between
/// them. Each holds at most [`TARGET`](Chunk::TARGET) updates; a graded sequence
/// is a maximal packing at that size (see [`is_graded`] and the module docs).
///
/// `Clone` is expected to be cheap — a refcount bump on shared backing storage,
/// not a deep copy. The trace merger relies on this to read its (shared,
/// immutable) source batches by cloning chunks rather than consuming them.
///
/// A chunk *has* a [`Cursor`] over its own `(key, val, time, diff)` contents —
/// the chunk is its own cursor `Storage`, mirroring [`BatchReader`]. This is what
/// lets a batch cursor delegate downward: the batch indexes which chunk holds a
/// key (reusing the chunk's `KeyContainer` / `ValContainer` for boundaries) and
/// then reads through that chunk's cursor. We do not provide this; the opaque
/// chunk implementor does.
///
/// # The transducer protocol
///
/// The four chunk-producing operations ([`merge`](Chunk::merge),
/// [`extract`](Chunk::extract), [`advance`](Chunk::advance),
/// [`regrade`](Chunk::regrade)) are all *stream transducers* over `VecDeque<Self>`,
/// sharing one calling convention so an implementor learns it once:
///
/// * **Consume from the front.** Read chunks off the front of the input deque(s).
/// * **Withhold by pushing back.** Anything consumed but not yet safe to commit
///   (advance's still-growing last group; regrade's sub-`TARGET` carry; merge's
///   partially-consumed front) is reformed into a single owned chunk and
///   `push_front`ed back onto its input. The only cross-call state is therefore the
///   deques themselves — clean owned runs, no indices escape a call.
/// * **Commit by appending.** Append committed chunks to the output deque; once
///   appended they are written and a downstream stage may take them immediately.
/// * **`done` forces the flush.** The unary stages take `done: bool`; while it is
///   false they may withhold, and a call that appends nothing has yielded — the
///   harness will not call again until more input arrives or `done` flips true. On
///   `done` the stage must drain its withheld state (the harness keeps calling
///   until the output stops growing).
///
/// Two operations vary only where their job demands it: [`merge`](Chunk::merge) is
/// binary (and the harness, not `merge`, handles a drained input by flushing the
/// other side's verbatim tail, so `merge` needs no `done`); [`extract`](Chunk::extract)
/// is the one-shot splitter (it drains its whole input, so it needs no `done` and
/// has two outputs plus a residual frontier).
///
/// Implementors are further expected to:
///
/// * **Emit near-graded output.** Fill `TARGET`-sized output chunks directly rather
///   than emitting one monolithic chunk; the terminal [`regrade`](Chunk::regrade)
///   only has to coalesce the trailing partials at the seams. Grading is a
///   *seal-time* property, not an invariant maintained between stages.
/// * **Recycle where possible.** Reuse the storage of chunks drained from the input
///   as the buffers for output, so allocations balance input against output rather
///   than allocating afresh per emitted chunk. `vec_chunk` is the worked example: it
///   fills buffers reclaimed from a stash of emptied input `Vec`s, and advance reuses
///   its withheld carry's storage in place so a giant key stays linear, not quadratic.
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

    /// Merge the fronts of two input deques through their shared horizon.
    ///
    /// Both deques are non-empty (the caller guarantees it). The two front chunks
    /// merge through updates present in both — up to the least last `(key, val, time)`
    /// triple across them — consolidating collisions and emitting committed chunks to
    /// `out`. The side owning the horizon is fully consumed and `pop_front`ed; the
    /// other's partially-consumed front is reformed (its consumed prefix dropped) and
    /// `push_front`ed back. So on return at least one deque has had its front retired.
    ///
    /// `merge` makes one front-pair's worth of progress and returns; the harness
    /// re-ticks it, refilling a drained deque from its source, and itself handles an
    /// exhausted source by flushing the other deque's verbatim tail — so `merge` needs
    /// no `done` and never has to reason about end-of-input.
    fn merge(in1: &mut VecDeque<Self>, in2: &mut VecDeque<Self>, out: &mut VecDeque<Self>);

    /// Partition the input by `frontier` into updates greater-or-equal it (`keep`) or
    /// not (`ship`). One-shot: the whole of `input` is consumed.
    ///
    /// The lower envelope of the times routed to `keep` is folded into `residual`, so
    /// the caller learns the frontier of data it still holds without a second pass.
    /// Outputs are near-graded but not regraded; a terminal [`regrade`](Chunk::regrade)
    /// zips up the seams.
    fn extract(
        input: &mut VecDeque<Self>,
        frontier: &Antichain<Self::Time>,
        residual: &mut Antichain<Self::Time>,
        keep: &mut VecDeque<Self>,
        ship: &mut VecDeque<Self>,
    );

    /// Advance times by `frontier`, consolidating each complete `(key, val)` group from
    /// the front of `input` into `out`.
    ///
    /// A group is complete once a later `(key, val)` is seen, so every group but the
    /// last is emitted; the last (which a future call might extend) is reformed and
    /// `push_front`ed back as the withheld carry — unless `done`, which flushes it too.
    /// The degenerate case is a single `(key, val)` spanning all available input: no
    /// group is provably complete, so nothing is committed (the whole buffer is
    /// withheld) until `done`.
    fn advance(
        input: &mut VecDeque<Self>,
        frontier: &Antichain<Self::Time>,
        done: bool,
        out: &mut VecDeque<Self>,
    );

    /// Reshape the front of `input` into a maximal packing in `out`: each chunk at most
    /// [`TARGET`](Chunk::TARGET), and any two adjacent summing past `TARGET` (so no
    /// neighbours could be combined). See [`is_graded`].
    ///
    /// The terminal stage of every pipeline. A sub-`TARGET` carry that might still grow
    /// is `push_front`ed back as the withheld remainder until `done`, which flushes it.
    fn regrade(
        input: &mut VecDeque<Self>,
        done: bool,
        out: &mut VecDeque<Self>,
    );

}

/// Merge two full chains of chunks into one, to completion, appending to `out`.
///
/// The whole-chain (non-fueled) driver used by the batcher's
/// [`Merger`](crate::trace::implementations::merge_batcher::Merger): both chains are in
/// hand, so it ticks [`Chunk::merge`] until one deque empties, then appends the other's
/// remainder (the verbatim tail). Output is near-graded; callers regrade as needed.
pub fn merge_chains<C: Chunk>(
    chain1: Vec<C>,
    chain2: Vec<C>,
    out: &mut VecDeque<C>,
) {
    let mut in1: VecDeque<C> = chain1.into();
    let mut in2: VecDeque<C> = chain2.into();
    while !in1.is_empty() && !in2.is_empty() {
        C::merge(&mut in1, &mut in2, out);
    }
    // One deque is empty; the other's remainder is all greater than everything merged.
    out.extend(in1.drain(..));
    out.extend(in2.drain(..));
}

/// A merge-batcher [`Merger`](crate::trace::implementations::merge_batcher::Merger)
/// over chains of [`Chunk`]s.
///
/// `merge` runs the whole-chain binary merger; `extract` splits by the seal frontier
/// using [`Chunk::extract`]. The batcher consolidates equal `(data, time)` updates
/// but does *not* advance times — time advancement is advance's job, handled later in
/// the trace. Both regrade their output, since the batcher's chains want to be graded.
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
        let mut merged = VecDeque::new();
        merge_chains(list1, list2, &mut merged);
        // No regrade: the batcher's ladder weighs chains by updates (not chunk count)
        // since #767, so intermediate grading buys nothing; the final batch is graded
        // at seal. merge's output is already near-`TARGET`.
        output.extend(merged);
    }

    fn extract(
        &mut self,
        merged: Vec<C>,
        upper: AntichainRef<C::Time>,
        frontier: &mut Antichain<C::Time>,
        ship: &mut Vec<C>,
        kept: &mut Vec<C>,
        _stash: &mut Vec<C>,
    ) {
        // `extract` keeps updates greater-or-equal `upper` and ships the rest,
        // folding the lower envelope of kept times into `frontier`.
        let upper = upper.to_owned();
        let mut input: VecDeque<C> = merged.into();
        let (mut keep, mut shipped) = (VecDeque::new(), VecDeque::new());
        C::extract(&mut input, &upper, frontier, &mut keep, &mut shipped);
        // No regrade: `kept` is re-merged later and `shipped` is regraded at seal by
        // the builder, so neither needs grading here.
        kept.extend(keep);
        ship.extend(shipped);
    }

    fn len(chunk: &C) -> usize { chunk.len() }
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

/// A merge of two [`ChunkBatch`]es in progress.
///
/// This is the [`ChunkBatch`] merger, wired in as its
/// [`Batch::Merger`](crate::trace::Batch::Merger), and has that trait's
/// `new` / `work` / `done` shape.
///
/// The merge is *resumable* and runs a two-stage deque pipeline:
/// [`merge`](Chunk::merge) feeds `merged`, [`advance`](Chunk::advance) consumes it
/// into `advanced`; the terminal [`regrade`](Chunk::regrade) runs once at `done`. Each
/// `work` step clones a burst from each source, ticks `merge` once, then advances the
/// fresh output, debiting `fuel` by the *merged* records that entered the pipe — the
/// total output across the merge, matching how the trace's other mergers account (cf.
/// `ord_neu`). The sources are read by *cloning* chunks (a cheap refcount bump per the
/// [`Chunk`] contract), never consumed or mutated; the same `source1`/`source2` must be
/// supplied on every call. When a source exhausts, the harness flushes the other's
/// verbatim tail one chunk per step. Once both are drained, a final `advance(done)`
/// flushes advance's withheld carry.
///
/// **Latency bound.** `fuel` bounds each step to roughly one burst-merge's output. Two
/// things ride *outside* fuel: the terminal `advance(done)` and `done`'s `regrade`. In
/// the worst case — a single `(key, val)` spanning the whole merge — `advance` withholds
/// the entire group until `done`, then sorts and consolidates it in one unfueled step.
/// `vec_chunk` keeps that step *linear* in the group (it accumulates the carry in place,
/// reusing its storage), so it is not the quadratic blow-up of an earlier design, but it
/// is one unbounded-latency step bounded by the largest single `(key, val)` group. A
/// chunk impl must keep this flush linear; the latency claimed is "per step ≈ a burst,
/// plus a final flush ≤ the largest group."
pub struct ChunkBatchMerger<C: Chunk> {
    /// Compaction frontier supplied at construction.
    frontier: Antichain<C::Time>,
    /// Result frontiers, retained for the output description.
    lower: Antichain<C::Time>,
    upper: Antichain<C::Time>,
    /// Input deques, refilled from the sources (clones) head-of-list at a time.
    in1: VecDeque<C>,
    in2: VecDeque<C>,
    /// Next source chunk to clone into `in1` / `in2`.
    idx1: usize,
    idx2: usize,
    /// `advance`'s input: the merge output plus advance's withheld carry at the front.
    merged: VecDeque<C>,
    /// `advance`'s output: the merged-and-advanced chunks, grown by `work`.
    advanced: VecDeque<C>,
    /// Set once both sources are drained and advance's final flush has run.
    complete: bool,
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
            in1: VecDeque::new(),
            in2: VecDeque::new(),
            idx1: 0,
            idx2: 0,
            merged: VecDeque::new(),
            advanced: VecDeque::new(),
            complete: false,
        }
    }

    /// Advance the merge by up to `fuel` updates, suspending when it runs out.
    fn work(&mut self, source1: &ChunkBatch<C>, source2: &ChunkBatch<C>, fuel: &mut isize) {
        if self.complete { return; }

        while *fuel > 0 {
            // Refill each input deque up to a burst of source chunks (clones); `merge`
            // drains the loaded burst per call. The burst trades fuel granularity (a
            // call does up to a burst's work before checking fuel) against re-pruning:
            // a chunk that straddles many chunks on the other side is walked by index
            // within one call but, once its tail spills past the loaded burst, its
            // unconsumed suffix is pushed back and re-copied next call — a bigger burst
            // absorbs more straddle per call. This workload is insensitive (1..32 flat
            // to ~noise at 1M), so 8 is a conservative default, not a tuned optimum.
            // After this, a deque is non-empty iff its source still has data.
            const BURST: usize = 8;
            while self.in1.len() < BURST && self.idx1 < source1.chunks.len() {
                self.in1.push_back(source1.chunks[self.idx1].clone());
                self.idx1 += 1;
            }
            while self.in2.len() < BURST && self.idx2 < source2.chunks.len() {
                self.in2.push_back(source2.chunks[self.idx2].clone());
                self.idx2 += 1;
            }

            // Merge's per-tick output (a burst's worth, or one tail chunk), measured
            // for fuel before it joins the carry already in `merged`.
            let mut produced = VecDeque::new();
            if !self.in1.is_empty() && !self.in2.is_empty() {
                // Both sides have data: drain the loaded burst.
                C::merge(&mut self.in1, &mut self.in2, &mut produced);
            } else if let Some(chunk) = self.in1.pop_front().or_else(|| self.in2.pop_front()) {
                // Exactly one side has data: flush its verbatim tail, one chunk a step.
                produced.push_back(chunk);
            } else {
                // Both sources drained: final flush of advance's withheld carry.
                C::advance(&mut self.merged, &self.frontier, true, &mut self.advanced);
                self.complete = true;
                break;
            }

            let work: usize = produced.iter().map(C::len).sum();
            self.merged.extend(produced);
            C::advance(&mut self.merged, &self.frontier, false, &mut self.advanced);
            *fuel -= work as isize;
        }
    }

    /// Extract the merged batch over `[lower, upper)` advanced to the frontier.
    ///
    /// Only valid once `work` has driven the merge to completion (left `fuel`
    /// positive), as the [`trace::Merger`](crate::trace::Merger) contract requires.
    fn done(self) -> ChunkBatch<C> {
        let description = Description::new(self.lower, self.upper, self.frontier);
        ChunkBatch::new(regrade_all(self.advanced), description)
    }
}

/// A [`Builder`](crate::trace::Builder) that collects pre-sorted chunks into a
/// [`ChunkBatch`].
///
/// The builder assumes its inputs arrive already sorted and consolidated (as the
/// `Builder` contract requires), so it does no merging: each pushed chunk is an
/// ordered run, fed straight to [`regrade`](Chunk::regrade) as it arrives — so a batch
/// built here is graded like one produced by the merger, rather than inheriting
/// whatever chunk sizes the caller happened to push.
pub struct ChunkBuilder<C: Chunk> {
    /// Pushed chunks awaiting regrading; holds regrade's sub-`TARGET` carry at the front.
    input: VecDeque<C>,
    /// The graded chunks emitted so far.
    output: VecDeque<C>,
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
        Self { input: VecDeque::new(), output: VecDeque::new() }
    }

    fn push(&mut self, chunk: &mut C) {
        let chunk = std::mem::take(chunk);
        if chunk.len() > 0 {
            self.input.push_back(chunk);
            C::regrade(&mut self.input, false, &mut self.output);
        }
    }

    fn done(self, description: Description<C::Time>) -> ChunkBatch<C> {
        let ChunkBuilder { mut input, mut output } = self;
        C::regrade(&mut input, true, &mut output);
        ChunkBatch::new(output.into(), description)
    }

    fn seal(chain: &mut Vec<C>, description: Description<C::Time>) -> ChunkBatch<C> {
        // The chain is sorted and consolidated but not necessarily graded; regrade it.
        // Already-`TARGET` chunks pass through as cheap `Rc` moves, so a chain that
        // arrives graded (as the batcher's does) pays only an O(#chunks) walk.
        ChunkBatch::new(regrade_all(std::mem::take(chain)), description)
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
    //! * **Trace side.** [`Chunk`] (merge / extract / advance / regrade / bounds)
    //!   plus a cursor. Key lookups are logarithmic by galloping search (`seek_*`),
    //!   independent of chunk size; stepping stays linear (short hops).
    //!
    //! `Clone` is a refcount bump, so the trace merger shares source chunks instead
    //! of copying them.
    //!
    //! **What a columnar impl can and can't reuse.** The protocol (the `VecDeque`
    //! in/out, withhold-by-`push_front`, grade-at-seal) is layout-agnostic and carries
    //! over unchanged. The *merge body* does not: this one merges a single contiguous
    //! `&[((K,V),T,R)]` and bulk-copies disjoint runs with `extend_from_slice` +
    //! `chunks(TARGET)`. A columnar chunk (ranging over `ord_neu`'s deduped layout) has
    //! no such slice — it must range-copy the key / val / time / diff columns with
    //! offset bookkeeping, emitting one key + its val/time run rather than repeated rows.
    //! That is the operation that beats the flat layout on repetitive keys (see the
    //! module-level note on the row-major vs. columnar crossover), and it is also where
    //! the earlier `col_chunk` got into trouble (decompress-and-recompress instead of a
    //! true range-copy). So a columnar `Chunk` is the open bet: nothing here exercises a
    //! columnar merge, and that body — not the protocol — is the phase-2 risk.

    use std::collections::VecDeque;
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

    use super::Chunk;

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
        // The absorb point is the grading target: the chunker fills a scratch chunk
        // to `TARGET` updates before emitting, so chunks arrive pre-graded rather than
        // at timely's byte-derived buffer size (which downstream regrading re-melds).
        fn at_capacity(&self) -> bool { self.0.len() >= TARGET }
        fn ensure_capacity(&mut self, _stash: &mut Option<Self>) {
            let inner = Rc::make_mut(&mut self.0);
            inner.reserve(TARGET.saturating_sub(inner.len()));
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

        /// A two-pointer binary merge that drains the two deques' *loaded* content
        /// through their shared horizon — the lesser of the two deques' last loaded
        /// `(key, val, time)`s — rather than one front-pair at a time. Consolidates
        /// equal triples and bulk-copies disjoint runs as slices, walking across chunk
        /// boundaries with local indices (`p1`/`p2`) that reset as each working chunk
        /// is retired. The side owning the horizon drains fully; the other's partial
        /// working chunk is pruned (its prefix dropped) and `push_front`ed back exactly
        /// once at the yield boundary — so the per-call prune cost amortizes over the
        /// whole burst the harness loaded, not over each chunk.
        fn merge(in1: &mut VecDeque<Self>, in2: &mut VecDeque<Self>, out: &mut VecDeque<Self>) {
            fn kv<K, V, T, R>(u: &((K, V), T, R)) -> (&K, &V) { (&u.0.0, &u.0.1) }

            let mut result: Vec<((K, V), T, R)> = Vec::with_capacity(TARGET);
            let mut flush = |result: &mut Vec<((K, V), T, R)>, force: bool| {
                if result.len() >= TARGET || (force && !result.is_empty()) {
                    out.push_back(VecChunk(Rc::new(std::mem::replace(result, Vec::with_capacity(TARGET)))));
                }
            };

            // Working chunks (the shared `Rc`, read by index — never `take`n, so a
            // source clone is not deep-copied) and their positions; both deques are
            // non-empty on entry. The guard keeps both cursors valid for indexing; a
            // working chunk consumed mid-merge is refilled at the foot of the loop, and
            // when a deque runs dry we stop — that side has presented all its loaded
            // data, so its last triple is the horizon and the rest is left for next time.
            let mut c1 = in1.pop_front().unwrap();
            let mut c2 = in2.pop_front().unwrap();
            let (mut p1, mut p2) = (0usize, 0usize);
            while p1 < c1.0.len() && p2 < c2.0.len() {
                let a = &c1.0[p1];
                let b = &c2.0[p2];
                match (kv(a), &a.1).cmp(&(kv(b), &b.1)) {
                    // Copy the run of one side strictly below the other's head (within
                    // the current working chunk): collisions are impossible within it,
                    // so it moves as slices cut at the grading target.
                    std::cmp::Ordering::Less => {
                        let run = gallop(&c1.0[..], p1 + 1, |u| (kv(u), &u.1) < (kv(b), &b.1));
                        for piece in c1.0[p1..run].chunks(TARGET) {
                            result.extend_from_slice(piece);
                            flush(&mut result, false);
                        }
                        p1 = run;
                    }
                    std::cmp::Ordering::Greater => {
                        let run = gallop(&c2.0[..], p2 + 1, |u| (kv(u), &u.1) < (kv(a), &a.1));
                        for piece in c2.0[p2..run].chunks(TARGET) {
                            result.extend_from_slice(piece);
                            flush(&mut result, false);
                        }
                        p2 = run;
                    }
                    std::cmp::Ordering::Equal => {
                        let mut diff = a.2.clone();
                        diff.plus_equals(&b.2);
                        if !diff.is_zero() {
                            result.push((a.0.clone(), a.1.clone(), diff));
                        }
                        p1 += 1;
                        p2 += 1;
                        flush(&mut result, false);
                    }
                }
                // Refill either working chunk consumed by the step above; stop the drain
                // once a deque is exhausted (the `&&` guard then never re-enters).
                if p1 == c1.0.len() {
                    match in1.pop_front() { Some(c) => { c1 = c; p1 = 0; } None => break }
                }
                if p2 == c2.0.len() {
                    match in2.pop_front() { Some(c) => { c2 = c; p2 = 0; } None => break }
                }
            }
            flush(&mut result, true);
            // One side's deque emptied with its working chunk exhausted; the other's
            // working chunk is partial — push back just its unconsumed suffix (one copy
            // per call), ahead of whatever loaded chunks remain in that deque.
            if p1 < c1.0.len() { in1.push_front(VecChunk(Rc::new(c1.0[p1..].to_vec()))); }
            if p2 < c2.0.len() { in2.push_front(VecChunk(Rc::new(c2.0[p2..].to_vec()))); }
        }

        fn extract(
            input: &mut VecDeque<Self>,
            frontier: &Antichain<T>,
            residual: &mut Antichain<T>,
            keep: &mut VecDeque<Self>,
            ship: &mut VecDeque<Self>,
        ) {
            // Fill `TARGET`-sized buffers directly, so the chunks pushed are already
            // graded and `regrade` passes them through as `Rc` moves rather than
            // re-splitting (and re-copying) a monolithic chunk. Emptied input `Vec`s
            // are recycled as the next buffers, so allocations balance input against
            // output instead of one fresh buffer per emitted chunk.
            let mut stash: Vec<Vec<((K, V), T, R)>> = Vec::new();
            let take_buf = |stash: &mut Vec<_>| stash.pop().unwrap_or_default();
            let (mut k, mut s) = (take_buf(&mut stash), take_buf(&mut stash));
            for chunk in input.drain(..) {
                let mut v = take(chunk);
                for u in v.drain(..) {
                    if frontier.borrow().less_equal(&u.1) {
                        residual.insert_ref(&u.1);
                        k.push(u);
                        if k.len() >= TARGET { keep.push_back(VecChunk(Rc::new(std::mem::replace(&mut k, take_buf(&mut stash))))); }
                    } else {
                        s.push(u);
                        if s.len() >= TARGET { ship.push_back(VecChunk(Rc::new(std::mem::replace(&mut s, take_buf(&mut stash))))); }
                    }
                }
                stash.push(v);
            }
            if !k.is_empty() { keep.push_back(VecChunk(Rc::new(k))); }
            if !s.is_empty() { ship.push_back(VecChunk(Rc::new(s))); }
        }

        fn advance(
            input: &mut VecDeque<Self>,
            frontier: &Antichain<T>,
            done: bool,
            out: &mut VecDeque<Self>,
        ) {
            // Advance and consolidate every *complete* `(key, val)` group eagerly,
            // so its updates can be released as soon as the input proves no later
            // time for the pair can arrive. A group is contiguous in the sorted
            // chain, so the only one that might continue in a future call is the last;
            // unless `done`, we process up to its start and `push_front` the rest as
            // the withheld carry for the next call.
            let mut stash: Vec<Vec<((K, V), T, R)>> = Vec::new();
            // Build the working buffer by *reusing the front chunk's storage* (the
            // carry from last time) and appending the rest (recycling each emptied
            // `Vec`). Reusing the front is what keeps a withheld group from being
            // recopied across calls: it just accumulates in place, so a `(key, val)`
            // larger than the working set costs O(total) over the run, not O(total²).
            let mut buf = match input.pop_front() { Some(chunk) => take(chunk), None => return };
            while let Some(chunk) = input.pop_front() {
                let mut v = take(chunk);
                buf.append(&mut v);
                stash.push(v);
            }
            if buf.is_empty() { return; }

            // If every available update shares one `(key, val)`, no group is provably
            // complete — a later call may extend it — so make no progress unless
            // `done`: push the accumulated buffer back as the carry and return. This is
            // the giant-key case; comparing only the first and last pair detects it
            // without scanning, and reusing the front above makes the retention free.
            if !done && buf[0].0 == buf[buf.len() - 1].0 {
                input.push_front(VecChunk(Rc::new(buf)));
                return;
            }

            // Otherwise at least the first group is complete. Withhold the last group
            // (a single `(key, val)`) as the next carry unless the input is complete.
            let end = if done { buf.len() } else {
                let last_kv = buf[buf.len() - 1].0.clone();
                let mut start = buf.len();
                while start > 0 && buf[start - 1].0 == last_kv { start -= 1; }
                start
            };
            if end < buf.len() {
                input.push_front(VecChunk(Rc::new(buf.split_off(end))));
            }
            // Advance + consolidate each group into `TARGET`-sized output chunks,
            // filling buffers reclaimed from the recycled `Vec`s.
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
                        if result.len() >= TARGET { out.push_back(VecChunk(Rc::new(std::mem::replace(&mut result, stash.pop().unwrap_or_default())))); }
                    }
                }
                i = j;
            }
            if !result.is_empty() { out.push_back(VecChunk(Rc::new(result))); }
        }

        fn regrade(input: &mut VecDeque<Self>, done: bool, out: &mut VecDeque<Self>) {
            // Maximal packing: emit chunks as large as possible up to `TARGET`,
            // never splitting a pair that could combine into one legal (`<= TARGET`)
            // chunk. A chunk of exactly `TARGET` is maximal — it cannot grow — so it
            // passes straight through as an `Rc` move; only sub-`TARGET` chunks are
            // copied, and only to coalesce with a neighbour. Producers fill to
            // `TARGET`, so in steady state every chunk passes through and only the
            // occasional trailing partial is coalesced.
            //
            // `carry` is the (sub-`TARGET`) chunk under construction. It is flushed
            // once it reaches `TARGET`, `push_front`ed back onto `input` between calls,
            // or emitted on `done`. Whenever `carry` is non-empty its left neighbour in
            // `out` is a `TARGET` chunk (or `carry` is `out`'s first chunk), so
            // emitting `carry` against a neighbour it cannot merge with — their sum
            // exceeds `TARGET` — keeps the packing maximal on both sides.
            let mut carry: Vec<((K, V), T, R)> = Vec::new();
            while let Some(chunk) = input.pop_front() {
                if carry.is_empty() {
                    absorb(chunk, &mut carry, out);
                } else if carry.len() + chunk.0.len() <= TARGET {
                    // Combines into one legal chunk; coalesce in place.
                    carry.extend(take(chunk));
                    if carry.len() == TARGET {
                        out.push_back(VecChunk(Rc::new(std::mem::take(&mut carry))));
                    }
                } else {
                    // Cannot combine without exceeding `TARGET`; `carry` is maximal
                    // against this neighbour, so emit it and absorb the chunk afresh.
                    out.push_back(VecChunk(Rc::new(std::mem::take(&mut carry))));
                    absorb(chunk, &mut carry, out);
                }
            }
            if !carry.is_empty() {
                let chunk = VecChunk(Rc::new(carry));
                if done { out.push_back(chunk); } else { input.push_front(chunk); }
            }
        }
    }

    /// Emit maximal `TARGET`-sized chunks off the front of `carry`, leaving the
    /// sub-`TARGET` tail behind.
    fn peel<K: Clone, V: Clone, T: Clone, R: Clone>(
        carry: &mut Vec<((K, V), T, R)>,
        out: &mut VecDeque<VecChunk<K, V, T, R>>,
    ) {
        let mut start = 0;
        while carry.len() - start >= TARGET {
            out.push_back(VecChunk(Rc::new(carry[start..start + TARGET].to_vec())));
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
        out: &mut VecDeque<VecChunk<K, V, T, R>>,
    ) {
        use std::cmp::Ordering::{Equal, Greater, Less};
        match chunk.0.len().cmp(&TARGET) {
            Equal => out.push_back(chunk),
            Less => *carry = take(chunk),
            Greater => { *carry = take(chunk); peel(carry, out); }
        }
    }

    #[cfg(test)]
    mod test {
        use std::collections::VecDeque;
        use super::{Chunk, VecChunk};
        use crate::trace::chunk::merge_chains;
        use std::rc::Rc;

        fn chunk(updates: Vec<((u64, u64), u64, i64)>) -> VecChunk<u64, u64, u64, i64> {
            VecChunk(Rc::new(updates))
        }

        // Flatten a chunk sequence back to its update stream.
        fn flat<I: IntoIterator<Item = VecChunk<u64, u64, u64, i64>>>(chunks: I) -> Vec<((u64, u64), u64, i64)> {
            chunks.into_iter().flat_map(|c| (*c.0).clone()).collect()
        }

        // `extract` partitions by frontier and folds the kept frontier into `residual`;
        // a terminal `regrade` then grades each side (the seams of near-graded output).
        #[test]
        fn extract_partitions_and_grades() {
            use super::TARGET;
            use crate::trace::chunk::{is_graded, regrade_all};
            use timely::progress::Antichain;

            // 4·TARGET updates spread over many input chunks; even times ship
            // (< frontier), odd times keep (>= frontier), so both sides straddle.
            let n = 4 * TARGET as u64;
            let mut input: VecDeque<_> = (0..n).map(|i| chunk(vec![((i, 0), i % 2, 1)])).collect();
            let frontier = Antichain::from_elem(1u64);
            let mut residual = Antichain::new();
            let (mut keep, mut ship) = (VecDeque::new(), VecDeque::new());
            VecChunk::extract(&mut input, &frontier, &mut residual, &mut keep, &mut ship);
            let (keep, ship) = (regrade_all(keep), regrade_all(ship));

            // Kept times are exactly {1}; that is the residual frontier.
            assert_eq!(residual, Antichain::from_elem(1u64));
            // Both sides are graded after the regrade.
            assert!(is_graded(&keep), "ungraded keep: {:?}", keep.iter().map(Chunk::len).collect::<Vec<_>>());
            assert!(is_graded(&ship), "ungraded ship: {:?}", ship.iter().map(Chunk::len).collect::<Vec<_>>());
            // Nothing lost: half the updates each way.
            assert_eq!(keep.iter().map(Chunk::len).sum::<usize>(), n as usize / 2);
            assert_eq!(ship.iter().map(Chunk::len).sum::<usize>(), n as usize / 2);
        }

        // `advance` advances and consolidates complete `(key, val)` groups eagerly,
        // pushing the (possibly-growing) last group back as the carry when not `done`.
        #[test]
        fn advance_emits_complete_groups_eagerly() {
            use timely::progress::Antichain;

            let frontier = Antichain::from_elem(5u64);
            // Group (0,0) is complete within this chunk; group (1,0) might still grow.
            let c0 = chunk(vec![((0, 0), 0, 1), ((0, 0), 1, 1), ((1, 0), 0, 1)]);
            let mut input: VecDeque<_> = VecDeque::from([c0]);
            let mut out = VecDeque::new();
            VecChunk::advance(&mut input, &frontier, false, &mut out);

            // The trailing group (1,0) is withheld as the carry at the front of `input`.
            assert_eq!(input.len(), 1);
            assert_eq!(Chunk::len(&input[0]), 1);
            // Group (0,0)'s times {0,1} advanced to 5 and consolidated, emitted now.
            assert_eq!(flat(out), vec![((0, 0), 5, 2)]);
        }

        // Streaming the input one chunk at a time must yield exactly what a single
        // all-at-once flush does — the resumable path is just the one-shot path cut
        // at group boundaries.
        #[test]
        fn advance_resumable_matches_oneshot() {
            use timely::progress::Antichain;

            let frontier = Antichain::from_elem(3u64);
            // Groups span chunk boundaries and carry several times each.
            let input = || vec![
                chunk(vec![((0, 0), 0, 1), ((0, 0), 1, 1), ((1, 0), 0, 1)]),
                chunk(vec![((1, 0), 5, 1), ((1, 1), 0, 1), ((2, 0), 0, 1)]),
                chunk(vec![((2, 0), 2, 1), ((2, 0), 9, 1)]),
            ];

            let oneshot = {
                let mut q: VecDeque<_> = input().into();
                let mut out = VecDeque::new();
                VecChunk::advance(&mut q, &frontier, false, &mut out);
                VecChunk::advance(&mut q, &frontier, true, &mut out);
                flat(out)
            };
            let incremental = {
                let mut q = VecDeque::new();
                let mut out = VecDeque::new();
                for c in input() { q.push_back(c); VecChunk::advance(&mut q, &frontier, false, &mut out); }
                VecChunk::advance(&mut q, &frontier, true, &mut out);
                flat(out)
            };
            assert_eq!(oneshot, incremental);
            // Times are advanced: nothing below the frontier survives.
            for u in &oneshot { assert!(u.1 >= 3); }
        }

        // A single `(key, val)` whose updates span every pushed chunk: `advance`
        // can make no progress until `done`, accumulating in the carry in place.
        // It must still produce the right advanced+consolidated result at the end.
        #[test]
        fn advance_single_key_spanning_pushes() {
            use timely::progress::Antichain;

            let frontier = Antichain::from_elem(100u64);
            let n = 50u64;
            let make = || (0..n).map(|t| chunk(vec![((7u64, 0u64), t, 1i64)])).collect::<Vec<_>>();

            let mut q = VecDeque::new();
            let mut out = VecDeque::new();
            for c in make() { q.push_back(c); VecChunk::advance(&mut q, &frontier, false, &mut out); }
            VecChunk::advance(&mut q, &frontier, true, &mut out);
            // All times advance to 100 and consolidate to one update of diff `n`.
            assert_eq!(flat(out), vec![((7u64, 0u64), 100u64, n as i64)]);
        }

        #[test]
        fn merge_chains_consolidates() {
            let a = chunk(vec![((0, 0), 0, 1), ((1, 0), 0, 1)]);
            let b = chunk(vec![((0, 0), 0, 1), ((2, 0), 0, 1)]);
            let mut out = VecDeque::new();
            merge_chains(vec![a], vec![b], &mut out);
            assert_eq!(flat(out), vec![((0, 0), 0, 2), ((1, 0), 0, 1), ((2, 0), 0, 1)]);
        }

        // Merging runs larger than `TARGET`, then regrading, yields a *graded* sequence
        // (each chunk `<= TARGET`, adjacent pairs summing past `TARGET`) reproducing the
        // consolidated sorted contents.
        #[test]
        fn merge_emits_graded_chunks() {
            use super::TARGET;
            use crate::trace::chunk::{is_graded, merge_chains, regrade_all};

            // Two interleaving single-chunk chains: evens and odds over `0..4·TARGET`.
            let n = 4 * TARGET as u64;
            let evens = chunk((0..n).step_by(2).map(|k| ((k, 0), 0, 1)).collect());
            let odds = chunk((0..n).step_by(2).map(|k| ((k + 1, 0), 0, 1)).collect());

            let mut out = VecDeque::new();
            merge_chains(vec![evens], vec![odds], &mut out);
            let chunks = regrade_all(out);

            assert!(is_graded(&chunks), "merge output not graded: {:?}",
                chunks.iter().map(Chunk::len).collect::<Vec<_>>());
            // Contents are exactly the sorted keys `0..4·TARGET`, each once.
            let want: Vec<_> = (0..n).map(|k| ((k, 0u64), 0u64, 1i64)).collect();
            assert_eq!(flat(chunks), want);
        }

        // Property test: merging two *multi-chunk* chains (driven through `merge` by
        // `merge_chains`) reproduces the union of all updates, consolidated. Tiny
        // chunks force `(key, val)` groups — which can span several times — to
        // straddle chunk boundaries on both sides, exercising the refill path the
        // single-chunk merge tests never reach. The independent oracle is
        // `consolidate_updates` over the concatenation.
        #[test]
        fn merge_matches_reference() {
            use crate::trace::chunk::merge_chains;
            use crate::consolidation::consolidate_updates;

            // Deterministic xorshift PRNG — no dev-dependency on `rand`.
            let mut seed = 0x2545F4914F6CDD1Du64;
            let mut rng = move || { seed ^= seed << 13; seed ^= seed >> 7; seed ^= seed << 17; seed };

            // A sorted, consolidated update set over a small (key, val, time) space,
            // so the two chains collide and a `(key, val)` carries several times.
            fn gen(rng: &mut impl FnMut() -> u64, n: usize) -> Vec<((u64, u64), u64, i64)> {
                let mut v: Vec<((u64, u64), u64, i64)> = (0..n).map(|_| {
                    let k = rng() % 20; let val = rng() % 3; let t = rng() % 8;
                    let d = if rng() % 4 == 0 { -1 } else { 1 };
                    ((k, val), t, d)
                }).collect();
                consolidate_updates(&mut v);
                v
            }
            // Split a consolidated set into a chain of small chunks (each sorted and
            // consolidated; together globally sorted), so groups straddle boundaries.
            fn chain(updates: &[((u64, u64), u64, i64)], sz: usize) -> Vec<VecChunk<u64, u64, u64, i64>> {
                updates.chunks(sz).map(|c| VecChunk(Rc::new(c.to_vec()))).collect()
            }

            for _ in 0..300 {
                let n1 = (rng() as usize % 60) + 1;
                let u1 = gen(&mut rng, n1);
                let n2 = (rng() as usize % 60) + 1;
                let u2 = gen(&mut rng, n2);
                if u1.is_empty() || u2.is_empty() { continue; }
                let sz = (rng() as usize % 5) + 1; // tiny chunks → heavy straddling

                let mut out = VecDeque::new();
                merge_chains(chain(&u1, sz), chain(&u2, sz), &mut out);
                let merged = flat(out);

                let mut reference: Vec<_> = u1.iter().chain(u2.iter()).cloned().collect();
                consolidate_updates(&mut reference);

                assert_eq!(merged, reference, "chunk size {sz}\n  u1={u1:?}\n  u2={u2:?}");
            }
        }

        // `regrade` must produce a *maximal packing*: adjacent sub-`TARGET` chunks
        // that could combine into one legal chunk are coalesced, full chunks pass
        // through as `Rc` moves, and contents are preserved exactly.
        #[test]
        fn regrade_maximal_packing() {
            use super::TARGET;
            use crate::trace::chunk::is_graded;

            // A mix of small and full chunks with distinct, increasing keys (so the
            // concatenation is sorted and nothing consolidates away).
            let t = TARGET;
            let sizes = [t / 3, t / 3, t / 3, t, t / 2, t / 2, t, 1, t - 1];
            let total: usize = sizes.iter().sum();
            let mut key = 0u64;
            let mut input = VecDeque::new();
            let mut output = VecDeque::new();
            for &s in &sizes {
                let updates: Vec<_> = (0..s).map(|_| { let k = key; key += 1; ((k, 0u64), 0u64, 1i64) }).collect();
                input.push_back(chunk(updates));
                VecChunk::regrade(&mut input, false, &mut output);
            }
            VecChunk::regrade(&mut input, true, &mut output);
            let chunks: Vec<_> = output.into();

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
