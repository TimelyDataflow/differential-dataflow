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
    /// granularity and a larger within-chunk seek. Required, not defaulted: the
    /// right value is layout-dependent, so every implementor chooses it deliberately.
    const TARGET: usize;

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

pub mod vec_chunk;
