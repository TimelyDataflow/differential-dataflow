//! Cursor navigation over chunks and chunk batches.
//!
//! Implementing [`NavigableChunk`] lets a [`ChunkBatch`] expose a [`ChunkBatchCursor`].
//! This cursor joins key and value groups that span chunk boundaries, delegating navigation within each chunk to its cursor.
//!
//! Searches across chunks read resident [`bounds`](NavigableChunk::bounds) metadata.
//! Key seeks gallop from a remembered chunk, and boundary crossings compare adjacent chunks' bounds.
//! Chunk bodies are read when navigation reaches them.
//! Implementors must keep `bounds` cheap even when a chunk's body is paged out.

use crate::trace::Navigable;
use crate::trace::cursor::Cursor;
use crate::trace::implementations::BatchContainer;

use super::{Chunk, ChunkBatch};

/// The navigation capability: a [`Chunk`] whose contents can be read by cursor.
///
/// This is optional. Batch formation and trace maintenance need only [`Chunk`];
/// implementing this trait additionally lets [`ChunkBatch`] offer the straddle
/// cursor ([`ChunkBatchCursor`]), which is how cursor-driven operator paths read
/// an arrangement. Chunks consumed only by whole-chunk logic (tactics) can skip it.
///
/// `bounds` must stay cheap even when a chunk's body is paged out: the straddle
/// cursor consults chunk bounds throughout navigation — seeks binary-search them,
/// boundary crossings compare against them — and opens a chunk's body only when a
/// query touches it.
pub trait NavigableChunk: Chunk + Navigable<Cursor: Cursor<Time = <Self as Chunk>::Time>> {
    /// The first and last `(key, val, time)` triples in the chunk.
    fn bounds(&self) -> (
        (<Self::Cursor as Cursor>::Key<'_>, <Self::Cursor as Cursor>::Val<'_>, <Self::Cursor as Cursor>::TimeGat<'_>),
        (<Self::Cursor as Cursor>::Key<'_>, <Self::Cursor as Cursor>::Val<'_>, <Self::Cursor as Cursor>::TimeGat<'_>),
    );
}

type KeyCon<C> = <<C as Navigable>::Cursor as Cursor>::KeyContainer;
type ValCon<C> = <<C as Navigable>::Cursor as Cursor>::ValContainer;

impl<C: NavigableChunk> crate::trace::Navigable for ChunkBatch<C> {
    type Cursor = ChunkBatchCursor<C>;
    fn cursor(&self) -> Self::Cursor {
        ChunkBatchCursor { key_chunk: 0, chunk: 0, inner: self.chunks.first().map(C::cursor) }
    }
}

/// A cursor over a [`ChunkBatch`], merging the per-chunk cursors.
///
/// Chunk breakpoints are unconstrained, so a single key — or `(key, val)` — may
/// straddle consecutive chunks. But the chunks are one globally-sorted sequence
/// merely cut at arbitrary points, so the operation is *concatenation*, never a
/// merge: across a boundary a key's vals concatenate and a `(key, val)`'s times
/// concatenate. The cursor exploits this. It holds the chunk currently being read
/// and a cursor into it; it seeks by galloping the chunks' resident
/// [`bounds`](NavigableChunk::bounds) from a remembered hint (the current key's first
/// chunk), and at boundaries it *continues* into the next chunk rather than merging —
/// consulting the two neighbouring chunks' bounds to detect when a key or `(key, val)`
/// spills forward, without touching chunk contents. No state is materialized up front:
/// a monotone seek sweep costs `O(log Δ)` bounds reads per seek and a sequential pass two
/// per boundary, so cursor construction is free.
pub struct ChunkBatchCursor<C: NavigableChunk> {
    /// First chunk of the current key's run; where `rewind_vals` returns to.
    key_chunk: usize,
    /// Chunk currently being read; `>= key_chunk`, within the current key's span.
    chunk: usize,
    /// Cursor into `chunk`; `None` once `chunk` is past the last chunk.
    inner: Option<C::Cursor>,
}

impl<C: NavigableChunk> ChunkBatchCursor<C> {
    /// Move the active chunk to `c`, opening a fresh inner cursor at its start.
    fn goto(&mut self, c: usize, storage: &ChunkBatch<C>) {
        self.chunk = c;
        self.inner = storage.chunks.get(c).map(C::cursor);
    }

    /// Does key `k` span the boundary between chunks `c` and `c + 1` — chunk `c`
    /// ends with it and chunk `c + 1` begins with it?
    ///
    /// Two resident [`bounds`](NavigableChunk::bounds) reads; the `reborrow`s
    /// unify the (invariant) item lifetimes with `k`'s.
    fn key_spills(s: &ChunkBatch<C>, c: usize, k: <C::Cursor as Cursor>::Key<'_>) -> bool {
        <KeyCon<C> as BatchContainer>::reborrow(s.chunks[c].bounds().1.0) == <KeyCon<C> as BatchContainer>::reborrow(k)
            && <KeyCon<C> as BatchContainer>::reborrow(s.chunks[c + 1].bounds().0.0) == <KeyCon<C> as BatchContainer>::reborrow(k)
    }

    /// Does `(k, v)` span the boundary between chunks `c` and `c + 1`?
    fn val_spills(s: &ChunkBatch<C>, c: usize, k: <C::Cursor as Cursor>::Key<'_>, v: <C::Cursor as Cursor>::Val<'_>) -> bool {
        Self::key_spills(s, c, k)
            && <ValCon<C> as BatchContainer>::reborrow(s.chunks[c].bounds().1.1) == <ValCon<C> as BatchContainer>::reborrow(v)
            && <ValCon<C> as BatchContainer>::reborrow(s.chunks[c + 1].bounds().0.1) == <ValCon<C> as BatchContainer>::reborrow(v)
    }

    /// The first chunk, at or after `hint`, whose last key is `>= key`: where `key`'s run
    /// begins. `hint` — the current key's first chunk (`key_chunk`) — is a valid lower bound
    /// for a forward seek; a backward seek is detected and served by a full search from the
    /// front. Galloping keeps a monotone seek sweep at `O(log Δ)` bounds reads per seek rather
    /// than `O(log chunks)`; only resident [`bounds`](NavigableChunk::bounds) are read.
    fn locate_key(s: &ChunkBatch<C>, hint: usize, key: <C::Cursor as Cursor>::Key<'_>) -> usize {
        let n = s.chunks.len();
        // `last_key(i) < key`, from chunk `i`'s resident bounds.
        let lt = |i: usize| <KeyCon<C> as BatchContainer>::reborrow(s.chunks[i].bounds().1.0)
            .lt(&<KeyCon<C> as BatchContainer>::reborrow(key));
        let hint = hint.min(n);
        // The hint can skip the answer only on a backward seek; then search from the front.
        let lo = if hint == 0 || lt(hint - 1) { hint } else { 0 };
        if lo >= n || !lt(lo) { return lo; }
        // Exponential search from `lo`, then binary within the final bracket.
        let (mut prev, mut step) = (lo, 1usize);
        while prev + step < n && lt(prev + step) { prev += step; step <<= 1; }
        let (mut a, mut b) = (prev + 1, (prev + step).min(n));
        while a < b { let m = a + (b - a) / 2; if lt(m) { a = m + 1; } else { b = m; } }
        a
    }
}

impl<C: NavigableChunk> Cursor for ChunkBatchCursor<C> {
    type Storage = ChunkBatch<C>;

    type KeyContainer = <C::Cursor as Cursor>::KeyContainer;
    type Key<'a> = <C::Cursor as Cursor>::Key<'a>;
    type ValContainer = <C::Cursor as Cursor>::ValContainer;
    type Val<'a> = <C::Cursor as Cursor>::Val<'a>;
    type ValOwn = <C::Cursor as Cursor>::ValOwn;
    type TimeContainer = <C::Cursor as Cursor>::TimeContainer;
    type TimeGat<'a> = <C::Cursor as Cursor>::TimeGat<'a>;
    type Time = <C::Cursor as Cursor>::Time;
    type DiffContainer = <C::Cursor as Cursor>::DiffContainer;
    type DiffGat<'a> = <C::Cursor as Cursor>::DiffGat<'a>;
    type Diff = <C::Cursor as Cursor>::Diff;

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
        while c + 1 < s.chunks.len() && Self::val_spills(s, c, k, v) {
            c += 1;
            s.chunks[c].cursor().map_times(&s.chunks[c], &mut logic);
        }
    }

    fn step_key(&mut self, s: &Self::Storage) {
        if !self.key_valid(s) { return; }
        let n = s.chunks.len();
        let k = self.key(s);
        // Advance to the last chunk the key spans.
        while self.chunk + 1 < n && Self::key_spills(s, self.chunk, k) {
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
        // First chunk whose last key is `>= key`: where `key`'s run begins. Gallop from the
        // current key's first chunk (`key_chunk`), a lower bound for forward seeks.
        let lo = Self::locate_key(s, self.key_chunk, key);
        self.goto(lo, s);
        self.key_chunk = lo;
        if lo < n { self.inner.as_mut().unwrap().seek_key(&s.chunks[lo], key); }
    }

    fn step_val(&mut self, s: &Self::Storage) {
        if !self.val_valid(s) { return; }
        let n = s.chunks.len();
        let (k, v) = (self.key(s), self.val(s));
        // Advance to the last chunk the (key, val) spans.
        while self.chunk + 1 < n && Self::val_spills(s, self.chunk, k, v) {
            self.goto(self.chunk + 1, s);
        }
        // Step past the (key, val) within that chunk.
        self.inner.as_mut().unwrap().step_val(&s.chunks[self.chunk]);
        // If the key's vals are exhausted here but the key spills, roll forward.
        if !self.inner.as_ref().unwrap().val_valid(&s.chunks[self.chunk])
            && self.chunk + 1 < n && Self::key_spills(s, self.chunk, k)
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
            if self.chunk + 1 < n && Self::key_spills(s, self.chunk, k) {
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
