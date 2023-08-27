//! Types for reference-counted batches.

use std::ops::Deref;
use std::rc::Rc;

use timely::communication::message::RefOrMut;
use timely::progress::frontier::AntichainRef;
use timely::progress::{Antichain, Timestamp};
use timely::PartialOrder;

use crate::trace::{Batch, BatchReader, Batcher, Builder, Cursor, Description, Merger};

/// A batch implementation that wraps underlying batches in `Rc`.
///
/// Keeps a description separate from that of the wrapped batch, to enable efficient merging with
/// empty batches by extending the reported lower/upper bounds.
pub struct RcBatch<B>
where
    B: BatchReader,
{
    inner: Rc<B>,
    desc: Description<B::Time>,
}

impl<B> RcBatch<B>
where
    B: BatchReader,
    B::Time: Timestamp,
{
    fn new(inner: B) -> Self {
        Rc::new(inner).into()
    }
}

impl<B> Deref for RcBatch<B>
where
    B: BatchReader,
{
    type Target = B;

    fn deref(&self) -> &B {
        &self.inner
    }
}

impl<B> Clone for RcBatch<B>
where
    B: BatchReader,
    B::Time: Timestamp,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            desc: self.desc.clone(),
        }
    }
}

impl<B> From<Rc<B>> for RcBatch<B>
where
    B: BatchReader,
    B::Time: Timestamp,
{
    fn from(inner: Rc<B>) -> Self {
        let desc = inner.description().clone();
        Self { inner, desc }
    }
}

impl<B> BatchReader for RcBatch<B>
where
    B: BatchReader,
{
    type Key = B::Key;
    type Val = B::Val;
    type Time = B::Time;
    type R = B::R;

    type Cursor = RcBatchCursor<B>;

    fn cursor(&self) -> Self::Cursor {
        RcBatchCursor {
            inner: self.inner.cursor(),
        }
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn description(&self) -> &Description<Self::Time> {
        &self.desc
    }
}

impl<B> Batch for RcBatch<B>
where
    B: Batch,
    B::Time: Timestamp,
{
    type Batcher = RcBatcher<B>;
    type Builder = RcBuilder<B>;
    type Merger = RcMerger<B>;

    fn merge_empty(mut self, other: &Self) -> Self {
        assert!(other.is_empty());

        let (lower, upper) = if self.lower() == other.upper() {
            (other.lower().clone(), self.upper().clone())
        } else if self.upper() == other.lower() {
            (self.lower().clone(), other.upper().clone())
        } else {
            panic!("trying to merge non-consecutive batches");
        };

        self.desc = Description::new(lower, upper, self.desc.since().clone());
        self
    }
}

/// A cursor for navigating `RcBatch`es.
pub struct RcBatchCursor<B: BatchReader> {
    inner: B::Cursor,
}

impl<B> Cursor for RcBatchCursor<B>
where
    B: BatchReader,
{
    type Key = B::Key;
    type Val = B::Val;
    type Time = B::Time;
    type R = B::R;

    type Storage = RcBatch<B>;

    #[inline]
    fn key_valid(&self, storage: &Self::Storage) -> bool {
        self.inner.key_valid(&storage.inner)
    }

    #[inline]
    fn val_valid(&self, storage: &Self::Storage) -> bool {
        self.inner.val_valid(&storage.inner)
    }

    #[inline]
    fn key<'a>(&self, storage: &'a Self::Storage) -> &'a Self::Key {
        self.inner.key(&storage.inner)
    }

    #[inline]
    fn val<'a>(&self, storage: &'a Self::Storage) -> &'a Self::Val {
        self.inner.val(&storage.inner)
    }

    #[inline]
    fn map_times<L>(&mut self, storage: &Self::Storage, logic: L)
    where
        L: FnMut(&Self::Time, &Self::R),
    {
        self.inner.map_times(&storage.inner, logic)
    }

    #[inline]
    fn step_key(&mut self, storage: &Self::Storage) {
        self.inner.step_key(&storage.inner)
    }

    #[inline]
    fn seek_key(&mut self, storage: &Self::Storage, key: &Self::Key) {
        self.inner.seek_key(&storage.inner, key)
    }

    #[inline]
    fn step_val(&mut self, storage: &Self::Storage) {
        self.inner.step_val(&storage.inner)
    }

    #[inline]
    fn seek_val(&mut self, storage: &Self::Storage, val: &Self::Val) {
        self.inner.seek_val(&storage.inner, val)
    }

    #[inline]
    fn rewind_keys(&mut self, storage: &Self::Storage) {
        self.inner.rewind_keys(&storage.inner)
    }

    #[inline]
    fn rewind_vals(&mut self, storage: &Self::Storage) {
        self.inner.rewind_vals(&storage.inner)
    }
}

/// A type used to assemble `RcBatch`es from unordered updates.
pub struct RcBatcher<B: Batch> {
    inner: B::Batcher,
}

impl<B> Batcher<RcBatch<B>> for RcBatcher<B>
where
    B: Batch,
    B::Time: Timestamp,
{
    fn new() -> Self {
        Self {
            inner: B::Batcher::new(),
        }
    }

    fn push_batch(&mut self, batch: RefOrMut<Vec<((B::Key, B::Val), B::Time, B::R)>>) {
        self.inner.push_batch(batch);
    }

    fn seal(&mut self, upper: Antichain<B::Time>) -> RcBatch<B> {
        RcBatch::new(self.inner.seal(upper))
    }

    fn frontier(&mut self) -> AntichainRef<B::Time> {
        self.inner.frontier()
    }
}

/// A type used to assemble `RcBatch`es from ordered update sequences.
pub struct RcBuilder<B: Batch> {
    inner: B::Builder,
}

impl<B> Builder<RcBatch<B>> for RcBuilder<B>
where
    B: Batch,
    B::Time: Timestamp,
{
    fn new() -> Self {
        Self {
            inner: B::Builder::new(),
        }
    }

    fn with_capacity(cap: usize) -> Self {
        Self {
            inner: B::Builder::with_capacity(cap),
        }
    }

    fn push(&mut self, element: (B::Key, B::Val, B::Time, B::R)) {
        self.inner.push(element);
    }

    fn done(
        self,
        lower: Antichain<B::Time>,
        upper: Antichain<B::Time>,
        since: Antichain<B::Time>,
    ) -> RcBatch<B> {
        RcBatch::new(self.inner.done(lower, upper, since))
    }
}

/// A type used to progressively merge `RcBatch`es.
pub struct RcMerger<B: Batch> {
    inner: B::Merger,
    lower: Antichain<B::Time>,
    upper: Antichain<B::Time>,
}

impl<B> Merger<RcBatch<B>> for RcMerger<B>
where
    B: Batch,
    B::Time: Timestamp,
{
    fn new(
        source1: &RcBatch<B>,
        source2: &RcBatch<B>,
        compaction_frontier: Option<AntichainRef<B::Time>>,
    ) -> Self {
        assert!(PartialOrder::less_equal(source1.upper(), source2.lower()));

        let lower = source1.lower().clone();
        let upper = source2.upper().clone();

        Self {
            inner: B::Merger::new(&source1.inner, &source2.inner, compaction_frontier),
            lower,
            upper,
        }
    }

    fn work(&mut self, source1: &RcBatch<B>, source2: &RcBatch<B>, fuel: &mut isize) {
        self.inner.work(&source1.inner, &source2.inner, fuel);
    }

    fn done(self) -> RcBatch<B> {
        let inner = self.inner.done();
        let since = inner.description().since().clone();
        let mut batch = RcBatch::new(inner);
        batch.desc = Description::new(self.lower, self.upper, since);
        batch
    }
}
