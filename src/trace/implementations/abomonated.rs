//! Types for abomonated batch.

use std::ops::Deref;

use abomonation::abomonated::Abomonated;
use abomonation::{measure, Abomonation};
use timely::communication::message::RefOrMut;
use timely::progress::frontier::AntichainRef;
use timely::progress::{Antichain, Timestamp};
use timely::PartialOrder;

use crate::trace::{Batch, BatchReader, Batcher, Builder, Cursor, Description, Merger};

/// A batch implementation that wraps underlying batches in `Abomonated`.
///
/// Keeps a description separate from that of the wrapped batch, to enable efficient merging with
/// empty batches by extending the reported lower/upper bounds.
pub struct AbomonatedBatch<B: BatchReader> {
    inner: Abomonated<B, Vec<u8>>,
    desc: Description<B::Time>,
}

impl<B> AbomonatedBatch<B>
where
    B: BatchReader + Abomonation,
    B::Time: Timestamp,
{
    fn new(inner: B) -> Self {
        let mut bytes = Vec::with_capacity(measure(&inner));
        unsafe { abomonation::encode(&inner, &mut bytes).unwrap() };
        let inner = unsafe { Abomonated::<B, _>::new(bytes).unwrap() };
        inner.into()
    }
}

impl<B> Deref for AbomonatedBatch<B>
where
    B: BatchReader,
{
    type Target = B;

    fn deref(&self) -> &B {
        &self.inner
    }
}

impl<B> From<Abomonated<B, Vec<u8>>> for AbomonatedBatch<B>
where
    B: BatchReader + Abomonation,
    B::Time: Timestamp,
{
    fn from(inner: Abomonated<B, Vec<u8>>) -> Self {
        let desc = inner.description().clone();
        Self { inner, desc }
    }
}

impl<B> BatchReader for AbomonatedBatch<B>
where
    B: BatchReader,
{
    type Key = B::Key;
    type Val = B::Val;
    type Time = B::Time;
    type R = B::R;

    type Cursor = AbomonatedBatchCursor<B>;

    fn cursor(&self) -> Self::Cursor {
        AbomonatedBatchCursor {
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

impl<B> Batch for AbomonatedBatch<B>
where
    B: Batch + Abomonation,
    B::Time: Timestamp,
{
    type Batcher = AbomonatedBatcher<B>;
    type Builder = AbomonatedBuilder<B>;
    type Merger = AbomonatedMerger<B>;

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

/// A cursor for navigating `AbomonatedBatch`es.
pub struct AbomonatedBatchCursor<B: BatchReader> {
    inner: B::Cursor,
}

impl<B> Cursor for AbomonatedBatchCursor<B>
where
    B: BatchReader,
{
    type Key = B::Key;
    type Val = B::Val;
    type Time = B::Time;
    type R = B::R;

    type Storage = AbomonatedBatch<B>;

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

/// A type used to assemble `AbomonatedBatch`es from unordered updates.
pub struct AbomonatedBatcher<B: Batch> {
    inner: B::Batcher,
}

impl<B> Batcher<AbomonatedBatch<B>> for AbomonatedBatcher<B>
where
    B: Batch + Abomonation,
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

    fn seal(&mut self, upper: Antichain<B::Time>) -> AbomonatedBatch<B> {
        AbomonatedBatch::new(self.inner.seal(upper))
    }

    fn frontier(&mut self) -> AntichainRef<B::Time> {
        self.inner.frontier()
    }
}

/// A type used to assemble `AbomonatedBatch`es from ordered update sequences.
pub struct AbomonatedBuilder<B: Batch> {
    inner: B::Builder,
}

impl<B> Builder<AbomonatedBatch<B>> for AbomonatedBuilder<B>
where
    B: Batch + Abomonation,
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
    ) -> AbomonatedBatch<B> {
        AbomonatedBatch::new(self.inner.done(lower, upper, since))
    }
}

/// A type used to progressively merge `AbomonatedBatch`es.
pub struct AbomonatedMerger<B: Batch> {
    inner: B::Merger,
    lower: Antichain<B::Time>,
    upper: Antichain<B::Time>,
}

impl<B> Merger<AbomonatedBatch<B>> for AbomonatedMerger<B>
where
    B: Batch + Abomonation,
    B::Time: Timestamp,
{
    fn new(
        source1: &AbomonatedBatch<B>,
        source2: &AbomonatedBatch<B>,
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

    fn work(
        &mut self,
        source1: &AbomonatedBatch<B>,
        source2: &AbomonatedBatch<B>,
        fuel: &mut isize,
    ) {
        self.inner.work(&source1.inner, &source2.inner, fuel);
    }

    fn done(self) -> AbomonatedBatch<B> {
        let inner = self.inner.done();
        let since = inner.description().since().clone();
        let mut batch = AbomonatedBatch::new(inner);
        batch.desc = Description::new(self.lower, self.upper, since);
        batch
    }
}
