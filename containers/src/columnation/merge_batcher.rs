//! Implementations of `ContainerQueue` and `MergerChunk` for `TimelyStack` containers (columnation).

use timely::progress::{Antichain, frontier::AntichainRef, Timestamp};
use columnation::Columnation;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::implementations::{BatchContainer, BuilderInput};
use differential_dataflow::trace::implementations::containers::BatchIndex;
use differential_dataflow::trace::implementations::merge_batcher::container::{ContainerMerger, ContainerQueue, MergerChunk};

use crate::columnation::TimelyStack;

/// A `Merger` implementation backed by `TimelyStack` containers (columnation).
pub type ColMerger<D, T, R> = ContainerMerger<TimelyStack<(D,T,R)>,TimelyStackQueue<(D, T, R)>>;

/// TODO
pub struct TimelyStackQueue<T: Columnation> {
    list: TimelyStack<T>,
    head: usize,
}

impl<D: Ord + Columnation, T: Ord + Columnation, R: Columnation> ContainerQueue<TimelyStack<(D, T, R)>> for TimelyStackQueue<(D, T, R)> {
    fn next_or_alloc(&mut self) -> Result<&(D, T, R), TimelyStack<(D, T, R)>> {
        if self.is_empty() {
            Err(std::mem::take(&mut self.list))
        }
        else {
            Ok(self.pop())
        }
    }
    fn is_empty(&self) -> bool {
        self.head == self.list[..].len()
    }
    fn cmp_heads(&self, other: &Self) -> std::cmp::Ordering {
        let (data1, time1, _) = self.peek();
        let (data2, time2, _) = other.peek();
        (data1, time1).cmp(&(data2, time2))
    }
    fn from(list: TimelyStack<(D, T, R)>) -> Self {
        TimelyStackQueue { list, head: 0 }
    }
}

impl<T: Columnation> TimelyStackQueue<T> {
    fn pop(&mut self) -> &T {
        self.head += 1;
        &self.list[self.head - 1]
    }

    fn peek(&self) -> &T {
        &self.list[self.head]
    }
}

impl<D: Ord + Columnation + 'static, T: Ord + timely::PartialOrder + Clone + Columnation + 'static, R: Default + Semigroup + Columnation + 'static> MergerChunk for TimelyStack<(D, T, R)> {
    type TimeOwned = T;
    type DiffOwned = R;

    fn time_kept((_, time, _): &Self::Item<'_>, upper: &AntichainRef<Self::TimeOwned>, frontier: &mut Antichain<Self::TimeOwned>) -> bool {
        if upper.less_equal(time) {
            frontier.insert_with(&time, |time| time.clone());
            true
        }
        else { false }
    }
    fn push_and_add<'a>(&mut self, item1: Self::Item<'a>, item2: Self::Item<'a>, stash: &mut Self::DiffOwned) {
        let (data, time, diff1) = item1;
        let (_data, _time, diff2) = item2;
        stash.clone_from(diff1);
        stash.plus_equals(&diff2);
        if !stash.is_zero() {
            self.copy_destructured(data, time, stash);
        }
    }
    fn account(&self) -> (usize, usize, usize, usize) {
        let (mut size, mut capacity, mut allocations) = (0, 0, 0);
        let cb = |siz, cap| {
            size += siz;
            capacity += cap;
            allocations += 1;
        };
        self.heap_size(cb);
        (self.len(), size, capacity, allocations)
    }
}

impl<K,KBC,V,VBC,T,R> BuilderInput<KBC, VBC> for TimelyStack<((K, V), T, R)>
where
    K: Ord + Columnation + Clone + 'static,
    KBC: BatchContainer,
    for<'a> KBC::Borrowed<'a>: BatchIndex<Owned=K>,
    V: Ord + Columnation + Clone + 'static,
    VBC: BatchContainer,
    for<'a> VBC::Borrowed<'a>: BatchIndex<Owned=V>,
    T: Timestamp + Lattice + Columnation + Clone + 'static,
    R: Ord + Clone + Semigroup + Columnation + 'static,
{
    type Key<'a> = &'a K;
    type Val<'a> = &'a V;
    type Time = T;
    type Diff = R;

    fn into_parts<'a>(((key, val), time, diff): Self::Item<'a>) -> (Self::Key<'a>, Self::Val<'a>, Self::Time, Self::Diff) {
        (key, val, time.clone(), diff.clone())
    }

    fn key_eq(this: &Self::Key<'_>, other: <KBC::Borrowed<'_> as BatchIndex>::Ref) -> bool {
        KBC::Borrowed::eq(other, *this)
    }

    fn val_eq(this: &Self::Val<'_>, other: <VBC::Borrowed<'_> as BatchIndex>::Ref) -> bool {
        VBC::Borrowed::eq(other, *this)
    }

    fn key_val_upd_counts(chain: &[Self]) -> (usize, usize, usize) {
        let mut keys = 0;
        let mut vals = 0;
        let mut upds = 0;
        let mut prev_keyval = None;
        for link in chain.iter() {
            for ((key, val), _, _) in link.iter() {
                if let Some((p_key, p_val)) = prev_keyval {
                    if p_key != key {
                        keys += 1;
                        vals += 1;
                    } else if p_val != val {
                        vals += 1;
                    }
                } else {
                    keys += 1;
                    vals += 1;
                }
                upds += 1;
                prev_keyval = Some((key, val));
            }
        }
        (keys, vals, upds)
    }
}
