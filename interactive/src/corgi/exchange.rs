//! The corgi exchange: how a [`CorgiContainer`] is split across workers.
//!
//! An arrangement is only correct if every update for a key reaches the same worker, and a join is
//! only correct if both of its arrangements agree on which worker that is. [`CorgiPact`] is the
//! contract that establishes both — the corgi backend's `arrange` uses it in place of `Pipeline`,
//! and that single substitution is what lifts the backend off one worker.
//!
//! # Partitioning a column, not a stream of rows
//!
//! Timely's stock distributor ([`DrainContainerDistributor`]) drains a container item by item and
//! pushes each into a per-destination builder. A corgi container has no items to drain: it is four
//! columns, and taking it apart row-wise would undo the representation before the data even left
//! the worker.
//!
//! So [`CorgiDistributor`] partitions the way a columnar engine does — a counting sort:
//!
//! 1. Hash the key column once, one pass, columnar ([`corgi::hash`]).
//! 2. Count how many rows each destination takes, then place row indices into one `order` buffer
//!    so each destination's rows are a contiguous run of it.
//! 3. Per destination, `gather` the key and value columns through its run.
//!
//! The output of step 3 is a freshly allocated, contiguous column per destination — which is
//! exactly what the [wire format](crate::corgi::bytes) wants to write, one `memcpy` per leaf. The
//! sort is stable, so rows keep their relative order within a destination.
//!
//! # The hash
//!
//! Routing uses [`corgi::hash`] — the same structural content hash that
//! [`present_key`](crate::corgi::chunk::present_key) prepends as a key's identifier lane. Any
//! deterministic function of the key would be correct here; choosing *this* one means the
//! distributor and the arrangement agree on what a key's identifier is, so the hash a receiver
//! recomputes is the one the sender routed by. (It is also seed-free and structural, so it agrees
//! across processes and across runs, and its low bits are mixed — a raw key column would route a
//! strided identifier space onto a fraction of the workers.)

use timely::communication::Push;
use timely::dataflow::channels::Message;
use timely::dataflow::channels::pact::{LogPuller, LogPusher, ParallelizationContract};
use timely::dataflow::channels::pushers::{Exchange, exchange::Distributor};
use timely::logging::TimelyLogger;
use timely::progress::{Stamp, Timestamp};
use timely::worker::Worker;

use corgi::arrange::gather;

use crate::corgi::container::CorgiContainer;

/// Partitions a [`CorgiContainer`] across workers by the structural hash of each row's key.
///
/// The index buffers are fields rather than locals because `partition` runs once per container for
/// the life of the dataflow; allocating three `Vec`s per batch is the kind of cost that shows up as
/// a flat tax on every exchange. (The one per-batch allocation left is the hash column itself,
/// which `corgi::hash` returns owned.)
pub struct CorgiDistributor<T, R> {
    marker: std::marker::PhantomData<(T, R)>,
    /// Row indices, grouped by destination: destination `p` owns `order[starts[p]..starts[p+1]]`.
    order: Vec<usize>,
    /// Running offsets into `order`, one per destination plus the total.
    starts: Vec<usize>,
    /// Write position within each destination's run, while `order` is being filled.
    cursor: Vec<usize>,
}

impl<T, R> Default for CorgiDistributor<T, R> {
    fn default() -> Self {
        CorgiDistributor { marker: std::marker::PhantomData, order: Vec::new(), starts: Vec::new(), cursor: Vec::new() }
    }
}

impl<T: Clone + 'static, R: Clone + 'static> CorgiDistributor<T, R> {
    /// Group row indices by destination, leaving each destination's rows as a contiguous,
    /// input-ordered run of `self.order` delimited by `self.starts`.
    ///
    /// A counting sort: one pass to count, one to place. `peers` is a power of two in the common
    /// case, where the modulus is a mask.
    fn counting_sort(&mut self, ids: &[u64], peers: usize) {
        self.starts.clear();
        self.starts.resize(peers + 1, 0);
        let dest = |h: u64| -> usize {
            if peers.is_power_of_two() { (h & (peers as u64 - 1)) as usize } else { (h % peers as u64) as usize }
        };
        for &h in ids {
            self.starts[dest(h) + 1] += 1;
        }
        for p in 0..peers {
            self.starts[p + 1] += self.starts[p];
        }
        // `cursor` walks each destination's run as rows are placed; `starts` is left intact.
        self.order.clear();
        self.order.resize(ids.len(), 0);
        self.cursor.clear();
        self.cursor.extend_from_slice(&self.starts[..peers]);
        for (row, &h) in ids.iter().enumerate() {
            let p = dest(h);
            self.order[self.cursor[p]] = row;
            self.cursor[p] += 1;
        }
    }
}

impl<T: Clone + 'static, R: Clone + 'static> Distributor<CorgiContainer<T, R>> for CorgiDistributor<T, R> {
    fn partition<Ts: Clone, P: Push<Message<Ts, CorgiContainer<T, R>>>>(
        &mut self,
        container: &mut CorgiContainer<T, R>,
        stamp: &Stamp<Ts>,
        pushers: &mut [P],
    ) {
        let rows = container.times.len();
        if rows == 0 {
            return;
        }
        let peers = pushers.len();

        let ids = corgi::hash(&container.keys).into_u64("corgi exchange: key hash").unwrap();
        self.counting_sort(&ids, peers);

        // Whole-container fast path. When every row shares a destination — a batch narrower than
        // the worker count, or data already partitioned upstream — the container moves as it
        // stands: no hash-ordered gather, no column copy, no per-row time clone.
        if let Some(p) = (0..peers).find(|&p| self.starts[p + 1] - self.starts[p] == rows) {
            Message::push_at(container, stamp.clone(), &mut pushers[p]);
            return;
        }

        for (p, pusher) in pushers.iter_mut().enumerate() {
            let idx = &self.order[self.starts[p]..self.starts[p + 1]];
            if idx.is_empty() {
                continue;
            }
            let mut part = CorgiContainer {
                keys: gather(&container.keys, idx),
                vals: gather(&container.vals, idx),
                times: idx.iter().map(|&i| container.times[i].clone()).collect(),
                diffs: idx.iter().map(|&i| container.diffs[i].clone()).collect(),
            };
            Message::push_at(&mut part, stamp.clone(), pusher);
        }
        // Every row of the input landed in exactly one output, so the record count timely tracks
        // is preserved. Clear the input rather than leaving it to be re-sent.
        *container = CorgiContainer::default();
    }

    /// Nothing is held back: `partition` ships each destination's rows immediately, so there is
    /// never a partial container waiting on a flush. Batching across containers would mean
    /// holding key columns per destination and concatenating them, which is the batcher's job one
    /// operator downstream — doing it here as well would just move the copy.
    fn flush<Ts: Clone, P: Push<Message<Ts, CorgiContainer<T, R>>>>(&mut self, _stamp: &Stamp<Ts>, _pushers: &mut [P]) {}

    fn relax(&mut self) {
        self.order = Vec::new();
        self.starts = Vec::new();
        self.cursor = Vec::new();
    }
}

/// The parallelization contract that shuffles [`CorgiContainer`]s by key hash.
///
/// Substituting this for `Pipeline` in the corgi backend's `arrange` is what makes the backend
/// multi-worker: it is the only place the backend's data has to move between workers, because
/// every other operator (`linear`, `join`, `reduce`, `as_collection`) is key-local given
/// correctly placed arrangements.
pub struct CorgiPact;

impl<Time, T, R> ParallelizationContract<Time, CorgiContainer<T, R>> for CorgiPact
where
    Time: Timestamp,
    T: Clone + Send + 'static,
    R: Clone + Send + 'static,
    CorgiContainer<T, R>: timely::dataflow::channels::ContainerBytes,
{
    type Pusher = Exchange<
        Time,
        LogPusher<Box<dyn Push<Message<Time, CorgiContainer<T, R>>>>>,
        CorgiDistributor<T, R>,
    >;
    type Puller = LogPuller<Box<dyn timely::communication::Pull<Message<Time, CorgiContainer<T, R>>>>>;

    fn connect(self, worker: &Worker, identifier: usize, address: std::rc::Rc<[usize]>, logging: Option<TimelyLogger>) -> (Self::Pusher, Self::Puller) {
        let (senders, receiver) = worker.allocate::<Message<Time, CorgiContainer<T, R>>>(identifier, address);
        let senders = senders
            .into_iter()
            .enumerate()
            .map(|(i, x)| LogPusher::new(x, worker.index(), i, identifier, logging.clone()))
            .collect::<Vec<_>>();
        (Exchange::new(senders, CorgiDistributor::default()), LogPuller::new(receiver, worker.index(), identifier, logging))
    }
}

#[cfg(test)]
mod test {
    use timely::dataflow::channels::pushers::exchange::Distributor;

    use super::CorgiDistributor;
    use crate::corgi::container::CorgiContainer;
    use crate::ir::{Diff, Time, Value as DValue};

    /// A pusher that keeps what it is given, so a test can inspect the partition directly.
    #[derive(Default)]
    struct Collect<T, R>(Vec<CorgiContainer<T, R>>);

    impl<Ts, T: 'static, R: 'static> timely::communication::Push<timely::dataflow::channels::Message<Ts, CorgiContainer<T, R>>> for Collect<T, R> {
        fn push(&mut self, message: &mut Option<timely::dataflow::channels::Message<Ts, CorgiContainer<T, R>>>) {
            if let Some(message) = message.take() {
                self.0.push(message.data);
            }
        }
    }

    fn time() -> Time {
        use differential_dataflow::dynamic::pointstamp::PointStamp;
        timely::order::Product::new(0, PointStamp::new(Default::default()))
    }

    /// Partition `updates` across `peers` destinations and read each destination back as rows.
    fn partition(updates: Vec<((DValue, DValue), Time, Diff)>, peers: usize) -> Vec<Vec<((DValue, DValue), Time, Diff)>> {
        let mut container = CorgiContainer::<Time, Diff>::from_updates_pinned(updates);
        let mut pushers: Vec<Collect<Time, Diff>> = (0..peers).map(|_| Collect::default()).collect();
        let mut distributor = CorgiDistributor::<Time, Diff>::default();
        distributor.partition(&mut container, &timely::progress::Stamp::from_elem(0u64), &mut pushers);
        pushers
            .into_iter()
            .map(|p| p.0.into_iter().flat_map(|c| c.into_updates()).collect())
            .collect()
    }

    fn scalar_updates(n: i64) -> Vec<((DValue, DValue), Time, Diff)> {
        (0..n).map(|i| ((DValue::Int(i), DValue::Int(i * 10)), time(), i)).collect()
    }

    /// Nothing is lost and nothing is duplicated: the partition is a partition.
    #[test]
    fn partition_preserves_every_update() {
        let updates = scalar_updates(200);
        for peers in [2, 3, 4, 8] {
            let parts = partition(updates.clone(), peers);
            let mut got: Vec<_> = parts.into_iter().flatten().collect();
            got.sort();
            let mut want = updates.clone();
            want.sort();
            assert_eq!(got, want, "peers = {peers}");
        }
    }

    /// The property arrangements depend on: a key's updates all land on one worker, whatever
    /// value, time, or diff they carry, and whichever batch they arrived in.
    #[test]
    fn a_key_goes_to_one_destination() {
        // Three updates per key, differing in value and diff, so a key that routed by anything
        // other than its key would scatter.
        let updates: Vec<_> = (0..100i64)
            .flat_map(|k| (0..3i64).map(move |v| ((DValue::Tuple(vec![DValue::Int(k), DValue::Int(k / 7)]), DValue::Int(v)), time(), v + 1)))
            .collect();
        for peers in [2, 3, 5, 8] {
            let parts = partition(updates.clone(), peers);
            let mut seen: std::collections::HashMap<DValue, usize> = Default::default();
            for (p, part) in parts.iter().enumerate() {
                for ((k, _), _, _) in part {
                    if let Some(&q) = seen.get(k) {
                        assert_eq!(q, p, "key {k:?} landed on both {q} and {p} at peers = {peers}");
                    } else {
                        seen.insert(k.clone(), p);
                    }
                }
            }
            assert_eq!(seen.len(), 100, "peers = {peers}");
        }
    }

    /// Two containers holding the same key route it to the same place — the property that makes
    /// a join correct, since its two inputs are separate streams.
    #[test]
    fn routing_is_independent_of_the_batch() {
        let keys: Vec<DValue> = (0..64i64).map(|k| DValue::Tuple(vec![DValue::Int(k), DValue::Int(-k)])).collect();
        let left: Vec<_> = keys.iter().map(|k| ((k.clone(), DValue::Int(1)), time(), 1)).collect();
        // The same keys, in a different order, with different values: only the key may matter.
        let right: Vec<_> = keys.iter().rev().map(|k| ((k.clone(), DValue::Int(2)), time(), 5)).collect();
        for peers in [2, 4, 7] {
            let (lp, rp) = (partition(left.clone(), peers), partition(right.clone(), peers));
            let dest = |parts: &Vec<Vec<((DValue, DValue), Time, Diff)>>| -> std::collections::HashMap<DValue, usize> {
                parts.iter().enumerate().flat_map(|(p, part)| part.iter().map(move |((k, _), _, _)| (k.clone(), p))).collect()
            };
            assert_eq!(dest(&lp), dest(&rp), "peers = {peers}");
        }
    }

    /// Hashing, not the key's own bits: a strided identifier space (every key a multiple of the
    /// worker count) must not collapse onto one worker, which is what routing by the raw key
    /// would do.
    #[test]
    fn strided_keys_still_spread() {
        let updates: Vec<_> = (0..400i64).map(|i| ((DValue::Int(i * 8), DValue::Int(i)), time(), 1)).collect();
        let parts = partition(updates, 8);
        for (p, part) in parts.iter().enumerate() {
            assert!(!part.is_empty(), "destination {p} got nothing from 400 strided keys");
        }
    }

    /// A container whose rows all share a destination moves whole, and the input is left empty
    /// either way so no row is sent twice.
    #[test]
    fn the_input_is_consumed() {
        let mut container = CorgiContainer::<Time, Diff>::from_updates_pinned(scalar_updates(50));
        let mut pushers: Vec<Collect<Time, Diff>> = (0..4).map(|_| Collect::default()).collect();
        let mut distributor = CorgiDistributor::<Time, Diff>::default();
        distributor.partition(&mut container, &timely::progress::Stamp::from_elem(0u64), &mut pushers);
        assert_eq!(container.times.len(), 0, "partition left rows in the input container");
        let shipped: usize = pushers.iter().map(|p| p.0.iter().map(|c| c.times.len()).sum::<usize>()).sum();
        assert_eq!(shipped, 50, "record count was not preserved");
    }

    /// An empty container is a no-op, not a panic or a stream of empty messages.
    #[test]
    fn empty_containers_ship_nothing() {
        let mut container = CorgiContainer::<Time, Diff>::default();
        let mut pushers: Vec<Collect<Time, Diff>> = (0..4).map(|_| Collect::default()).collect();
        let mut distributor = CorgiDistributor::<Time, Diff>::default();
        distributor.partition(&mut container, &timely::progress::Stamp::from_elem(0u64), &mut pushers);
        assert!(pushers.iter().all(|p| p.0.is_empty()));
    }
}
