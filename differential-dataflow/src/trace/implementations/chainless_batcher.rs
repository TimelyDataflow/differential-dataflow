//! A `Batcher` implementation based on merge sort.

use timely::progress::frontier::AntichainRef;
use timely::progress::{frontier::Antichain, Timestamp};

use crate::logging::Logger;
use crate::trace;

/// A type that can be used as storage within a merge batcher.
pub trait BatcherStorage<T: Timestamp> : Default + Sized {
    /// Number of contained updates.
    fn len(&self) -> usize;
    /// Merges two storage containers into one.
    ///
    /// This is expected to consolidate updates as it goes.
    fn merge(self, other: Self) -> Self;
    /// Extracts elements not greater or equal to the frontier.
    fn split(&mut self, frontier: AntichainRef<T>) -> Self;
    /// Ensures `frontier` is less or equal to all contained times.
    ///
    /// Consider merging with `split`, but needed for new stores as well.
    fn lower(&self, frontier: &mut Antichain<T>);
}

/// A batcher that simple merges `BatcherStorage` implementors.
pub struct Batcher<T: Timestamp, S: BatcherStorage<T>> {
    /// Each store is at least twice the size of the next.
    storages: Vec<S>,
    /// The lower bound of timestamps of the maintained updates.
    lower: Antichain<T>,
    /// The previosly minted frontier.
    prior: Antichain<T>,

    /// Logger for size accounting.
    _logger: Option<Logger>,
    /// Timely operator ID.
    _operator_id: usize,
}

impl<T: Timestamp, S: BatcherStorage<T>> Batcher<T, S> {
    /// Ensures lists decrease in size geometrically.
    fn tidy(&mut self) {
        self.storages.retain(|x| x.len() > 0);
        self.storages.sort_by_key(|x| x.len());
        self.storages.reverse();
        while let Some(pos) = (1..self.storages.len()).position(|i| self.storages[i-1].len() < 2 * self.storages[i].len()) {
            while self.storages.len() > pos + 1 {
                let x = self.storages.pop().unwrap();
                let y = self.storages.pop().unwrap();
                self.storages.push(x.merge(y));
                self.storages.sort_by_key(|x| x.len());
                self.storages.reverse();
            }
        }
    }
}

impl<T: Timestamp, S: BatcherStorage<T>> trace::Batcher for Batcher<T, S> {
    type Time = T;
    type Input = S;
    type Output = S;

    fn new(logger: Option<Logger>, operator_id: usize) -> Self {
        Self {
            storages: Vec::default(),
            lower: Default::default(),
            prior: Antichain::from_elem(T::minimum()),
            _logger: logger,
            _operator_id: operator_id,
        }
    }

    fn push_container(&mut self, batch: &mut Self::Input) {
        if batch.len() > 0 {
            // TODO: This appears to be optional based on `frontier` only being called after `seal`.
            //       For the moment, the trait doesn't promise this, but keep eyes on the cost.
            batch.lower(&mut self.lower);
            self.storages.push(std::mem::take(batch));
            self.tidy();
        }
    }

    fn seal<B: trace::Builder<Input=Self::Output, Time=Self::Time>>(&mut self, upper: Antichain<Self::Time>) -> B::Output {
        let description = trace::Description::new(self.prior.clone(), upper.clone(), Antichain::new());
        self.prior = upper.clone();
        let mut stores = self.storages.iter_mut().rev();
        if let Some(store) = stores.next() {
            self.lower.clear();
            let mut ship = store.split(upper.borrow());
            for store in stores {
                let split = store.split(upper.borrow());
                ship = ship.merge(split);
            }
            self.tidy();
            store.lower(&mut self.lower);
            B::seal(&mut vec![ship], description)
        }
        else {
            B::seal(&mut vec![], description)
        }
    }

    fn frontier(&mut self) -> AntichainRef<'_, Self::Time> { self.lower.borrow() }
}
