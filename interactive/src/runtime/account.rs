//! Accounts of what DDIR's chunks hold, logged by the chunks themselves.
//!
//! The contents of every [`CorgiChunk`](crate::runtime::chunk::CorgiChunk) carry a [`Ticket`].
//! Made with the contents, it logs `+(updates, bytes)` against the account current at that moment
//! and remembers that account; when the contents drop, it logs the same amounts back against it.
//! Clones of a chunk share its contents and its ticket, and log nothing. So no code that holds
//! chunks reports them: a chunk is counted wherever it is, in a batcher, a merge in progress, a
//! trace, or a batch a reader still holds.
//!
//! An account is an operator's. One is current at a time on each worker: an operator enters its
//! own around the work that makes chunks ([`enter`]), and a chunk made from chunks (merging,
//! advancing, settling, extracting) is made under its inputs' account, wherever that happens. A
//! chunk made with no operator's account current is logged against the worker's unattributed
//! account, operator [`UNATTRIBUTED`]. A worker that has no logger for [`STREAM`] has no accounts,
//! and its tickets log nothing and measure nothing.

use std::cell::RefCell;
use std::time::Duration;

use timely::progress::frontier::AntichainRef;
use timely::worker::Worker;

use differential_dataflow::trace::Batcher;

/// The logging stream of chunk events.
pub const STREAM: &str = "ddir/chunks";

/// The operator of a worker's unattributed account.
pub const UNATTRIBUTED: usize = usize::MAX;

/// A change in what an account's chunks hold: signed, `+` when contents are made and `-` when
/// they drop.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChunkEvent {
    /// The operator whose account it is, or [`UNATTRIBUTED`].
    pub operator: usize,
    /// The change in updates.
    pub updates: i64,
    /// The change in bytes: key and value columns, time lanes and diffs, by stored length.
    pub bytes: i64,
}

/// The container builder of the [`STREAM`] logger.
pub type ChunkEventBuilder = timely::container::CapacityContainerBuilder<Vec<(Duration, ChunkEvent)>>;

/// The [`STREAM`] logger.
pub(crate) type ChunkLogger = timely::logging_core::Logger<ChunkEventBuilder>;

/// The worker's [`STREAM`] logger, if one is registered.
pub(crate) fn logger(worker: &Worker) -> Option<ChunkLogger> {
    worker.logger_for::<ChunkEventBuilder>(STREAM)
}

/// An operator's account: what chunks made under it hold is logged against the operator.
#[derive(Clone)]
pub(crate) struct Account {
    operator: usize,
    logger: ChunkLogger,
}

impl Account {
    pub(crate) fn new(operator: usize, logger: ChunkLogger) -> Self {
        Account { operator, logger }
    }

    fn log(&self, updates: i64, bytes: i64) {
        self.logger.log(ChunkEvent { operator: self.operator, updates, bytes });
    }
}

thread_local! {
    /// The account chunks made now are logged against: an operator's while it is entered, and
    /// otherwise the worker's unattributed account, once [`start`] has made one.
    static CURRENT: RefCell<Option<Account>> = const { RefCell::new(None) };
}

/// Make the worker's unattributed account current, for chunks made while no operator's account
/// is. Called once the worker registers a logger for [`STREAM`]; without one, nothing is counted.
pub fn start(worker: &Worker) {
    let account = logger(worker).map(|logger| Account::new(UNATTRIBUTED, logger));
    CURRENT.with(|current| *current.borrow_mut() = account);
}

/// Make `account` current until the returned guard drops, which makes the previous one current
/// again. `None` leaves the current account current.
pub(crate) fn enter(account: Option<&Account>) -> Entered {
    Entered { previous: account.map(|a| CURRENT.with(|current| current.replace(Some(a.clone())))) }
}

/// The guard of an entered account.
pub(crate) struct Entered {
    previous: Option<Option<Account>>,
}

impl Drop for Entered {
    fn drop(&mut self) {
        if let Some(previous) = self.previous.take() {
            CURRENT.with(|current| *current.borrow_mut() = previous);
        }
    }
}

/// What one chunk's contents hold, counted in an account while they live.
pub(crate) struct Ticket {
    account: Option<Account>,
    updates: i64,
    bytes: i64,
}

impl Ticket {
    /// A ticket for contents of `updates` updates, logged against the current account. `bytes`
    /// measures them, and is called only when there is an account and something to count.
    pub(crate) fn new(updates: usize, bytes: impl FnOnce() -> usize) -> Self {
        if updates == 0 { return Self::none(); }
        match CURRENT.with(|current| current.borrow().clone()) {
            Some(account) => {
                let (updates, bytes) = (updates as i64, bytes() as i64);
                account.log(updates, bytes);
                Ticket { account: Some(account), updates, bytes }
            }
            None => Self::none(),
        }
    }

    /// A ticket that counts nothing, for contents that hold nothing.
    pub(crate) fn none() -> Self {
        Ticket { account: None, updates: 0, bytes: 0 }
    }

    /// The account these contents are counted in, under which chunks made from them are made.
    pub(crate) fn account(&self) -> Option<&Account> {
        self.account.as_ref()
    }
}

/// A copy of the contents is counted again, in the same account.
impl Clone for Ticket {
    fn clone(&self) -> Self {
        if let Some(account) = &self.account { account.log(self.updates, self.bytes); }
        Ticket { account: self.account.clone(), updates: self.updates, bytes: self.bytes }
    }
}

impl Drop for Ticket {
    fn drop(&mut self) {
        if let Some(account) = &self.account { account.log(-self.updates, -self.bytes); }
    }
}

/// A batcher that makes its chunks under its operator's account: what `arrange` hands
/// `arrange_core`, so that the chunks its chunker forms from containers and its merges make are
/// counted as the arranging operator's.
pub(crate) struct AccountedBatcher<B> {
    batcher: B,
    account: Option<Account>,
}

impl<B> AccountedBatcher<B> {
    pub(crate) fn new(batcher: B, account: Option<Account>) -> Self {
        AccountedBatcher { batcher, account }
    }
}

impl<C, B: Batcher<C>> Batcher<C> for AccountedBatcher<B> {
    type Time = B::Time;
    type Output = B::Output;

    fn insert(&mut self, container: &mut C) {
        let _account = enter(self.account.as_ref());
        self.batcher.insert(container);
    }

    fn extract<'a>(&'a mut self, upper: AntichainRef<'_, B::Time>) -> (Option<B::Output>, AntichainRef<'a, B::Time>) {
        let _account = enter(self.account.as_ref());
        self.batcher.extract(upper)
    }
}

#[cfg(test)]
mod test {
    use std::cell::RefCell;
    use std::collections::{HashMap, VecDeque};
    use std::rc::Rc;

    use corgi::Value as CValue;
    use differential_dataflow::trace::chunk::merge_chains;
    use timely::worker::Worker;

    use crate::runtime::chunk::CorgiChunk;
    use super::{enter, logger, start, Account, ChunkEventBuilder, STREAM, UNATTRIBUTED};

    type Held = Rc<RefCell<HashMap<usize, (i64, i64)>>>;

    /// What each account holds, once the logger has delivered everything logged so far.
    fn held(worker: &Worker, held: &Held) -> HashMap<usize, (i64, i64)> {
        worker.log_register().unwrap().flush();
        held.borrow().iter().filter(|(_, h)| **h != (0, 0)).map(|(op, h)| (*op, *h)).collect()
    }

    fn chunk(keys: std::ops::Range<i64>) -> CorgiChunk<u64, i64> {
        let n = (keys.end - keys.start) as usize;
        CorgiChunk::from_columns(CValue::i64(keys.collect()), CValue::Unit(n), (0..n).map(|_| 0).collect(), vec![1; n])
    }

    /// A merge's output is counted in its inputs' account, and an input a reader still holds
    /// once the merge has consumed it stays counted until the reader drops it.
    #[test]
    fn kept_chunks_stay_counted() {
        timely::execute_directly(|worker| {
            let counts: Held = Default::default();
            let sink = Rc::clone(&counts);
            worker.log_register().unwrap().insert::<ChunkEventBuilder, _>(STREAM, move |_time, data| {
                if let Some(events) = data {
                    for (_, event) in events.drain(..) {
                        let held = &mut *sink.borrow_mut();
                        let account = held.entry(event.operator).or_default();
                        account.0 += event.updates;
                        account.1 += event.bytes;
                    }
                }
            });
            start(worker);
            let (a, b) = {
                let _account = enter(Some(&Account::new(7, logger(worker).unwrap())));
                (chunk(0..4), chunk(2..6))
            };
            let reader = a.clone();
            let held_a = held(worker, &counts)[&7];
            assert_eq!(held_a.0, 8, "two chunks of four updates");

            let mut merged = VecDeque::new();
            merge_chains(vec![a], vec![b], &mut merged);
            let with_reader = held(worker, &counts);
            assert_eq!(with_reader.keys().collect::<Vec<_>>(), [&7], "{with_reader:?}");
            assert_eq!(with_reader[&7].0, 6 + 4, "the merge's six updates and the four the reader keeps");

            drop(reader);
            let without = held(worker, &counts);
            assert_eq!(without[&7].0, 6);
            assert!(without[&7].1 < with_reader[&7].1);

            drop(merged);
            assert!(held(worker, &counts).is_empty(), "nothing left, unattributed included ({UNATTRIBUTED})");
        });
    }
}
