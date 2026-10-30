//! One numbered root frontier sequence, and each half join's retained portion of it.

use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::{BTreeMap, VecDeque};
use std::rc::Rc;

use timely::dataflow::{Scope, Stream};
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::vec::Broadcast;
use timely::dataflow::operators::generic::{builder_rc::OperatorBuilder, OutputBuilder};
use timely::progress::{Antichain, Timestamp};
use timely::progress::frontier::AntichainRef;
use timely::PartialOrder;

use crate::backend::corgi::CorgiImport;
use crate::ir::Time;

pub(crate) type Step = (u64, Vec<Time>);
pub(crate) type StepStream<'s> = Stream<'s, Time, Vec<Step>>;
pub(crate) type SharedSteps = Rc<RefCell<Steps>>;

/// Observe the union of the root import frontiers. Timely supplies global input frontiers;
/// only worker zero mints steps, so scheduling on other workers cannot change the order.
/// Data is drained, never held or tagged. The control stream stays at minimum until the
/// boundary closes; its consumers promise no control-to-data progress path.
pub(crate) fn boundary<'s>(scope: Scope<'s, Time>, imports: &[CorgiImport<'s>]) -> StepStream<'s> {
    let mut builder = OperatorBuilder::new("HalfJoinBoundary".into(), scope);
    let mut drain: Vec<Box<dyn FnMut()>> = Vec::new();
    for import in imports {
        match import {
            CorgiImport::Columns(c) => {
                let mut input = builder.new_input(c.inner.clone(), Pipeline);
                drain.push(Box::new(move || input.for_each(|_, _| {})));
            }
            CorgiImport::Export(a) => {
                let mut input = builder.new_input(a.arranged.stream.clone(), Pipeline);
                drain.push(Box::new(move || input.for_each(|_, _| {})));
            }
        }
    }
    let (output, stream) = builder.new_output_connection([]);
    let mut output = OutputBuilder::<_, timely::container::CapacityContainerBuilder<Vec<Step>>>::from(output);
    let minter = scope.index() == 0;
    builder.build(move |mut caps| {
        let mut cap = minter.then(|| caps.pop().unwrap());
        let mut previous = None;
        let mut next = 0;
        move |frontiers| {
            for drain in &mut drain { drain(); }
            if let Some(c) = &cap {
                let frontier: Antichain<Time> = frontiers.iter().flat_map(|f| f.frontier().to_vec()).collect();
                if previous.as_ref() != Some(&frontier) {
                    output.activate().session(c).give((next, frontier.elements().to_vec()));
                    next += 1;
                    previous = Some(frontier.clone());
                }
                if frontier.is_empty() { cap = None; }
            }
        }
    });
    stream.broadcast()
}

#[derive(Default)]
pub(crate) struct Steps {
    next: u64,
    waiting: BTreeMap<u64, Vec<Time>>,
    /// One collapsed prefix boundary plus all later steps. Its floor advances with the
    /// compaction policy below, even when a fixed coordinate prevents predecessor(F) from
    /// dominating old boundaries. Deferred work can hold this floor back.
    history: VecDeque<(u64, Antichain<Time>)>,
    retired: bool,
}

impl Steps {
    /// Consume only a contiguous numbered prefix, regardless of delivery order.
    pub(crate) fn receive(&mut self, messages: &mut Vec<Step>) {
        if self.retired { messages.clear(); return; }
        for (number, frontier) in messages.drain(..) {
            assert!(number >= self.next && self.waiting.insert(number, frontier).is_none(), "duplicate half-join step");
        }
        while let Some(frontier) = self.waiting.remove(&self.next) {
            let frontier: Antichain<Time> = frontier.into_iter().collect();
            if let Some((_, last)) = self.history.back() { assert!(PartialOrder::less_equal(last, &frontier)); }
            self.history.push_back((self.next, frontier));
            self.next += 1;
        }
    }

    fn step(&self, time: &Time) -> u64 {
        // Frontier membership only changes from true to false as the sequence advances.
        let index = self.history.partition_point(|(_, f)| f.less_equal(time));
        self.history.get(index).map_or(self.next, |(i, _)| *i)
    }

    pub(crate) fn compare(&self, a: &Time, b: &Time) -> Ordering {
        self.step(a).cmp(&self.step(b)).then_with(|| a.cmp(b))
    }

    /// All times passed by this boundary are known, and complete in the arrangement.
    /// F <= upper means every time the arrangement can still gain lies beyond F.
    pub(crate) fn eligible(&self, upper: AntichainRef<'_, Time>) -> Option<Antichain<Time>> {
        self.history.iter().rev().find(|(_, f)| PartialOrder::less_equal(&f.borrow(), &upper)).map(|(_, f)| f.clone())
    }

    /// Retained boundaries not yet examined by an unassigned batch. A late arrival starts
    /// at zero; compaction has retained every boundary it can still need.
    pub(crate) fn from(&self, next: u64) -> impl Iterator<Item = (u64, AntichainRef<'_, Time>)> {
        self.history.iter().skip(self.history.partition_point(|(i, _)| *i < next))
            .map(|(i, f)| (*i, f.borrow()))
    }

    pub(crate) fn next(&self) -> u64 { self.next }

    /// The completed prefix of step numbers, without cloning a frontier.
    pub(crate) fn eligible_step(&self, upper: AntichainRef<'_, Time>) -> Option<u64> {
        let end = self.history.partition_point(|(_, f)| PartialOrder::less_equal(&f.borrow(), &upper));
        end.checked_sub(1).map(|i| self.history[i].0)
    }

    /// Choose a boundary F below every future, staged, or deferred stream time. Compacting
    /// by the coordinate-wise predecessors of F leaves times beyond F unchanged and keeps
    /// every time passed by F passed: for each f, predecessor(f) cannot fill a missing lane
    /// of f. Thus only times preceding ALL future work can change step, and their lattice
    /// joins with future data times are unchanged. The ordinary step_back(stream) rule alone
    /// would not preserve a progress-defined order on incomparable times.
    pub(crate) fn compaction(&mut self, future: AntichainRef<'_, Time>) -> Antichain<Time> {
        if future.is_empty() {
            self.retired = true;
            self.history.clear();
            self.waiting.clear();
            return Antichain::new();
        }
        let index = self.history.iter().rposition(|(_, f)| PartialOrder::less_equal(&f.borrow(), &future));
        if let Some(index) = index {
            // No future/deferred stream time is passed by F. Collapse ALL steps through F
            // into one, even if an unchanged nonzero coordinate means predecessor(F) never
            // dominates an older boundary. Comparing against the collapsed prefix always
            // says "earlier", both before and after arrangement compaction.
            self.history.drain(..index);
            self.history[0].1.iter().map(|t| Time::new(t.iter().map(|x| x.saturating_sub(1)).collect())).collect()
        } else {
            Antichain::from_elem(Time::minimum())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use differential_dataflow::lattice::Lattice;
    use crate::runtime::col_times::Lanes;

    /// Delivery gaps cannot close steps; reclamation must preserve pair selection and data
    /// times, including incomparable timestamps and a frontier with several elements.
    #[test]
    fn numbered_steps_and_compaction_preserve_future_pairs() {
        let t = |x, y| Time::from_coordinates([x, y].into_iter());
        let mut steps = Steps::default();
        steps.receive(&mut vec![(1, vec![t(0, 1)])]);
        assert!(steps.eligible(AntichainRef::new(&[])).is_none());
        steps.receive(&mut vec![(0, vec![t(0, 0)])]);
        assert_eq!(steps.compare(&t(2, 0), &t(0, 1)), Ordering::Less);
        assert_eq!(steps.eligible(AntichainRef::new(&[t(0, 0)])).unwrap(), Antichain::from_elem(t(0, 0)));

        // Repeated, non-principal frontiers with progress in either lane. Each compaction
        // can discard arbitrarily much old history while the tail remains exact.
        for round in 1..100 {
            let f: Antichain<_> = [t(round, round + 2), t(round + 2, round)].into_iter().collect();
            steps.receive(&mut vec![(steps.next, f.elements().to_vec())]);
            let future: Antichain<_> = [t(round, round + 1), t(round + 1, round)].into_iter().collect();
            let points: Vec<_> = (0..round + 4).flat_map(|x| (0..round + 4).map(move |y| t(x, y))).collect();
            // Sample old and recent arranged times against all recent future times.
            let arranged = [t(0, 0), t(0, round + 3), t(round + 3, 0), t(round - 1, round + 1), t(round, round), t(round + 1, round + 2)];
            let mut expected = Vec::new();
            for b in points.iter().filter(|b| future.less_equal(b)) {
                for a in &arranged { expected.push((a.clone(), b.clone(), steps.compare(a, b))); }
            }
            let compact = steps.compaction(future.borrow());
            assert!(steps.history.len() <= 4, "history grows with uncompacted steps, not lifetime");
            for (a, b, order) in expected {
                let mut advanced = a.clone();
                advanced.advance_by(compact.borrow());
                assert_eq!(steps.compare(&advanced, &b), order, "{a:?} vs {b:?}, compact {compact:?}");
                assert_eq!(a.join(&b), advanced.join(&b));
            }
        }
        // A nonzero, fixed first coordinate must not pin history forever.
        let mut fixed = Steps::default();
        for n in 0..1_000 {
            fixed.receive(&mut vec![(n, vec![t(100, n)])]);
            fixed.compaction(AntichainRef::new(&[t(100, n)]));
            assert_eq!(fixed.history.len(), 1);
        }
        assert!(steps.compaction(AntichainRef::new(&[])).is_empty());
        steps.receive(&mut vec![(steps.next, vec![])]);
        assert!(steps.history.is_empty());
    }
}
