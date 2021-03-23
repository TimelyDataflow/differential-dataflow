extern crate timely;
extern crate graph_map;
extern crate differential_dataflow;

#[macro_use]
extern crate abomonation_derive;
extern crate abomonation;

extern crate dogsdogsdogs;

use timely::dataflow::operators::probe::Handle;
use timely::dataflow::operators::UnorderedInput;
use timely::dataflow::operators::Map;
use differential_dataflow::AsCollection;

fn main() {

    use pair::Pair;

    timely::execute_from_args(std::env::args().skip(2), move |worker| {

        let mut probe = Handle::new();

        let (mut i1, mut i2, c1, c2) = worker.dataflow::<Pair<usize, usize>,_,_>(|scope| {

            use timely::dataflow::operators::unordered_input::UnorderedHandle;

            let ((input1, capability1), data1): ((UnorderedHandle<Pair<usize, usize>, ((usize, usize), Pair<usize, usize>, isize)>, _), _) = scope.new_unordered_input();
            let ((input2, capability2), data2): ((UnorderedHandle<Pair<usize, usize>, ((usize, usize), Pair<usize, usize>, isize)>, _), _) = scope.new_unordered_input();

            let edges1 = data1.as_collection();
            let edges2 = data2.as_collection();

            // Graph oriented both ways, indexed by key.
            use differential_dataflow::operators::arrange::ArrangeByKey;
            let forward1 = edges1.arrange_by_key();
            let forward2 = edges2.arrange_by_key();

            // Grab the stream of changes. Stash the initial time as payload.
            let changes1 = edges1.inner.map(|((k,v),t,r)| ((k,v,t.clone()),t,r)).as_collection();
            let changes2 = edges2.inner.map(|((k,v),t,r)| ((k,v,t.clone()),t,r)).as_collection();

            use dogsdogsdogs::operators::half_join;

            // pick a frontier that will not mislead TOTAL ORDER comparisons.
            let closure = |time: &Pair<usize, usize>| Pair::new(time.first.saturating_sub(1), time.second.saturating_sub(1));

            let path1 =
            half_join(
                &changes1,
                forward2,
                closure.clone(),
                |t1,t2| t1.lt(t2),  // This one ignores concurrent updates.
                |key, val1, val2| (key.clone(), (val1.clone(), val2.clone())),
            );

            let path2 =
            half_join(
                &changes2,
                forward1,
                closure.clone(),
                |t1,t2| t1.le(t2),  // This one can "see" concurrent updates.
                |key, val1, val2| (key.clone(), (val2.clone(), val1.clone())),
            );

            // Delay updates until the worked payload time.
            // This should be at least the ignored update time.
            path1.concat(&path2)
                .inner.map(|(((k,v),t),_,r)| ((k,v),t,r)).as_collection()
                .inspect(|x| println!("{:?}", x))
                .probe_with(&mut probe);

            (input1, input2, capability1, capability2)
        });

        i1
            .session(c1.clone())
            .give(((5, 6), Pair::new(0, 13), 1));

        i2
            .session(c2.clone())
            .give(((5, 7), Pair::new(11, 0), 1));

    }).unwrap();
}


/// This module contains a definition of a new timestamp time, a "pair" or product.
///
/// This is a minimal self-contained implementation, in that it doesn't borrow anything
/// from the rest of the library other than the traits it needs to implement. With this
/// type and its implementations, you can use it as a timestamp type.
mod pair {

    /// A pair of timestamps, partially ordered by the product order.
    #[derive(Hash, Default, Clone, Eq, PartialEq, Ord, PartialOrd, Abomonation)]
    pub struct Pair<S, T> {
        pub first: S,
        pub second: T,
    }

    impl<S, T> Pair<S, T> {
        /// Create a new pair.
        pub fn new(first: S, second: T) -> Self {
            Pair { first, second }
        }
    }

    // Implement timely dataflow's `PartialOrder` trait.
    use timely::order::PartialOrder;
    impl<S: PartialOrder, T: PartialOrder> PartialOrder for Pair<S, T> {
        fn less_equal(&self, other: &Self) -> bool {
            self.first.less_equal(&other.first) && self.second.less_equal(&other.second)
        }
    }

    use timely::progress::timestamp::Refines;
    impl<S: Timestamp, T: Timestamp> Refines<()> for Pair<S, T> {
        fn to_inner(_outer: ()) -> Self { Self::minimum() }
        fn to_outer(self) -> () { () }
        fn summarize(_summary: <Self>::Summary) -> () { () }
    }

    // Implement timely dataflow's `PathSummary` trait.
    // This is preparation for the `Timestamp` implementation below.
    use timely::progress::PathSummary;

    impl<S: Timestamp, T: Timestamp> PathSummary<Pair<S,T>> for () {
        fn results_in(&self, timestamp: &Pair<S, T>) -> Option<Pair<S,T>> {
            Some(timestamp.clone())
        }
        fn followed_by(&self, other: &Self) -> Option<Self> {
            Some(other.clone())
        }
    }

    // Implement timely dataflow's `Timestamp` trait.
    use timely::progress::Timestamp;
    impl<S: Timestamp, T: Timestamp> Timestamp for Pair<S, T> {
        fn minimum() -> Self { Pair { first: S::minimum(), second: T::minimum() }}
        type Summary = ();
    }

    // Implement differential dataflow's `Lattice` trait.
    // This extends the `PartialOrder` implementation with additional structure.
    use differential_dataflow::lattice::Lattice;
    impl<S: Lattice, T: Lattice> Lattice for Pair<S, T> {
        fn join(&self, other: &Self) -> Self {
            Pair {
                first: self.first.join(&other.first),
                second: self.second.join(&other.second),
            }
        }
        fn meet(&self, other: &Self) -> Self {
            Pair {
                first: self.first.meet(&other.first),
                second: self.second.meet(&other.second),
            }
        }
    }

    use std::fmt::{Formatter, Error, Debug};

    /// Debug implementation to avoid seeing fully qualified path names.
    impl<TOuter: Debug, TInner: Debug> Debug for Pair<TOuter, TInner> {
        fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
            f.write_str(&format!("({:?}, {:?})", self.first, self.second))
        }
    }

}
