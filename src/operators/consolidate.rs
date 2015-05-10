use timely::example_shared::*;
use timely::example_shared::operators::*;
use timely::communication::*;
use timely::communication::pact::Exchange;

use sort::*;
use collection_trace::Lookup;
use collection_trace::lookup::UnsignedInt;

use columnar::Columnar;

pub trait ConsolidateExt<D> {
    fn consolidate<U: UnsignedInt,
                   F: Fn(&D)->U+'static,
                   Q: Fn()->F>(self, part: Q) -> Self;
}

impl<G: GraphBuilder, D: Ord+Data+Columnar> ConsolidateExt<D> for Stream<G, (D, i32)> {
    fn consolidate<U: UnsignedInt,
                   F: Fn(&D)->U+'static,
                   Q: Fn()->F>(self, part: Q) -> Self {

        let mut inputs = Vec::new();    // Vec<(G::Timestamp, Vec<(D, i32))>

        let part1 = part();             // we may want a second one later on.
        // let part2 = part();             // we may want a second one later on.

        let exch = Exchange::new(move |&(ref x,_)| part1(x).as_usize() as u64);
        self.unary_notify(exch, format!("Consolidate"), vec![], move |input, output, notificator| {

            // 1. read each input, and stash it in our staging area
            while let Some((index, mut data)) = input.pull() {
                notificator.notify_at(&index);
                inputs.entry_or_insert(index.clone(), || Vec::new())
                      .extend(data.drain(..));
            }

            // 2. go through each time of interest that has reached completion
            while let Some((index, _count)) = notificator.next() {
                if let Some(mut stash) = inputs.remove_key(&index) {
                    // let len = stash.len();
                    coalesce(&mut stash);
                    // println!("consolidating at {:?}: {} -> {}", index, len, stash.len());
                    output.give_at(&index, stash.drain(..));
                }
            }
        })
    }
}
