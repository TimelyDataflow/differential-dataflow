use timely::example_shared::*;
use timely::example_shared::operators::*;
use timely::communication::*;
use timely::communication::pact::Exchange;

use sort::*;
use collection_trace::Lookup;
use collection_trace::lookup::UnsignedInt;

use columnar::Columnar;

use timely::drain::DrainExt;

pub trait ConsolidateExt<D> {
    fn consolidate<U: UnsignedInt,
                   F1: Fn(&D)->U+'static,
                   F2: Fn(&D)->U+'static>(self, part1: F1, part2: F2) -> Self;
}

impl<G: GraphBuilder, D: Ord+Data+Columnar> ConsolidateExt<D> for Stream<G, (D, i32)> {
    fn consolidate<U: UnsignedInt,
                   F1: Fn(&D)->U+'static,
                   F2: Fn(&D)->U+'static>(self, part1: F1, part2: F2) -> Self {

        let mut inputs = Vec::new();    // Vec<(G::Timestamp, Vec<(D, i32))>

        let exch = Exchange::new(move |&(ref x,_)| part1(x).as_u64());
        self.unary_notify(exch, format!("Consolidate"), vec![], move |input, output, notificator| {

            // 1. read each input, and stash it in our staging area
            while let Some((index, mut data)) = input.pull() {
                notificator.notify_at(&index);
                // TODO : Coalescing pre-extend() can mean less allocation,
                // TODO : but is a hack; shouldn't come in as big buffers.
                // coalesce8(&mut data, &|x| part2(x).as_u64());
                inputs.entry_or_insert(index.clone(), || Vec::new())
                      .extend(data.drain_temp());
            }

            // 2. go through each time of interest that has reached completion
            while let Some((index, _count)) = notificator.next() {
                if let Some(mut stash) = inputs.remove_key(&index) {
                    // let len = stash.len();
                    coalesce8(&mut stash, &|x| part2(x).as_u64());
                    // println!("consolidating at {:?}: {} -> {}", index, len, stash.len());
                    output.give_at(&index, stash.drain_temp());
                }
            }
        })
    }
}
