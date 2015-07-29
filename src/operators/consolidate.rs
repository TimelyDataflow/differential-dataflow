use std::rc::Rc;

use timely::construction::*;
use timely::construction::operators::*;
use timely::communication::Data;
use timely::communication::pact::Exchange;
use timely::serialization::Serializable;

use sort::*;
use collection_trace::Lookup;
use collection_trace::lookup::UnsignedInt;

use timely::drain::DrainExt;

pub trait ConsolidateExt<D> {
    fn consolidate<U: UnsignedInt, F: Fn(&D)->U+'static>(&self, part: F) -> Self;
}

impl<G: GraphBuilder, D: Ord+Data+Serializable> ConsolidateExt<D> for Stream<G, (D, i32)> {
    fn consolidate<U: UnsignedInt, F: Fn(&D)->U+'static>(&self, part: F) -> Self {

        let mut inputs = Vec::new();    // Vec<(G::Timestamp, Vec<(D, i32))>
        let part1 = Rc::new(part);
        let part2 = part1.clone();

        let exch = Exchange::new(move |&(ref x,_)| (*part1)(x).as_u64());
        self.unary_notify(exch, "Consolidate", vec![], move |input, output, notificator| {

            // 1. read each input, and stash it in our staging area
            while let Some((index, data)) = input.pull() {
                notificator.notify_at(&index);
                inputs.entry_or_insert(index.clone(), || Vec::new())
                      .extend(data.drain_temp());
            }

            // 2. go through each time of interest that has reached completion
            while let Some((index, _count)) = notificator.next() {
                if let Some(mut stash) = inputs.remove_key(&index) {
                    // let len = stash.len();
                    coalesce8(&mut stash, &|x| (*part2)(x).as_u64());
                    // println!("consolidating at {:?}: {} -> {}", index, len, stash.len());
                    output.session(&index).give_iterator(stash.drain_temp());
                }
            }
        })
    }
}
