use std::fmt::Debug;

use timely::drain::DrainExt;

use collection_trace::{Trace, LeastUpperBound, Lookup, Offset};
use collection_trace::collection_trace::CollectionIterator;

pub struct OperatorTrace<K: Ord, T, S: Ord, R: Ord, L: Lookup<K, Offset>> {
    dst:            Vec<(R,i32)>,
    pub source:     Trace<K, T, S, L>,
    pub result:     Trace<K, T, R, L>,
}

impl<K: Ord+Clone, T:Clone+LeastUpperBound, S: Ord+Clone+Debug, R: Ord+Clone+Debug, L: Lookup<K, Offset>> OperatorTrace<K, T, S, R, L> {
    pub fn new<F: Fn()->L>(lookup: F) -> OperatorTrace<K, T, S, R, L> {
        OperatorTrace {
            source:  Trace::new(lookup()),
            result:  Trace::new(lookup()),
            dst:     Vec::new(),
        }
    }

    pub fn set_collections_and<F, G>(&mut self, time: &T, keys: Vec<K>, func: &F, and: &mut G)
        where F: Fn(&K, &mut CollectionIterator<S>, &mut Vec<(R, i32)>),
              G: FnMut(&K, &V, i32) {

        let mut accum = Compact::new(0,0,0);
        for key in keys.into_iter() {

            // fetch the collection at the time
            let mut iter = self.source.get_collection(&key, time);

            // if we have some data, invoke logic to populate self.dst
            if iter.peek().is_some() { logic(key, &mut iter, &mut self.dst); }

            // determine the gap between the current
            let mut difference = self.result.get_collection(&key, time)
                                            .map(|(v,w)| (v,-w))
                                            .merge(self.dst.drain_temp())
                                            .coalesce();

            if let Some((val, mut old_wgt)) = difference.next() {
                accum.vals.push(val);
                and(&key, &val, old_wgt);

                let mut counter = 1;
                let mut wgt_cnt = 1;
                for (val, wgt) in difference {
                    accum.vals.push(val);
                    and(&key, &val, wgt);

                    if old_wgt != wgt {
                        accum.wgts.push((old_wgt, wgt_cnt));
                        old_wgt = wgt;
                        wgt_cnt = 0;
                    }
                    wgt_cnt += 1;
                    counter += 1;
                }

                accum.wgts.push((old_wgt, wgt_cnt));
                accum.keys.push(key);
                accum.cnts.push(counter);
            }
        }

        self.result.set_differences(time.clone(), accum);
    }
}

// special-cased for set_collection.
fn subtract<V: Ord+Clone, I1: Iterator<Item=(V,i32)>, I2: Iterator<Item=(V,i32)>>(mut a: &[(V, i32)], mut b: &[(V, i32)], target: &mut Vec<(V, i32)>) {
    while a.len() > 0 && b.len() > 0 {
        match a[0].0.cmp(&b[0].0) {
            Ordering::Less    => { target.push(a[0].clone()); a = &a[1..]; },
            Ordering::Greater => { target.push(b[0].clone()); b = &b[1..]; },
            Ordering::Equal   => { target.push((a[0].0.clone(), a[0].1 + b[0].1));
                                   a = &a[1..]; b = &b[1..]; },
        }
    }

    if a.len() > 0 { target.extend(a.iter().map(|x| x.clone())); }
    if b.len() > 0 { target.extend(b.iter().map(|x| x.clone())); }
}
