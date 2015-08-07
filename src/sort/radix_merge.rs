// use std::rc::Rc;

use itertools::Itertools;
use sort::coalesce;

use timely::drain::DrainExt;
/*

The goal here is to provide an interface that receives (K, V, i32) triples, and does its best to
maintain them in a compact form, taking not much more space than if you were to sort them,
accumulate weights and drop those with zero accumulation, and then run-length encode the K keys.

Additionally, it would be nice if there was a good way to compress the i32 values. I suspect RLE
would also make sense here, but random access would be nice for the "galloping iterator". You could
imagine such an iterator making fast progress if there was good compression (subtracting steps from
RLE counts)...

ASIDE : a common RLE representation represents runs as two of the same element, followed by a count.
ASIDE : the downside here is that if you just have two elements, you need a third to say "2", but
ASIDE : there should be no overhead for single elements, nor triple elements, and things get better
ASIDE : for more than that (assuming the count takes less-or-equal bytes as the data).

Another approach is just to compress everything. The "data" are effectively [u8], which is gross
but whatever. It *would* ensure locality of things like arrays and whatnot, which could be cool,
but it would be super important to avoid re-sizing anything ever (which would be fine because it is
all immutable). Abomonation compression could also slim the in-memory representation when used with
an iterator, as you wouldn't need the e.g. 24 bytes for a string, but rather just the length (the
surrounding 24 bytes being produced by the iterator).

*/

struct Columns<K, V> {
    keys: Vec<K>,           // keys
    cnts: Vec<u32>,         // counts
    vals: Vec<V>,           // values
    wgts: Vec<(i32,u32)>,   // run-length encoded within key group
}

impl<K, V> Columns<K, V> {
    pub fn new(k: usize, v: usize, w: usize) -> Columns<K, V> {
        Columns {
            keys: Vec::with_capacity(k),
            cnts: Vec::with_capacity(k),
            vals: Vec::with_capacity(v),
            wgts: Vec::with_capacity(w),
        }
    }
    // pub fn size(&self) -> usize {
    //     self.keys.len() * ::std::mem::size_of::<K>() +
    //     self.cnts.len() * 4 +
    //     self.vals.len() * ::std::mem::size_of::<V>() +
    //     self.wgts.len() * 8
    // }
}

pub struct Merge<K, V> {
    sorted: Columns<K, V>,
    staged: Vec<((K, V), i32)>
}

// TODO : Clone requirement is crap! Fix it.
impl<K: Ord+Copy, V: Ord> Merge<K,V> {
    pub fn new() -> Merge<K,V> { Merge {sorted: Columns::new(4, 4, 4), staged: Vec::with_capacity(4) } }
    pub fn prune(&mut self) {
        self.merge();
        self.staged.shrink_to_fit();
    }
    pub fn push(&mut self, key: K, val: V, wgt: i32) {
        self.staged.push(((key, val), wgt));

        // TODO : smarter tests exist based on sizes of K, V, etc.
        if self.staged.len() == self.staged.capacity() {
            // println!("merging at {} vals, with {} vals", self.sorted.vals.len(), self.staged.len());

            // merge with self.*_sorted
            self.merge();
            if self.staged.capacity() < self.sorted.vals.len() / 4 {
                self.staged = Vec::with_capacity(self.staged.capacity() * 2);
                println!("doubling capacity to {}", self.staged.capacity());
            }
        }
    }

    fn merge(&mut self) {

        self.staged.sort();
        coalesce(&mut self.staged);


        // make sure to pre-allocate enough based on existing sizes
        let mut result = Columns::new(self.sorted.keys.len() + self.staged.len(),
                                      self.sorted.vals.len() + self.staged.len(),
                                      self.sorted.wgts.len() + self.staged.len());

        {
            let keys = self.sorted.keys.drain_temp();
            let cnts = self.sorted.cnts.drain_temp();
            let vals = self.sorted.vals.drain_temp();
            let wgts = self.sorted.wgts.drain_temp().flat_map(|(w,c)| ::std::iter::repeat(w).take(c as usize));

            let sorted = keys.zip(cnts).flat_map(|(k,c)| ::std::iter::repeat(k).take(c as usize))
                             .zip(vals)
                             .zip(wgts);

            let staged = self.staged.drain_temp();

            let mut iterator = sorted.merge(staged).coalesce(|(p1,w1), (p2,w2)| {
                if p1 == p2 { Ok((p1, w1 + w2)) }
                else { Err(((p1,w1), (p2,w2))) }
            });

            // populate a new `Columns` with merged, coalesced data.
            if let Some(((mut old_key, val), mut old_wgt)) = iterator.next() {
                let mut key_cnt = 1;
                let mut wgt_cnt = 1;

                // always stash the val
                result.vals.push(val);

                for ((key, val), wgt) in iterator {

                    // always stash the val
                    result.vals.push(val);

                    // if they key or weight has changed, stash the weight.
                    if old_key != key || old_wgt != wgt {
                        // stash wgt, using run-length encoding
                        result.wgts.push((old_wgt, wgt_cnt));
                        old_wgt = wgt;
                        wgt_cnt = 0;
                    }

                    wgt_cnt += 1;

                    // if the key has changed, stash the key
                    if old_key != key {
                        result.keys.push(old_key);
                        result.cnts.push(key_cnt);
                        old_key = key;
                        key_cnt = 0;
                    }

                    key_cnt += 1;
                }

                result.keys.push(old_key);
                result.cnts.push(key_cnt);
                result.wgts.push((old_wgt, wgt_cnt));
            }
        }

        result.keys.shrink_to_fit();
        result.cnts.shrink_to_fit();
        result.vals.shrink_to_fit();
        result.wgts.shrink_to_fit();

        self.sorted = result;
    }
}




















//
