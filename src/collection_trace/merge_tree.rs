use iterators::merge::Merge;
use iterators::coalesce::Coalesce;
use timely::drain::DrainExt;

use std::iter;

// A collection of geometrically aged sorted lists, used to merge new sorted lists
// in an amortized fashion. The goal is to maintain (k,v,w) triples sorted by (k,v)
// and with w values aggregated, maintaining relatively small-ish space requirements

pub struct MergeTree<K, V> {
    dataz: Vec<(Vec<(K, usize)>, Vec<V>, Vec<(i32, usize)>)>,   // keys, vals, wgts
    stash: Vec<(K, V, i32)>,
    limit: usize,
}

impl<K: Ord+Eq, V: Ord+Eq> MergeTree<K, V> {
    pub fn new(limit: usize) -> MergeTree<K, V> {
        MergeTree {
            dataz: vec![],
            stash: vec![],
            limit: limit,
        }
    }

    // adds elements to stash; may provoke a merge if stash fills up
    pub fn extend<I: Iterator<Item=(K, V, i32)>>(&mut self, items: I) {
        self.stash.extend(items);
        if self.stash.len() > self.limit {
            self.flush_stash();
            self.think_merge(false);
        }
    }

    // moves data out of stash, then merges all dataz into one vector.
    pub fn finalize(mut self) -> Option<(Vec<(K, usize)>, Vec<V>, Vec<(i32, usize)>)> {
        self.flush_stash();
        self.think_merge(true);
        self.dataz.pop()
    }

    // moves the contents of stash into the last element of dataz.
    // obligatory to call do_merge before calling this again, to
    // ensure the last element of dataz is always empty.
    // TODO : can avoid merge in the case that the second to last
    // TODO : element of dataz is also empty.
    fn flush_stash(&mut self) {

        self.stash.sort();

        let mut keys = vec![];
        let mut vals = vec![];
        let mut wgts = vec![];

        // fill in a non-optimal manner, as ...
        for (k,v,w) in self.stash.drain_temp() {
            keys.push((k,1));
            vals.push(v);
            wgts.push((w,1));
        }

        self.dataz.push((keys, vals, wgts));
    }

    fn think_merge(&mut self, manual_override: bool) {

        let mut count = 0;
        let mut k_vec = Vec::new();
        let mut v_vec = Vec::new();
        while let Some((ks, vs, ws)) = self.dataz.pop() {
            if manual_override || vs.len() < count / 2 {
                count += vs.len();
                let v_iter = vs.into_iter();
                let w_iter = ws.into_iter().flat_map(|(w,l)| iter::repeat(w).take(l));
                k_vec.push(ks.into_iter());
                v_vec.push(v_iter.zip(w_iter));
            }
            else {
                break;
            }
        }

        self.dataz.push(merge_up(k_vec, v_vec, count));
    }
}

pub fn merge_up<K: Ord, V: Ord, IK: Iterator<Item=(K, usize)>, IV: Iterator<Item=(V, i32)>> (
    key_iters: Vec<IK>, mut val_iters: Vec<IV>, hint: usize) -> (Vec<(K, usize)>, Vec<V>, Vec<(i32, usize)>) {

    let mut key_merge = vec![];         // output for keys
    let mut val_merge = Vec::with_capacity(hint);         // output for vals
    let mut wgt_merge = vec![];         // output for wgts

    // an iterator over ((key, len), idx)
    let mut merged_keys = key_iters.into_iter()
                                   .enumerate()
                                   .map(|(i,v)| v.map(move |x| (x,i)))
                                   .merge()
                                   .peekable();

    // as long as keys remain ...
    while let Some(((key,len),idx)) = merged_keys.next() {

        // collect same keys
        let mut vec = vec![0; val_iters.len()];
        vec[idx] = len;
        while merged_keys.peek().map(|&((ref k,_),_)| k == &key) == Some(true) {
            if let Some(((_k,l),i)) = merged_keys.next() {
                vec[i] = l;
            }
        }

        // merge, coalesce vec[i] vals from each iterator
        let mut merged_vals = val_iters.iter_mut()
                                       .enumerate()
                                       .map(|(i,iter)| iter.take(vec[i]))
                                       .merge()
                                       .coalesce();

        // go through the vals, run-length coding the wgts
        if let Some((val, mut wgt)) = merged_vals.next() {

            val_merge.push(val);
            let mut val_count = 1;
            let mut wgt_count = 1;

            for (new_val, new_wgt) in merged_vals {

                // if wgt changes, record previous
                if wgt != new_wgt {
                    wgt_merge.push((wgt, wgt_count));
                    wgt_count = 0;
                    wgt = new_wgt;
                }
                val_merge.push(new_val);
                val_count += 1;
                wgt_count += 1;
            }

            // push remaining counts
            wgt_merge.push((wgt, wgt_count));
            key_merge.push((key, val_count));
        }
    }

    (key_merge, val_merge, wgt_merge)
}

#[test]
fn merge_tree() {
    let mut tree = MergeTree::new(4);
    tree.extend((0..5).map(|i| (i, i, i)));
    tree.extend((0..5).map(|i| (0, i, 1)));
    tree.extend((0..5).map(|i| (i, i, i)));
    tree.extend((0..5).map(|i| (i, 2, i)));

    println!("{:?}", tree.finalize());
}
