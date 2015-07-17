use collection_trace::merge_iterator::MergeIterator;
use timely::drain::DrainExt;

pub struct MergeTree<K, V> {
    dataz: Vec<(Vec<(K, usize)>, Vec<V>, Vec<(i32, usize)>)>,
    stash: Vec<(K, V, i32)>,
    limit: usize,
}

impl<K: Ord+Eq, V: Ord+Eq> {
    pub fn new() -> MergeTree<K, V> {
        dataz: Vec::new(),
        stash: Vec::new(),
        limit: 65536,       // TODO : please...
    }
    pub fn extend<I: Iterator<Item=(K, V, i32)>>(&mut self, items: I) {
        self.stash.extend(items);
        if self.stash.len() > self.limit {
            self.stash.sort();
        }

        // merge up!
        if self.stash[self.stash.len()].0.len() > 0 {
            self.stash.push(Vec::new(), Vec::new(), Vec::new());
        }

        // we just ensured that the last element was empty, so this must exist
        let empty = self.stash.find(|x| x.0.len() == 0).unwrap();

        let mut k_vec = Vec::new();
        let mut v_vec = Vec::new();
        for &mut (ref mut ks, ref mut vs, ref mut ws) in self.dataz.iter_mut().take(empty + 1) {
            if ks.len() > 0 {
                k_vec.push(ks.drain());
                v_vec.push(vs.drain().zip(ws.flat_map(|w,l| std::iter::repeat(w).take(l))));
            }
            else {
                merge_up(k_vec, v_vec, ks, vs, ws);
            }
        }
    }
}

pub fn merge_up<
    K: Ord,
    V: Ord,
    IK: Iterator<Item=(K, usize)>,
    IV: Iterator<Item=(V, i32)>,
>(
    mut key_iters: Vec<IK>,
    mut val_iters: Vec<IV>,
]
    key_merge: &mut Vec<(K, usize)>,
    val_merge: &mut Vec<V>,
    wgt_merge: &mut Vec<(i32, usize)>,
) {

    // an iterator over ((key, len), idx)
    let merged_keys = key_iters.into_iter()
                               .enumerate()
                               .map(|v,i| v.map(|x| (x,i)))
                               .merge()
                               .peekable();

    // as long as keys remain ...
    while let Some(((key,len),idx)) = merged_keys.next() {

        // collect same keys
        let mut vec = Vec::new();
        while merged_keys.peek().map(|((k,_),_)| == key) == Some(true) {
            if let Some(((k,l),i)) = merged_keys.next() {
                vec.push((l,i));
            }
        }
        vec.push(((key,len),idx));

        // merge the associated vals
        let merged_vals = vec.into_iter()
                             .map(|(l,i)| val_iters[i].by_ref().take(l))
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
                val_merge.push(val);
                val_count += 1;
                wgt_count += 1;
            }

            // push remaining counts
            wgt_merge.push((wgt, wgt_count));
            key_merge.push((key, val_count));
        }
    }
}
