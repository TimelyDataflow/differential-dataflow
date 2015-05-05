use std::marker::PhantomData;
use std::mem;

use sort::coalesce;

use collection_trace::{BatchCollectionTrace, close_under_lub, LeastUpperBound, Lookup};
use collection_trace::collection_trace::OperatorTrace;

pub struct BatchedUnaryTrace<K, T, V, L: Lookup<K, usize>> {
    phantom:    PhantomData<K>,
    updates:    Vec<V>,
    times:      Vec<(T, usize, usize, usize, usize)>,  // time, offset, pos_len, neg_len, next
    keys:       L,                                     // points to head
}

impl<K: Eq, T, V, L: Lookup<K, usize>> Default for BatchedUnaryTrace<K, T, V, L> {
    fn default() -> BatchedUnaryTrace<K, T, V, L> {
        BatchedUnaryTrace {
            phantom: PhantomData,
            updates: Vec::new(),
            times:   Vec::new(),
            keys:    L::new(),
        }
    }
}

// TODO : References could be even better here, due to more copies of things
fn merge<V: Eq+Ord+Clone>(mut pos: Vec<&[V]>,
                          mut neg: Vec<&[V]>,
                          tgt_pos: &mut Vec<V>,
                          tgt_neg: &mut Vec<V>) {

    pos.retain(|p| p.len() > 0);
    neg.retain(|n| n.len() > 0);

    while pos.len() > 0 || neg.len() > 0 {

        // find the minimal element at the head of either pos or neg
        let mut value = if pos.len() > 0 { &pos[0][0] } else { &neg[0][0] };
        for p in &pos { if &p[0] < value { value = &p[0]; } }
        for n in &neg { if &n[0] < value { value = &n[0]; } }

        // accumulate freq
        let mut count = 0;
        for p in &mut pos { while p.len() > 0 && &p[0] == value { count += 1; *p = &p[1..]; }}
        for n in &mut neg { while n.len() > 0 && &n[0] == value { count -= 1; *n = &n[1..]; }}

        // push appropriate number of copies
        while count > 0 { tgt_pos.push(value.clone()); count -= 1; }
        while count < 0 { tgt_neg.push(value.clone()); count += 1; }

        // thin out slices
        pos.retain(|p| p.len() > 0);
        neg.retain(|n| n.len() > 0);
    }
}

impl<K: Eq, L: Lookup<K, usize>, T: LeastUpperBound+Clone, V: Eq+Ord+Clone> BatchedUnaryTrace<K, T, V, L> {

    fn set_difference(&mut self, key: K, time: T, difference: &mut Vec<(V, i32)>) {
        coalesce(difference);
        if difference.len() > 0 {
            let position = self.keys.entry_or_insert(key, || usize::max_value());

            let offset = self.updates.len();    // offset
            for &(ref val, wgt) in difference.iter() { if wgt > 0 { for _ in 0..wgt { self.updates.push(val.clone()); } } }
            let pos_len = self.updates.len() - offset;
            for &(ref val, wgt) in difference.iter() { if wgt < 0 { for _ in 0..-wgt { self.updates.push(val.clone()); } } }
            let neg_len = self.updates.len() - offset - pos_len;

            self.times.push((time, offset, pos_len, neg_len, *position));
            *position = self.times.len() - 1;
        }
    }

    fn set_collection(&mut self, key: K, time: T, collection: &mut Vec<V>) {
        collection.sort();

        let mut tgt_pos = Vec::new();
        let mut tgt_neg = Vec::new();

        {
            let mut pos = Vec::new();         // could re-use these, as keys are batched
            let mut neg = vec![&collection[..]];   // could re-use these, as keys are batched

            // populate pos and negate per get_collection
            if let Some(&position) = self.keys.get_ref(&key) {
                let mut position = position;
                while position != usize::max_value() {
                    let (_, offset, pos_len, neg_len, next) = self.times[position];
                    if self.times[position].0 <= time {
                        if pos_len > 0 { pos.push(&self.updates[offset..][..pos_len]); }
                        if neg_len > 0 { neg.push(&self.updates[(offset + pos_len)..][..neg_len]); }
                    }
                    position = next;
                }
            }

            merge(pos, neg, &mut tgt_pos, &mut tgt_neg);    // merge everything down
            mem::swap(&mut tgt_pos, &mut tgt_neg);          // silly, but communicates negation!
        }

        let position = self.keys.entry_or_insert(key, || usize::max_value());

        let offset = self.updates.len();
        self.updates.push_all(&tgt_pos);
        self.updates.push_all(&tgt_neg);
        self.times.push((time, offset, tgt_pos.len(), tgt_neg.len(), *position));
        *position = self.times.len() - 1;
    }

    fn get_difference(&self, key: &K, time: &T, difference: &mut Vec<(V, i32)>) {
        if let Some(&position) = self.keys.get_ref(key) {
            let mut position = position;
            while position != usize::max_value() {
                if &self.times[position].0 == time {

                    let pos = &self.updates[self.times[position].1..][..self.times[position].2];
                    let neg = &self.updates[self.times[position].1+self.times[position].2..][..self.times[position].3];

                    unary_to_binary(pos, neg, difference);

                    return;
                }
                position = self.times[position].4;
            }
        }
    }
    fn get_collection(&self, key: &K, time: &T, tgt_pos: &mut Vec<V>) {
        if let Some(&position) = self.keys.get_ref(key) {
            let mut position = position;

            let mut pos = Vec::new();   // could re-use these, as keys are batched
            let mut neg = Vec::new();   // could re-use these, as keys are batched

            while position != usize::max_value() {
                let (_, offset, pos_len, neg_len, next) = self.times[position];
                if &self.times[position].0 <= time {
                    if pos_len > 0 { pos.push(&self.updates[offset..][..pos_len]); }
                    if neg_len > 0 { neg.push(&self.updates[(offset + pos_len)..][..neg_len]); }
                }
                position = next;
            }

            let mut tgt_neg = Vec::new();
            merge(pos, neg, tgt_pos, &mut tgt_neg);
            assert!(tgt_neg.len() == 0);
        }
    }

    fn interesting_times(&mut self, key: &K, index: &T, result: &mut Vec<T>) {
        if let Some(&position) = self.keys.get_ref(key) {
            let mut position = position;
            while position != usize::max_value() {
                let lub = index.least_upper_bound(&self.times[position].0);
                if !result.contains(&lub) {
                    result.push(lub);
                }
                position = self.times[position].4;
            }
        }
        close_under_lub(result);
    }
}

impl<K: Eq, L: Lookup<K, usize>, T: LeastUpperBound+Clone, V: Eq+Ord+Clone> BatchedUnaryTrace<K, T, V, L> {
    pub fn new(l: L) -> BatchedUnaryTrace<K, T, V, L> {
        BatchedUnaryTrace {
            phantom: PhantomData,
            updates: Vec::new(),
            times:   Vec::new(),
            keys:    l,
        }
    }
}

// fn binary_to_unary<V: Ord+Eq+Clone>(bin: &[(V,i32)], pos: &mut Vec<V>, neg: &mut Vec<V>) {
//     unimplemented!()
// }

fn unary_to_binary<V: Ord+Eq+Clone>(mut pos: &[V], mut neg: &[V], bin: &mut Vec<(V,i32)>) {

    while pos.len() > 0 && neg.len() > 0 {
        while pos.len() > 0 && (neg.len() == 0 || pos[0] < neg[0]) {
            let mut count = 0;
            while count < pos.len() && pos[count] == pos[0] { count += 1; }
            bin.push((pos[0].clone(), count as i32));
            pos = &pos[count..];
        }
        while neg.len() > 0 && (pos.len() == 0 || pos[0] > neg[0]) {
            let mut count = 0;
            while count < neg.len() && neg[count] == neg[0] { count += 1; }
            bin.push((neg[0].clone(), -(count as i32)));
            neg = &neg[count..];
        }
    }
}

pub struct BatchUnaryShard<K, T, S, R,
                           L: Lookup<K, usize>,
                           F: Fn(&K, &[S], &mut Vec<R>)> {
    phantom:    PhantomData<K>,
    logic:      F,
    unary_s:    Vec<S>,
    unary_r:    Vec<R>,
    source:     BatchedUnaryTrace<K, T, S, L>,
    result:     BatchedUnaryTrace<K, T, R, L>,
}

impl<K: Eq+Clone,
     T: LeastUpperBound+Clone,
     S: Ord+Clone,
     R: Ord+Clone,
     L: Lookup<K, usize>,
     F: Fn(&K, &[S], &mut Vec<R>),
     > BatchUnaryShard<K, T, S, R, L, F> {
    pub fn new<G: Fn()->L>(lookup: G, logic: F) -> BatchUnaryShard<K, T, S, R, L, F> {
        BatchUnaryShard {
            logic:   logic,
            phantom: PhantomData,
            source:  BatchedUnaryTrace::new(lookup()),
            result:  BatchedUnaryTrace::new(lookup()),
            unary_s: Vec::new(),
            unary_r: Vec::new(),
        }
    }
}
impl<K: Eq+Clone,
     T: LeastUpperBound+Clone,
     S: Ord+Clone,
     R: Ord+Clone,
     L: Lookup<K, usize>,
     F: Fn(&K, &[S], &mut Vec<R>),
     >
 OperatorTrace for BatchUnaryShard<K,T,S, R,L,F> {
     type Key = K;
     type Index = T;
     type Input = S;
     type Output = R;

    fn set_difference_at(&mut self, key: &K, index: &T, diffs: &mut Vec<(S, i32)>, times: &mut Vec<T>) {
        self.source.set_difference(key.clone(), index.clone(), diffs);
        self.source.interesting_times(&key, &index, times);
    }

    fn get_difference_at(&mut self, key: &K, index: &T, diffs: &mut Vec<(R, i32)>) {

        self.source.get_collection(&key, &index, &mut self.unary_s);

        if self.unary_s.len() > 0 { (self.logic)(key, &self.unary_s, &mut self.unary_r); }

        self.result.set_collection(key.clone(), index.clone(), &mut self.unary_r);
        self.result.get_difference(&key, &index, diffs);

        self.unary_s.clear();
        self.unary_r.clear();
    }
}

#[test]
fn batch_unary_shard() {
    let mut operator = BatchUnaryShard::new(|| (Vec::new(), 0), |key, src, dst| {
        for &x in src.iter() { dst.push(x + 0u64); }
    });

    let mut src_diffs = vec![(0u64, 1), (10u64, 2), (20u64, 3)];
    let mut dst_diffs = vec![];
    let mut idx       = vec![];

    operator.set_difference_at(&5u64, &4u64, &mut src_diffs, &mut idx);
    operator.get_difference_at(&5u64, &4u64, &mut dst_diffs);
}
