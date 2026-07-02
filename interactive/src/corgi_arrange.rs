//! Phase 2 / M2(b) Route B — the cursor-less corgi arrangement batch.
//!
//! `CorgiBatch` stores a sorted, consolidated run of `(key, val, time, diff)` with key/val as corgi
//! columns and time/diff as Rust Vecs. It implements `BatchReader` (len/description) but NO `Cursor`:
//! navigation is the tactic's concern (it reads the corgi columns in bulk). Sorting/merging use the
//! corgi `arrange` API (`compare_at` for the (key,val) order, `gather`/`gather_lanes` to materialize),
//! with the time tiebreak + diff consolidation + time-advance in plain Rust (corgi never sees time).
//!
//! Order: (key, val) by corgi structural order, then time by `Ord`. Any total order works as long as
//! it is used consistently (correctness compares multisets, not against DDIR's derived `Ord`).

use std::cmp::Ordering;

use timely::progress::{Antichain, Timestamp};
use timely::progress::frontier::AntichainRef;

use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::{Batch, BatchReader, Builder as DdBuilder, Description, Merger as DdMerger};

use corgi::Value as CValue;
use corgi::arrange::{compare_at, gather, gather_lanes};

use crate::corgi_logic::{infer_shape, transcode, untranscode};
use crate::ir::Value as DValue;

/// A sorted, consolidated arrangement batch with corgi-columnar key/val payload.
#[derive(Clone)]
pub struct CorgiBatch<T, R> {
    /// Key column (corgi), aligned with `vals`/`times`/`diffs`, sorted by (key, val, time).
    pub keys: CValue,
    /// Val column (corgi).
    pub vals: CValue,
    /// Per-update times (Rust; the lattice algebra lives here, not in corgi).
    pub times: Vec<T>,
    /// Per-update diffs (Rust).
    pub diffs: Vec<R>,
    /// Time bounds of the batch.
    pub desc: Description<T>,
}

impl<T: Timestamp + Lattice, R: Semigroup + 'static> BatchReader for CorgiBatch<T, R> {
    type Time = T;
    fn len(&self) -> usize { self.times.len() }
    fn description(&self) -> &Description<T> { &self.desc }
}

/// The (key,val) sort payload as one corgi Prod column (cheap Arc bumps).
fn kv_of<T, R>(b: &CorgiBatch<T, R>) -> CValue {
    CValue::Prod(vec![b.keys.clone(), b.vals.clone()])
}

/// Split a `Prod([keys, vals])` corgi value into its two columns.
fn split_kv(kv: CValue) -> (CValue, CValue) {
    let mut cols = kv.into_prod("corgi batch kv");
    let vals = cols.pop().unwrap();
    let keys = cols.pop().unwrap();
    (keys, vals)
}

impl<T: Clone, R: Copy> CorgiBatch<T, R> {
    /// Read the batch back to `((key, val), time, diff)` rows (cursor-free; untranscodes the corgi
    /// key/val columns). Used by `Backend::as_collection`, the reduce tactic, and tests.
    pub fn to_updates(&self) -> Vec<((DValue, DValue), T, R)> {
        if self.times.is_empty() {
            return Vec::new();
        }
        let keys = untranscode(self.keys.clone(), &corgi::shape_of_value(&self.keys));
        let vals = untranscode(self.vals.clone(), &corgi::shape_of_value(&self.vals));
        keys.into_iter()
            .zip(vals)
            .zip(&self.times)
            .zip(&self.diffs)
            .map(|(((k, v), t), d)| ((k, v), t.clone(), *d))
            .collect()
    }
}

impl<T: Timestamp + Lattice, R: Semigroup + Clone + 'static> CorgiBatch<T, R> {
    /// Build a batch from parallel, UNSORTED columns: sort by (key,val,time) and consolidate
    /// equal (key,val,time) triples (dropping zero diffs).
    pub fn from_unsorted(keys: CValue, vals: CValue, times: Vec<T>, diffs: Vec<R>, desc: Description<T>) -> Self {
        let n = times.len();
        let kv = CValue::Prod(vec![keys, vals]);
        let mut perm: Vec<usize> = (0..n).collect();
        perm.sort_by(|&i, &j| compare_at(&kv, i, &kv, j).then_with(|| times[i].cmp(&times[j])));
        let kv_s = gather(&kv, &perm);
        let times_s: Vec<T> = perm.iter().map(|&i| times[i].clone()).collect();
        let diffs_s: Vec<R> = perm.iter().map(|&i| diffs[i].clone()).collect();

        let (mut keep, mut ot, mut od) = (Vec::new(), Vec::new(), Vec::new());
        let mut i = 0;
        while i < n {
            let t = times_s[i].clone();
            let mut d = diffs_s[i].clone();
            let mut k = i + 1;
            while k < n && compare_at(&kv_s, i, &kv_s, k) == Ordering::Equal && times_s[k] == t {
                d.plus_equals(&diffs_s[k]);
                k += 1;
            }
            if !d.is_zero() {
                keep.push(i);
                ot.push(t);
                od.push(d);
            }
            i = k;
        }
        let (keys, vals) = split_kv(gather(&kv_s, &keep));
        CorgiBatch { keys, vals, times: ot, diffs: od, desc }
    }
}

/// Advance a group's (time, diff) pairs to `frontier`, consolidate by advanced time, and emit the
/// surviving rows pointing at one representative source row `(rep_tag, rep_off)` (same key/val).
#[allow(clippy::too_many_arguments)]
fn emit_group<T, R>(
    pairs: Vec<(T, R)>,
    rep_tag: usize,
    rep_off: usize,
    frontier: AntichainRef<T>,
    tags: &mut Vec<usize>,
    off: &mut Vec<usize>,
    times: &mut Vec<T>,
    diffs: &mut Vec<R>,
) where
    T: Timestamp + Lattice,
    R: Semigroup + Clone,
{
    let mut adv: Vec<(T, R)> = pairs
        .into_iter()
        .map(|(mut t, d)| {
            t.advance_by(frontier);
            (t, d)
        })
        .collect();
    adv.sort_by(|a, b| a.0.cmp(&b.0));
    let mut k = 0;
    while k < adv.len() {
        let t = adv[k].0.clone();
        let mut d = adv[k].1.clone();
        k += 1;
        while k < adv.len() && adv[k].0 == t {
            d.plus_equals(&adv[k].1);
            k += 1;
        }
        if !d.is_zero() {
            tags.push(rep_tag);
            off.push(rep_off);
            times.push(t);
            diffs.push(d);
        }
    }
}

/// Merge two sorted `CorgiBatch`es into one, advancing all times to `frontier` and consolidating.
/// (key,val) groups are walked in sorted order via `compare_at`; each group's (time,diff) pairs from
/// both sources are advanced+consolidated; output columns are materialized with `gather_lanes`.
pub fn merge<T, R>(b1: &CorgiBatch<T, R>, b2: &CorgiBatch<T, R>, frontier: AntichainRef<T>) -> CorgiBatch<T, R>
where
    T: Timestamp + Lattice,
    R: Semigroup + Clone + 'static,
{
    let kv1 = kv_of(b1);
    let kv2 = kv_of(b2);
    let (n1, n2) = (b1.len(), b2.len());
    let (mut i, mut j) = (0, 0);
    let (mut tags, mut off) = (Vec::new(), Vec::new());
    let (mut times, mut diffs) = (Vec::new(), Vec::new());

    // length of b1's (key,val) run starting at `s`.
    let run1 = |s: usize, i: &mut usize| {
        let mut pairs = Vec::new();
        while *i < n1 && compare_at(&kv1, s, &kv1, *i) == Ordering::Equal {
            pairs.push((b1.times[*i].clone(), b1.diffs[*i].clone()));
            *i += 1;
        }
        pairs
    };
    let run2 = |s: usize, j: &mut usize| {
        let mut pairs = Vec::new();
        while *j < n2 && compare_at(&kv2, s, &kv2, *j) == Ordering::Equal {
            pairs.push((b2.times[*j].clone(), b2.diffs[*j].clone()));
            *j += 1;
        }
        pairs
    };

    while i < n1 && j < n2 {
        match compare_at(&kv1, i, &kv2, j) {
            Ordering::Less => {
                let s = i;
                let pairs = run1(s, &mut i);
                emit_group(pairs, 0, s, frontier, &mut tags, &mut off, &mut times, &mut diffs);
            }
            Ordering::Greater => {
                let s = j;
                let pairs = run2(s, &mut j);
                emit_group(pairs, 1, s, frontier, &mut tags, &mut off, &mut times, &mut diffs);
            }
            Ordering::Equal => {
                let (s1, s2) = (i, j);
                let mut pairs = run1(s1, &mut i);
                pairs.extend(run2(s2, &mut j));
                emit_group(pairs, 0, s1, frontier, &mut tags, &mut off, &mut times, &mut diffs);
            }
        }
    }
    while i < n1 {
        let s = i;
        let pairs = run1(s, &mut i);
        emit_group(pairs, 0, s, frontier, &mut tags, &mut off, &mut times, &mut diffs);
    }
    while j < n2 {
        let s = j;
        let pairs = run2(s, &mut j);
        emit_group(pairs, 1, s, frontier, &mut tags, &mut off, &mut times, &mut diffs);
    }

    // An empty source carries a `Unit(0)` placeholder shape (see `Batch::empty`), which would trip
    // `gather_lanes`' cross-source shape check even though no tag references it. Substitute the
    // non-empty side's `kv` for any empty source so all sources share the real shape; the substituted
    // reference is never indexed (no tag points to an empty side).
    let (src0, src1) = match (n1 == 0, n2 == 0) {
        (false, true) => (&kv1, &kv1),
        (true, false) => (&kv2, &kv2),
        _ => (&kv1, &kv2),
    };
    let (keys, vals) = if tags.is_empty() {
        // Both empty (or fully cancelled): use whichever source has a real shape.
        let witness = if n1 != 0 { &b1.keys } else { &b2.keys };
        let witness_v = if n1 != 0 { &b1.vals } else { &b2.vals };
        (gather(witness, &[]), gather(witness_v, &[]))
    } else {
        split_kv(gather_lanes(&[Some(src0), Some(src1)], &tags, &off))
    };
    let lower = b1.desc.lower().meet(b2.desc.lower());
    let upper = b1.desc.upper().join(b2.desc.upper());
    let desc = Description::new(lower, upper, frontier.to_owned());
    CorgiBatch { keys, vals, times, diffs, desc }
}

/// `Batch::Merger` for `CorgiBatch`: an eager wrapper around [`merge`]. Correctness-first — it does
/// the whole merge on the first `work` and leaves `fuel` untouched (positive → "complete"), so there
/// is no latency smoothing yet. Fuel-bounded incremental merging is a later refinement.
pub struct CorgiMerger<T, R> {
    result: Option<CorgiBatch<T, R>>,
    frontier: Antichain<T>,
}

impl<T, R> DdMerger<CorgiBatch<T, R>> for CorgiMerger<T, R>
where
    T: Timestamp + Lattice,
    R: Semigroup + Clone + 'static,
{
    fn new(_s1: &CorgiBatch<T, R>, _s2: &CorgiBatch<T, R>, frontier: AntichainRef<T>) -> Self {
        CorgiMerger { result: None, frontier: frontier.to_owned() }
    }
    fn work(&mut self, s1: &CorgiBatch<T, R>, s2: &CorgiBatch<T, R>, _fuel: &mut isize) {
        if self.result.is_none() {
            self.result = Some(merge(s1, s2, self.frontier.borrow()));
        }
    }
    fn done(self) -> CorgiBatch<T, R> {
        self.result.expect("CorgiMerger::done called before work")
    }
}

impl<T, R> Batch for CorgiBatch<T, R>
where
    T: Timestamp + Lattice,
    R: Semigroup + Clone + 'static,
{
    type Merger = CorgiMerger<T, R>;
    fn empty(lower: Antichain<T>, upper: Antichain<T>) -> Self {
        CorgiBatch {
            keys: CValue::Unit(0),
            vals: CValue::Unit(0),
            times: Vec::new(),
            diffs: Vec::new(),
            desc: Description::new(lower, upper, Antichain::from_elem(T::minimum())),
        }
    }
}

/// Build a `CorgiBatch` from DDIR row updates (the arrangement-ingest boundary transcode). The
/// batcher delivers sorted+consolidated chains; `from_unsorted` re-sorts idempotently for safety.
fn build_batch<T, R>(updates: Vec<((DValue, DValue), T, R)>, desc: Description<T>) -> CorgiBatch<T, R>
where
    T: Timestamp + Lattice,
    R: Semigroup + Clone + 'static,
{
    if updates.is_empty() {
        return CorgiBatch { keys: CValue::Unit(0), vals: CValue::Unit(0), times: Vec::new(), diffs: Vec::new(), desc };
    }
    let kshape = infer_shape(&updates[0].0 .0);
    let vshape = infer_shape(&updates[0].0 .1);
    let keys_rows: Vec<DValue> = updates.iter().map(|u| u.0 .0.clone()).collect();
    let vals_rows: Vec<DValue> = updates.iter().map(|u| u.0 .1.clone()).collect();
    let times = updates.iter().map(|u| u.1.clone()).collect();
    let diffs = updates.iter().map(|u| u.2.clone()).collect();
    let keys = transcode(&keys_rows, &kshape);
    let vals = transcode(&vals_rows, &vshape);
    CorgiBatch::from_unsorted(keys, vals, times, diffs, desc)
}

/// DD `Builder` for `CorgiBatch`: consumes sorted/consolidated row chains (the batcher's `Output`)
/// and builds a corgi-columnar batch. `Input = Vec<((key,val), time, diff)>` so the existing
/// `MergeBatcher` over vec rows can be reused as the `Batcher`. Wrapped by `RcBuilder` → `Rc<CorgiBatch>`.
pub struct CorgiBatchBuilder<T, R> {
    updates: Vec<((DValue, DValue), T, R)>,
}

impl<T, R> DdBuilder for CorgiBatchBuilder<T, R>
where
    T: Timestamp + Lattice,
    R: Semigroup + Clone + 'static,
{
    type Input = Vec<((DValue, DValue), T, R)>;
    type Time = T;
    type Output = CorgiBatch<T, R>;

    fn with_capacity(_keys: usize, _vals: usize, upds: usize) -> Self {
        CorgiBatchBuilder { updates: Vec::with_capacity(upds) }
    }
    fn push(&mut self, chunk: &mut Self::Input) {
        self.updates.append(chunk);
    }
    fn done(self, description: Description<T>) -> CorgiBatch<T, R> {
        build_batch(self.updates, description)
    }
    fn seal(chain: &mut Vec<Self::Input>, description: Description<T>) -> CorgiBatch<T, R> {
        let mut all = Vec::new();
        for c in chain.iter_mut() {
            all.append(c);
        }
        build_batch(all, description)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn xorshift(seed: &mut u64) -> u64 {
        *seed ^= *seed << 13;
        *seed ^= *seed >> 7;
        *seed ^= *seed << 17;
        *seed
    }

    /// Build a CorgiBatch from u64 (key,val) updates with a [lower,upper) description.
    fn batch(updates: &[((u64, u64), u64, i64)], lower: u64, upper: u64) -> CorgiBatch<u64, i64> {
        let keys = CValue::u64(updates.iter().map(|u| u.0 .0).collect());
        let vals = CValue::u64(updates.iter().map(|u| u.0 .1).collect());
        let times = updates.iter().map(|u| u.1).collect();
        let diffs = updates.iter().map(|u| u.2).collect();
        let desc = Description::new(
            Antichain::from_elem(lower),
            Antichain::from_elem(upper),
            Antichain::from_elem(0u64),
        );
        CorgiBatch::from_unsorted(keys, vals, times, diffs, desc)
    }

    /// Read a batch back to a ((k,v),t)->d map.
    fn read(b: &CorgiBatch<u64, i64>) -> std::collections::BTreeMap<((u64, u64), u64), i64> {
        let ks = b.keys.clone().into_u64("k");
        let vs = b.vals.clone().into_u64("v");
        let mut m = std::collections::BTreeMap::new();
        for idx in 0..b.times.len() {
            *m.entry(((ks[idx], vs[idx]), b.times[idx])).or_insert(0) += b.diffs[idx];
        }
        m.retain(|_, d| *d != 0);
        m
    }

    /// Reference: union both update sets, advance times to `f`, consolidate by ((k,v),t).
    fn reference(u1: &[((u64, u64), u64, i64)], u2: &[((u64, u64), u64, i64)], f: u64) -> std::collections::BTreeMap<((u64, u64), u64), i64> {
        let mut m = std::collections::BTreeMap::new();
        for u in u1.iter().chain(u2) {
            let t = u.1.max(f); // advance_by for a total u64 order = max
            *m.entry((u.0, t)).or_insert(0) += u.2;
        }
        m.retain(|_, d| *d != 0);
        m
    }

    #[test]
    fn from_unsorted_sorts_and_consolidates() {
        let u = vec![((1, 0), 3, 1), ((0, 0), 0, 1), ((1, 0), 3, 1), ((0, 0), 0, -1), ((2, 5), 1, 1)];
        let b = batch(&u, 0, 10);
        // (0,0)@0 cancels; (1,0)@3 sums to 2; (2,5)@1 stays.
        let got = read(&b);
        assert_eq!(got.get(&((1, 0), 3)), Some(&2));
        assert_eq!(got.get(&((0, 0), 0)), None);
        assert_eq!(got.get(&((2, 5), 1)), Some(&1));
    }

    #[test]
    fn builder_builds_corgi_batch() {
        let rows: Vec<((DValue, DValue), u64, i64)> = vec![
            ((DValue::Int(1), DValue::Int(10)), 0, 1),
            ((DValue::Int(1), DValue::Int(10)), 0, 1), // dup -> consolidates to diff 2
            ((DValue::Int(2), DValue::Int(20)), 0, 1),
        ];
        let desc = Description::new(Antichain::from_elem(0u64), Antichain::from_elem(1u64), Antichain::from_elem(0u64));
        let mut b = CorgiBatchBuilder::<u64, i64>::with_capacity(0, 0, 3);
        b.push(&mut rows.clone());
        let batch = b.done(desc);
        assert_eq!(batch.len(), 2, "consolidated to 2 distinct (key,val)");
        let ks = batch.keys.clone().into_u64("k");
        let vs = batch.vals.clone().into_u64("v");
        // locate (1,10) and check its diff is 2
        let idx = (0..batch.len()).find(|&i| ks[i] == 1 && vs[i] == 10).expect("(1,10) present");
        assert_eq!(batch.diffs[idx], 2);
    }

    #[test]
    fn merger_trait_drives() {
        // Drive the Batch::Merger (fuel loop, as the spine does) and confirm == reference.
        let u1 = vec![((0, 0), 0, 1), ((1, 0), 2, 1), ((1, 0), 5, 1)];
        let u2 = vec![((1, 0), 2, 1), ((2, 3), 0, 1), ((0, 0), 0, -1)];
        let f = 3u64;
        let b1 = batch(&u1, 0, 10);
        let b2 = batch(&u2, 0, 10);
        let mut m = CorgiMerger::new(&b1, &b2, Antichain::from_elem(f).borrow());
        loop {
            let mut fuel = 1isize;
            m.work(&b1, &b2, &mut fuel);
            if fuel > 0 {
                break;
            }
        }
        let out = m.done();
        assert_eq!(read(&out), reference(&u1, &u2, f));
    }

    #[test]
    fn merge_matches_reference() {
        let mut seed = 0x1234_5678_9abc_def1u64;
        for _ in 0..300 {
            let gen = |seed: &mut u64| -> Vec<((u64, u64), u64, i64)> {
                let n = (xorshift(seed) % 30) as usize + 1;
                (0..n)
                    .map(|_| {
                        let k = xorshift(seed) % 6;
                        let v = xorshift(seed) % 4;
                        let t = xorshift(seed) % 8;
                        let d = if xorshift(seed) % 4 == 0 { -1 } else { 1 };
                        ((k, v), t, d)
                    })
                    .collect()
            };
            let u1 = gen(&mut seed);
            let u2 = gen(&mut seed);
            let f = xorshift(&mut seed) % 8;

            let b1 = batch(&u1, 0, 10);
            let b2 = batch(&u2, 0, 10);
            let merged = merge(&b1, &b2, Antichain::from_elem(f).borrow());

            assert_eq!(read(&merged), reference(&u1, &u2, f), "f={f}\n u1={u1:?}\n u2={u2:?}");
        }
    }
}
