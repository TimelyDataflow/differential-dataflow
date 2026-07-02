//! M2(b)-4 (mechanism): the cursor-less JOIN compute over corgi batches — before the
//! JoinTactic defer/work/capability plumbing. Merge-join two sorted corgi batches by KEY
//! (`compare_at`), cross-product the matched val runs (`gather`), multiply diffs (Rust). Verified
//! vs a reference hash-join, with retractions (negative diffs). Single time (the (time) lattice
//! join lives in the tactic; here we isolate the columnar match+cross-product+diff algebra).
//!
//!   ~/.cargo/bin/cargo run --release -p interactive --example corgi_join_mechanism

use std::cmp::Ordering;
use std::collections::BTreeMap;

use timely::progress::Antichain;
use differential_dataflow::trace::{BatchReader, Description};

use interactive::corgi_arrange::CorgiBatch;

use corgi::arrange::{compare_at, gather};
use corgi::Value as CValue;

fn make_batch(updates: &[((u64, u64), i64)]) -> CorgiBatch<u64, i64> {
    let keys = CValue::u64(updates.iter().map(|u| u.0 .0).collect());
    let vals = CValue::u64(updates.iter().map(|u| u.0 .1).collect());
    let times = vec![0u64; updates.len()];
    let diffs = updates.iter().map(|u| u.1).collect();
    let desc = Description::new(Antichain::from_elem(0), Antichain::from_elem(1), Antichain::from_elem(0));
    CorgiBatch::from_unsorted(keys, vals, times, diffs, desc)
}

/// Cursor-less equijoin compute: matched (key, valL, valR) -> summed diff (diffL*diffR).
fn corgi_join(l: &CorgiBatch<u64, i64>, r: &CorgiBatch<u64, i64>) -> BTreeMap<(u64, u64, u64), i64> {
    let (nl, nr) = (l.len(), r.len());
    let (mut i, mut j) = (0, 0);
    let (mut li, mut ri, mut ds) = (Vec::new(), Vec::new(), Vec::new());
    while i < nl && j < nr {
        match compare_at(&l.keys, i, &r.keys, j) {
            Ordering::Less => i += 1,
            Ordering::Greater => j += 1,
            Ordering::Equal => {
                // runs of this key on each side (batches sorted by (key,val))
                let mut i2 = i;
                while i2 < nl && compare_at(&l.keys, i, &l.keys, i2) == Ordering::Equal {
                    i2 += 1;
                }
                let mut j2 = j;
                while j2 < nr && compare_at(&r.keys, j, &r.keys, j2) == Ordering::Equal {
                    j2 += 1;
                }
                for a in i..i2 {
                    for b in j..j2 {
                        li.push(a);
                        ri.push(b);
                        ds.push(l.diffs[a] * r.diffs[b]);
                    }
                }
                i = i2;
                j = j2;
            }
        }
    }
    // Materialize matched columns in bulk via gather (corgi-native), then fold to a map.
    let kc = gather(&l.keys, &li).into_u64("k");
    let lc = gather(&l.vals, &li).into_u64("vl");
    let rc = gather(&r.vals, &ri).into_u64("vr");
    let mut m = BTreeMap::new();
    for x in 0..ds.len() {
        *m.entry((kc[x], lc[x], rc[x])).or_insert(0) += ds[x];
    }
    m.retain(|_, d| *d != 0);
    m
}

/// Reference hash-join over the same updates.
fn reference(lu: &[((u64, u64), i64)], ru: &[((u64, u64), i64)]) -> BTreeMap<(u64, u64, u64), i64> {
    let mut lmap: BTreeMap<u64, Vec<(u64, i64)>> = BTreeMap::new();
    let mut rmap: BTreeMap<u64, Vec<(u64, i64)>> = BTreeMap::new();
    for ((k, v), d) in lu {
        lmap.entry(*k).or_default().push((*v, *d));
    }
    for ((k, v), d) in ru {
        rmap.entry(*k).or_default().push((*v, *d));
    }
    let mut m = BTreeMap::new();
    for (k, lvs) in &lmap {
        if let Some(rvs) = rmap.get(k) {
            for (vl, dl) in lvs {
                for (vr, dr) in rvs {
                    *m.entry((*k, *vl, *vr)).or_insert(0) += dl * dr;
                }
            }
        }
    }
    m.retain(|_, d| *d != 0);
    m
}

fn xorshift(s: &mut u64) -> u64 {
    *s ^= *s << 13;
    *s ^= *s >> 7;
    *s ^= *s << 17;
    *s
}

fn main() {
    // Directed example incl. multi-val keys and a retraction.
    let lu = vec![((1, 10), 1), ((1, 11), 1), ((2, 20), 1), ((3, 30), 1)];
    let ru = vec![((1, 100), 1), ((2, 200), 1), ((2, 201), 1), ((4, 400), 1)];
    let got = corgi_join(&make_batch(&lu), &make_batch(&ru));
    let want = reference(&lu, &ru);
    assert_eq!(got, want, "directed join mismatch");
    println!("  directed: {} matched rows  OK (e.g. key 2 → 1×2 = 2 pairs)", got.len());

    // Randomized property test incl. negative diffs (retractions → diffL*diffR can be negative).
    let mut seed = 0xfeed_face_dead_beefu64;
    for _ in 0..300 {
        let gen = |s: &mut u64| -> Vec<((u64, u64), i64)> {
            let n = (xorshift(s) % 20) as usize + 1;
            (0..n)
                .map(|_| {
                    let k = xorshift(s) % 5;
                    let v = xorshift(s) % 6;
                    let d = if xorshift(s) % 4 == 0 { -1 } else { 1 };
                    ((k, v), d)
                })
                .collect()
        };
        let lu = gen(&mut seed);
        let ru = gen(&mut seed);
        assert_eq!(corgi_join(&make_batch(&lu), &make_batch(&ru)), reference(&lu, &ru), "lu={lu:?} ru={ru:?}");
    }
    println!("  randomized: 300 joins == reference (incl. retractions / negative product diffs)");
    println!("\nM2(b)-4 mechanism: cursor-less corgi equijoin (merge-join via compare_at + gather");
    println!("  cross-product + diff-multiply) verified. Next: wrap in CorgiJoinTactic defer/work.");
}
