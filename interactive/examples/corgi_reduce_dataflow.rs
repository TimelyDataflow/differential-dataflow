//! M2(b) reduce LIVE: the cursor-less `CorgiReduceTactic` (built on Frank's `active_times`) driving
//! a real `reduce_with_tactic` over a corgi-columnar arrangement. Count-per-key, fed across two
//! times with a retraction-to-zero, read back from the output arrangement's batch stream
//! (`Vec<Rc<CorgiBatch>>`, cursor-free) and compared to a Rust reference.
//!
//!   ~/.cargo/bin/cargo run --release -p interactive --example corgi_reduce_dataflow

use std::collections::BTreeMap;
use std::rc::Rc;

use timely::order::Product;
use timely::progress::Timestamp;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::capture::{Capture, Event};
use timely::dataflow::operators::*;

use differential_dataflow::input::Input;
use differential_dataflow::operators::arrange::arrangement::arrange_core;
use differential_dataflow::operators::reduce::reduce_with_tactic;
use differential_dataflow::trace::implementations::chunker::ContainerChunker;
use differential_dataflow::trace::implementations::merge_batcher::vec::VecMerger;
use differential_dataflow::trace::implementations::merge_batcher::MergeBatcher;
use differential_dataflow::trace::implementations::spine_fueled::Spine;
use differential_dataflow::trace::rc_blanket_impls::RcBuilder;

use differential_dataflow::operators::reduce::ReduceTactic;
use differential_dataflow::trace::Description;
use timely::progress::Antichain;

use interactive::corgi_arrange::{CorgiBatch, CorgiBatchBuilder};
use interactive::corgi_backend::CorgiContainer;
use interactive::corgi_logic::{infer_shape, transcode, untranscode};
use interactive::corgi_reduce::CorgiReduceTactic;
use interactive::ir::{Diff, Value as DValue};
use interactive::parse::Reducer;

type Row = DValue;

fn read_batch<T: Clone>(b: &CorgiBatch<T, Diff>) -> Vec<((Row, Row), T, Diff)> {
    if b.times.is_empty() {
        return Vec::new();
    }
    let keys = untranscode(b.keys.clone(), &corgi::shape_of_value(&b.keys));
    let vals = untranscode(b.vals.clone(), &corgi::shape_of_value(&b.vals));
    keys.into_iter()
        .zip(vals)
        .zip(&b.times)
        .zip(&b.diffs)
        .map(|(((k, v), t), d)| ((k, v), t.clone(), *d))
        .collect()
}

/// Run the corgi backend's reduce over `updates`, return final consolidated `(key, reduced_val)`.
/// Generic over the time `T` so we can test both totally-ordered (`u64`) and partial-order
/// (`Product<u64,u64>`) times — the latter exercises `active_times`' synthetic-join-time generation.
fn run_corgi(updates: Vec<((Row, Row), u64, Diff)>, reducer: Reducer) -> BTreeMap<(Row, Row), Diff> {
    let (tx, rx) = std::sync::mpsc::channel::<Event<u64, Vec<((Row, Row), u64, Diff)>>>();
    timely::execute_directly(move |worker| {
        let mut handle = worker.dataflow::<u64, _, _>(|scope| {
            let (handle, coll) = scope.new_collection::<(Row, Row), Diff>();
            let corgi = coll.inner.unary(Pipeline, "ToCorgi", |_, _| {
                |input, output| {
                    input.for_each(|cap, data| {
                        let mut c = CorgiContainer::from_updates(std::mem::take(data));
                        output.session(&cap).give_container(&mut c);
                    });
                }
            });
            let arr = arrange_core::<_, CorgiContainer<u64, Diff>, ContainerChunker<Vec<((Row, Row), u64, Diff)>>, MergeBatcher<VecMerger<(Row, Row), u64, Diff>>, RcBuilder<CorgiBatchBuilder<u64, Diff>>, Spine<Rc<CorgiBatch<u64, Diff>>>>(corgi, Pipeline, "CorgiArrange");
            let reduced = reduce_with_tactic::<_, Spine<Rc<CorgiBatch<u64, Diff>>>, _>(arr, "CorgiReduce", CorgiReduceTactic::<u64>::new(reducer));
            reduced
                .stream
                .unary(Pipeline, "ReadReduceOut", |_, _| {
                    |input, output| {
                        input.for_each(|cap, data| {
                            let mut rows: Vec<((Row, Row), u64, Diff)> = Vec::new();
                            for batch in data.iter() {
                                rows.extend(read_batch(batch));
                            }
                            output.session(&cap).give_container(&mut rows);
                        });
                    }
                })
                .capture_into(tx);
            handle
        });
        for ((k, v), t, d) in updates {
            handle.update_at((k, v), t, d);
        }
    });

    let mut got: BTreeMap<(Row, Row), Diff> = BTreeMap::new();
    for event in rx {
        if let Event::Messages(_, rows) = event {
            for ((k, v), _, d) in rows {
                *got.entry((k, v)).or_insert(0) += d;
            }
        }
    }
    got.retain(|_, d| *d != 0);
    got
}

/// Rust reference: net input per (key,val), then the reducer per key → final `(key, reduced_val): 1`.
fn reference<T>(updates: &[((Row, Row), T, Diff)], reducer: &Reducer) -> BTreeMap<(Row, Row), Diff> {
    let mut net: BTreeMap<(i64, i64), Diff> = BTreeMap::new();
    for ((k, v), _, d) in updates {
        if let (DValue::Int(k), DValue::Int(v)) = (k, v) {
            *net.entry((*k, *v)).or_insert(0) += d;
        }
    }
    let mut by_key: BTreeMap<i64, Vec<(i64, Diff)>> = BTreeMap::new();
    for ((k, v), d) in net {
        by_key.entry(k).or_default().push((v, d));
    }
    let mut out: BTreeMap<(Row, Row), Diff> = BTreeMap::new();
    for (k, vals) in by_key {
        let reduced: Option<Row> = match reducer {
            Reducer::Count => {
                let c: Diff = vals.iter().map(|(_, d)| *d).sum();
                (c > 0).then(|| DValue::Tuple(vec![DValue::Int(c)]))
            }
            Reducer::Min => vals.iter().filter(|(_, d)| *d > 0).map(|(v, _)| *v).min().map(DValue::Int),
            Reducer::Distinct => vals.iter().any(|(_, d)| *d > 0).then(DValue::unit),
            Reducer::Collect => unreachable!("not tested here"),
        };
        if let Some(r) = reduced {
            out.insert((DValue::Int(k), r), 1);
        }
    }
    out
}

fn xorshift(s: &mut u64) -> u64 {
    *s ^= *s << 13;
    *s ^= *s >> 7;
    *s ^= *s << 17;
    *s
}

fn main() {
    // Directed: Count, 2 rounds incl. retraction-to-zero (key 2 vanishes; key 1 counts down 2→1).
    let directed: Vec<((Row, Row), u64, Diff)> = vec![
        ((DValue::Int(1), DValue::Int(10)), 0, 1),
        ((DValue::Int(1), DValue::Int(20)), 0, 1),
        ((DValue::Int(2), DValue::Int(30)), 0, 1),
        ((DValue::Int(1), DValue::Int(10)), 1, -1),
        ((DValue::Int(2), DValue::Int(30)), 1, -1),
        ((DValue::Int(3), DValue::Int(40)), 1, 1),
    ];
    assert_eq!(run_corgi(directed.clone(), Reducer::Count), reference(&directed, &Reducer::Count), "directed Count");
    println!("directed Count (retraction-to-zero): OK");

    // Randomized property test: Count over random multi-time ±1 updates (robust to any net diffs).
    let mut seed = 0x51ed_c0_fefe_beadu64;
    for it in 0..120 {
        let n = (xorshift(&mut seed) % 24) as usize + 1;
        let ups: Vec<((Row, Row), u64, Diff)> = (0..n)
            .map(|_| {
                let k = (xorshift(&mut seed) % 5) as i64;
                let v = (xorshift(&mut seed) % 7) as i64;
                let t = xorshift(&mut seed) % 3;
                let d = if xorshift(&mut seed) % 4 == 0 { -1 } else { 1 };
                ((DValue::Int(k), DValue::Int(v)), t, d)
            })
            .collect();
        let got = run_corgi(ups.clone(), Reducer::Count);
        let want = reference(&ups, &Reducer::Count);
        assert_eq!(got, want, "randomized Count iter {it}: {ups:?}");
    }
    println!("randomized Count: 120 cases == reference (random keys/vals/times/retractions)");

    // Directed well-formed Min & Distinct (set semantics, no over-retraction).
    let setlike: Vec<((Row, Row), u64, Diff)> = vec![
        ((DValue::Int(1), DValue::Int(50)), 0, 1),
        ((DValue::Int(1), DValue::Int(30)), 0, 1),
        ((DValue::Int(1), DValue::Int(70)), 1, 1),
        ((DValue::Int(1), DValue::Int(30)), 2, -1), // remove the current min at t2
        ((DValue::Int(2), DValue::Int(9)), 0, 1),
    ];
    assert_eq!(run_corgi(setlike.clone(), Reducer::Min), reference(&setlike, &Reducer::Min), "Min");
    assert_eq!(run_corgi(setlike.clone(), Reducer::Distinct), reference(&setlike, &Reducer::Distinct), "Distinct");
    println!("directed Min & Distinct (min changes under retraction): OK");

    // PARTIAL-ORDER test: the crux of `active_times`. Two updates for key 1 at incomparable times
    // (0,1) and (1,0); the count is 1 at each, but must become 2 at the synthetic join time (1,1) —
    // a time present in NEITHER input. If `active_times` failed to derive (1,1), the final count
    // would be wrong (Tuple[1]:2 instead of Tuple[2]:1).
    // (Partial-order times can't use `new_collection` [needs TotalOrder], so drive `retire` directly
    // over one interval covering everything — this is also the most direct test of `active_times`.)
    type PT = Product<u64, u64>;
    let key_rows = vec![DValue::Int(1), DValue::Int(1), DValue::Int(2)];
    let val_rows = vec![DValue::Int(10), DValue::Int(20), DValue::Int(5)];
    let times: Vec<PT> = vec![Product::new(0, 1), Product::new(1, 0), Product::new(0, 0)];
    let keys = transcode(&key_rows, &infer_shape(&key_rows[0]));
    let vals = transcode(&val_rows, &infer_shape(&val_rows[0]));
    let min = Antichain::from_elem(PT::minimum());
    let batch = Rc::new(CorgiBatch::from_unsorted(keys, vals, times, vec![1, 1, 1], Description::new(min.clone(), Antichain::new(), min.clone())));

    let mut tactic = CorgiReduceTactic::<PT>::new(Reducer::Count);
    let (produced, _frontier) = tactic.retire(vec![], vec![], vec![batch], &min, &Antichain::new(), &min);

    let mut got: BTreeMap<(Row, Row), Diff> = BTreeMap::new();
    for (_ship, b) in &produced {
        for ((k, v), _t, d) in read_batch(b) {
            *got.entry((k, v)).or_insert(0) += d;
        }
    }
    got.retain(|_, d| *d != 0);
    // key1 sees vals 10 and 20 (at incomparable times) → count 2 at the synthetic join (1,1); key2 → 1.
    let want: BTreeMap<(Row, Row), Diff> = [
        ((DValue::Int(1), DValue::Tuple(vec![DValue::Int(2)])), 1),
        ((DValue::Int(2), DValue::Tuple(vec![DValue::Int(1)])), 1),
    ]
    .into_iter()
    .collect();
    assert_eq!(got, want, "partial-order Count via direct retire (synthetic join time)");
    println!("partial-order Count via direct retire (synthetic join (1,1) from (0,1)+(1,0) → count 2): OK  <-- active_times core");

    println!("\nCursor-less corgi REDUCE (active_times) verified live vs reference — the last operator works.");
}
