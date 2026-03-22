//! Test enter_at behavior with PointStamp timestamps at nesting level 2.
//! Wraps propagate inside an outer scope (like SCC does).

use timely::order::Product;
use timely::dataflow::Scope;

use differential_dataflow::VecCollection;
use differential_dataflow::input::Input;
use differential_dataflow::operators::iterate::VecVariable;
use differential_dataflow::dynamic::pointstamp::{PointStamp, PointStampSummary};
use differential_dataflow::dynamic::feedback_summary;

type Diff = i64;
type Row = Vec<u64>;
type Col<G> = VecCollection<G, (Row, Row), Diff>;

fn main() {
    let n: usize = std::env::args().nth(1).unwrap_or("10".into()).parse().unwrap();

    let mut rng = n as u64;
    let lcg = |r: &mut u64| { *r = r.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407); *r };
    let mut edge_list = Vec::new();
    for _ in 0..(2 * n) {
        let src = (lcg(&mut rng) >> 32) % (n as u64);
        let dst = (lcg(&mut rng) >> 32) % (n as u64);
        edge_list.push((src, dst));
    }

    println!("Testing propagate at level 2 with n={}", n);

    timely::execute_directly(move |worker| {
        let (mut edge_input, probe) = worker.dataflow::<u64, _, _>(|scope| {
            let (handle, edges) = scope.new_collection::<(Row, Row), Diff>();
            let mut probe = timely::dataflow::ProbeHandle::new();

            scope.iterative::<PointStamp<u64>, _, _>(|inner| {
                let edges_inner = edges.enter(inner);

                // Outer scope (level 1) — simulates the SCC variable scope.
                // We just use the edges directly as the "cycle" for propagation.
                let outer_step: Product<u64, PointStampSummary<u64>> =
                    Product::new(0, feedback_summary::<u64>(1, 1));
                let (_outer_var, outer_col) = VecVariable::new_from(edges_inner.clone(), outer_step);
                // outer_col = edges (since we're not iterating the outer scope here)

                // Inner propagate at level 2, WITHOUT delay
                let labels_no_delay = {
                    let nodes: Col<_> = edges_inner.clone()
                        .flat_map(|(_src, dst)| Some((dst.clone(), dst)));
                    let step: Product<u64, PointStampSummary<u64>> =
                        Product::new(0, feedback_summary::<u64>(2, 1));
                    let (var, proposals) = VecVariable::new(inner, step);
                    let combined: Col<_> = proposals.concat(nodes);
                    let labels = combined.reduce(|_key, vals, output| {
                        if let Some(min) = vals.iter().map(|(v, _)| *v).min() {
                            output.push((min.clone(), 1));
                        }
                    });
                    let propagated: Col<_> = labels.clone().arrange_by_key()
                        .join_core(outer_col.clone().arrange_by_key(), |_node, label, dst| {
                            Some((dst.clone(), label.clone()))
                        });
                    var.set(propagated);
                    labels.leave_dynamic(2)
                };

                // Inner propagate at level 2, WITH delay at PointStamp index 1
                let labels_delay = {
                    let nodes: Col<_> = edges_inner.clone()
                        .join_function(|(_src, dst): (Row, Row)| {
                            let delay = 256 * (64 - dst.first().copied().unwrap_or(0).leading_zeros() as u64);
                            let time = Product::new(0u64, PointStamp::new(smallvec::smallvec![0u64, delay]));
                            vec![((dst.clone(), dst), time, 1)]
                        });
                    let step: Product<u64, PointStampSummary<u64>> =
                        Product::new(0, feedback_summary::<u64>(2, 1));
                    let (var, proposals) = VecVariable::new(inner, step);
                    let combined: Col<_> = proposals.concat(nodes);
                    let labels = combined.reduce(|_key, vals, output| {
                        if let Some(min) = vals.iter().map(|(v, _)| *v).min() {
                            output.push((min.clone(), 1));
                        }
                    });
                    let propagated: Col<_> = labels.clone().arrange_by_key()
                        .join_core(outer_col.arrange_by_key(), |_node, label, dst| {
                            Some((dst.clone(), label.clone()))
                        });
                    var.set(propagated);
                    labels.leave_dynamic(2)
                };

                // Don't iterate outer — just bind to itself.
                _outer_var.set(edges_inner.clone());

                // Diff the two
                let diff = labels_no_delay.clone().negate().concat(labels_delay.clone());
                diff.leave()
                    .consolidate()
                    .inspect(|x| eprintln!("  DIFF: {:?}", x))
                    .probe_with(&mut probe);
            });

            (handle, probe)
        });

        for (src, dst) in &edge_list {
            edge_input.update((vec![*src], vec![*dst]), 1);
        }
        edge_input.advance_to(1);
        edge_input.flush();
        while probe.less_than(&1u64) {
            worker.step();
        }

        println!("Done.");
    });
}
