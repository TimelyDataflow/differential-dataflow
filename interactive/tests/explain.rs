//! End-to-end semantic tests for the explanation rewrite, built on
//! `backend::vec::evaluate` (explicit inputs in, every export out).
//!
//! The central property is *sufficiency*: for a query against a program's
//! output, the demand-sets the rewritten program reports must — fed back
//! through the original program as its only inputs — regenerate the queried
//! output row. Tests marked `#[ignore]` are heavier sweeps meant for
//! `cargo test --release -- --ignored`.

use std::collections::BTreeSet;

use interactive::backend::vec::{evaluate, Row};
use interactive::scope_ir::Program;
use interactive::{explain, lower, parse};

/// SCC with the scc edge-set itself as the result, so individual output
/// edges are queryable. Mirrors `examples/programs/scc.ddp` minus the final
/// `map(;)` aggregation.
const SCC_ROW: &str = r#"
    let edges = input 0 | key($0[0] ; $0[1]);
    let trans = edges | key($1 ; $0);
    outer: {
        let scc = edges + trim;
        fwd: {
            let nodes = edges | key($1 ; $1) | enter_at($1[0]);
            let labels = proposals + nodes | min;
            var proposals = labels | join(scc, ($2 ; $1));
        }
        let trim_fwd = edges
            | join(fwd::labels, ($1 ; $0, $2))
            | join(fwd::labels, ($0 ; $1, $2))
            | filter($1[1] == $1[2])
            | key($0 ; $1[0]);
        bwd: {
            let nodes = trans | key($1 ; $1) | enter_at($1[0]);
            let labels = proposals + nodes | min;
            var proposals = labels | join(trim_fwd, ($2 ; $1));
        }
        let trim_bwd = trans
            | join(bwd::labels, ($1 ; $0, $2))
            | join(bwd::labels, ($0 ; $1, $2))
            | filter($1[1] == $1[2])
            | key($0 ; $1[0]);
        var trim = trim_bwd - edges;
    }
    export "result" = outer::scc;
"#;
const SCC_SHAPES: &[(usize, usize)] = &[(2, 0)];

/// Transitive closure, with the closure's pairs as the result.
const TC_ROW: &str = r#"
    let edges = input 0 | key($0[0] ; $0[1]);
    outer: {
        let tc = edges + more
            | key($0[0], $1[0] ;)
            | distinct
            | key($0[0] ; $0[1]);
        var more = tc
            | key($1 ; $0)
            | join(edges, ($1 ; $2));
    }
    export "result" = outer::tc;
"#;
const TC_SHAPES: &[(usize, usize)] = &[(2, 0)];

/// Reachability from roots (two inputs), the reached set as the result.
const REACH_ROW: &str = r#"
    let edges = input 0 | key($0[0] ; $0[1]);
    let roots = input 1 | key($0[0] ;);
    reach: {
        let proposals = reach | join(edges, ($2 ;));
        var reach = roots + proposals | distinct;
    }
    export "result" = reach::reach;
"#;
const REACH_SHAPES: &[(usize, usize)] = &[(2, 0), (1, 0)];

fn lowered(src: &str) -> Program {
    lower::lower_tree(parse::pipe::parse(src))
}

fn gen_edges(nodes: u64, edges: u64) -> Vec<(Row, Row)> {
    (0..edges).map(|e| interactive::gen_row::<Row>(e, nodes, 2)).collect()
}

/// A different deterministic graph per seed (offset into the same hash space).
fn gen_edges_seeded(seed: u64, nodes: u64, edges: u64) -> Vec<(Row, Row)> {
    (0..edges)
        .map(|e| interactive::gen_row::<Row>(seed.wrapping_mul(1_000_003).wrapping_add(e), nodes, 2))
        .collect()
}

fn row(fields: &[i64]) -> Row {
    fields.iter().copied().collect()
}

/// Run `p` on `inputs` and return one export's rows (asserting positive
/// multiplicities — a set-like result).
fn export_rows(p: &Program, inputs: &[Vec<(Row, Row)>], export: &str) -> BTreeSet<(Row, Row)> {
    let exports = evaluate(p, inputs);
    exports
        .get(export)
        .unwrap_or_else(|| panic!("no export {:?}; have {:?}", export, exports.keys().collect::<Vec<_>>()))
        .iter()
        .map(|((k, v), d)| {
            assert!(*d > 0, "negative multiplicity {} for {:?}", d, (k, v));
            (k.clone(), v.clone())
        })
        .collect()
}

fn optimized(src: &str) -> Program {
    let mut p = lowered(src);
    p.optimize();
    p
}

/// The per-input demand-sets for a batch of query rows (q ids assigned in
/// order) against `src`'s first export, with `src` run on `inputs`.
fn demand_for_queries(
    src: &str,
    shapes: &[(usize, usize)],
    inputs: &[Vec<(Row, Row)>],
    queries: &[(Row, Row)],
) -> Vec<Vec<(Row, Row)>> {
    let tree = lowered(src);
    let mut ex = explain::explain(&tree, shapes);
    ex.optimize();
    let query_rows: Vec<(Row, Row)> = queries
        .iter()
        .enumerate()
        .map(|(q, (k, v))| {
            let mut val = v.clone();
            val.push(q as i64); // query id
            (k.clone(), val)
        })
        .collect();
    let mut ex_inputs: Vec<Vec<(Row, Row)>> = inputs.to_vec();
    ex_inputs.push(query_rows);
    let exports = evaluate(&ex, &ex_inputs);
    (0..inputs.len())
        .map(|i| {
            let name = format!("demand:input{}", i);
            exports
                .get(&name)
                .unwrap_or_else(|| panic!("no export {:?}; have {:?}", name, exports.keys().collect::<Vec<_>>()))
                .iter()
                .map(|((k, v), d)| {
                    assert!(*d > 0, "negative multiplicity {} for {:?}", d, (k, v));
                    (k.clone(), v.clone())
                })
                .collect()
        })
        .collect()
}

/// Single-query convenience over [`demand_for_queries`].
fn demand_for(
    src: &str,
    shapes: &[(usize, usize)],
    inputs: &[Vec<(Row, Row)>],
    q_key: &Row,
    q_val: &Row,
) -> Vec<Vec<(Row, Row)>> {
    demand_for_queries(src, shapes, inputs, &[(q_key.clone(), q_val.clone())])
}

fn demand_total(demand: &[Vec<(Row, Row)>]) -> usize {
    demand.iter().map(|d| d.len()).sum()
}

/// Sufficiency for every row of `src`'s output on `inputs`: query it, take
/// the demand-sets, re-run the original program on the demand-sets alone,
/// and require the queried row in the output. Returns (row, total demand)
/// per query.
fn assert_all_rows_sufficient(
    src: &str,
    shapes: &[(usize, usize)],
    inputs: &[Vec<(Row, Row)>],
) -> Vec<((Row, Row), usize)> {
    let p = optimized(src);
    let result = export_rows(&p, inputs, "result");
    let mut sizes = Vec::new();
    for (k, v) in &result {
        let demand = demand_for(src, shapes, inputs, k, v);
        let replay = export_rows(&p, &demand, "result");
        assert!(
            replay.contains(&(k.clone(), v.clone())),
            "insufficient explanation for {:?}: demanded {:?} regenerates only {:?}",
            (k, v), demand, replay,
        );
        sizes.push(((k.clone(), v.clone()), demand_total(&demand)));
    }
    sizes
}

/// `clone_identity` must preserve a program's outputs.
#[test]
fn clone_identity_preserves_scc_output() {
    let edges = gen_edges(50, 55);
    let p = optimized(SCC_ROW);
    let mut c = explain::clone_identity(&lowered(SCC_ROW));
    c.optimize();
    assert_eq!(
        export_rows(&p, &[edges.clone()], "result"),
        export_rows(&c, &[edges], "result"),
    );
}

/// Every scc edge of the 50-node / 55-edge instance (four small components
/// plus one self-loop) is explained by a demand-set that regenerates it.
#[test]
fn scc_row_explanations_sufficient_small() {
    let sizes = assert_all_rows_sufficient(SCC_ROW, SCC_SHAPES, &[gen_edges(50, 55)]);
    assert_eq!(sizes.len(), 12, "expected 12 scc edges at 50/55");
}

/// Every pair of the 20-node / 22-edge transitive closure is explained by a
/// demand-set that regenerates it.
#[test]
fn tc_row_explanations_sufficient_small() {
    let sizes = assert_all_rows_sufficient(TC_ROW, TC_SHAPES, &[gen_edges(20, 22)]);
    assert_eq!(sizes.len(), 34, "expected 34 tc pairs at 20/22");
}

/// Every node reached from root 0 in the 50-node / 55-edge instance is
/// explained by demand-sets (edges and roots) that regenerate it.
#[test]
fn reach_row_explanations_sufficient_small() {
    let inputs = vec![gen_edges(50, 55), vec![(row(&[0]), Row::new())]];
    let sizes = assert_all_rows_sufficient(REACH_ROW, REACH_SHAPES, &inputs);
    assert_eq!(sizes.len(), 5, "expected 5 reached nodes at 50/55 from root 0");
}

/// A demand-set is grounded in actual input rows.
#[test]
fn demand_is_a_subset_of_the_input() {
    let edges = gen_edges(50, 55);
    let p = optimized(SCC_ROW);
    let result = export_rows(&p, &[edges.clone()], "result");
    let edge_set: BTreeSet<(Row, Row)> = edges.iter().cloned().collect();
    let (k, v) = result.iter().next().unwrap();
    for set in demand_for(SCC_ROW, SCC_SHAPES, &[edges.clone()], k, v) {
        for row in set {
            assert!(edge_set.contains(&row), "demanded row {:?} is not an input row", row);
        }
    }
}

/// The full 100-node / 110-edge sweep: 22 scc edges, each sufficient.
#[test]
#[ignore = "heavier sweep; run with --release -- --ignored"]
fn scc_row_explanations_sufficient_100() {
    let sizes = assert_all_rows_sufficient(SCC_ROW, SCC_SHAPES, &[gen_edges(100, 110)]);
    assert_eq!(sizes.len(), 22, "expected 22 scc edges at 100/110");
}

/// Regression for the join backward rule's partner-time cancellation
/// (explain-join-time-filters): at 1000 nodes / 1100 edges, the scc edge
/// (773, 466) sits on a 13-node cycle whose label-flood justification pairs
/// `labels(25)=0` with the scc edge `(25,236)`; scc retracts that edge at
/// outer iteration 1, and without both-side time filters the +1/-1 pair rows
/// cancel after projection, the flood path is never demanded, and the
/// demand-set fails to regenerate the queried row.
#[test]
#[ignore = "heavier instance; run with --release -- --ignored"]
fn scc_join_partner_time_regression() {
    let edges = gen_edges(1000, 1100);
    let p = optimized(SCC_ROW);
    let demand = demand_for(SCC_ROW, SCC_SHAPES, &[edges], &row(&[773]), &row(&[466]));
    let replay = export_rows(&p, &demand, "result");
    assert!(
        replay.contains(&(row(&[773]), row(&[466]))),
        "insufficient explanation for (773, 466): demanded {} rows regenerate only {:?}",
        demand_total(&demand), replay,
    );
}

/// Greedy 1-minimal shrink across all inputs' demand-sets: drop demanded
/// rows while the replay still regenerates `target`. The result is locally
/// minimal (no single row can be removed), a practical lower-bound estimate
/// for measuring excess demand.
fn greedy_shrink(
    p: &Program,
    demand: &[Vec<(Row, Row)>],
    target: &(Row, Row),
) -> Vec<Vec<(Row, Row)>> {
    let mut keep: Vec<Vec<(Row, Row)>> = demand.to_vec();
    for input in 0..keep.len() {
        let mut i = 0;
        while i < keep[input].len() {
            let mut trial = keep.clone();
            trial[input].remove(i);
            if export_rows(p, &trial, "result").contains(target) {
                keep = trial;
            } else {
                i += 1;
            }
        }
    }
    keep
}

/// Not an assertion — a report. Prints per-program total demand vs greedy
/// 1-minimal totals, the working metric for the over-approximation work.
/// Run with: cargo test --release -- --ignored report_demand_excess --nocapture
#[test]
#[ignore = "metric report; run with --release -- --ignored --nocapture"]
fn report_demand_excess() {
    let cases: Vec<(&str, &str, &[(usize, usize)], Vec<Vec<(Row, Row)>>)> = vec![
        ("scc 50/55", SCC_ROW, SCC_SHAPES, vec![gen_edges(50, 55)]),
        ("scc 100/110", SCC_ROW, SCC_SHAPES, vec![gen_edges(100, 110)]),
        ("tc 20/22", TC_ROW, TC_SHAPES, vec![gen_edges(20, 22)]),
        ("tc 50/55", TC_ROW, TC_SHAPES, vec![gen_edges(50, 55)]),
        ("reach 50/55", REACH_ROW, REACH_SHAPES, vec![gen_edges(50, 55), vec![(row(&[0]), Row::new())]]),
    ];
    for (name, src, shapes, inputs) in cases {
        let p = optimized(src);
        let result = export_rows(&p, &inputs, "result");
        let (mut tot_d, mut tot_m, mut worst): (usize, usize, f64) = (0, 0, 1.0);
        for (k, v) in &result {
            let demand = demand_for(src, shapes, &inputs, k, v);
            let min = greedy_shrink(&p, &demand, &(k.clone(), v.clone()));
            let (d, m) = (demand_total(&demand), demand_total(&min));
            tot_d += d;
            tot_m += m;
            worst = worst.max(d as f64 / m as f64);
        }
        println!(
            "{:>12}: {:>4} queries, demand {:>5} vs 1-minimal {:>5} ({:.2}x avg, {:.2}x worst)",
            name, result.len(), tot_d, tot_m, tot_d as f64 / tot_m as f64, worst,
        );
    }
}

/// In-degree counts: the one `count` reducer case. With duplicate-free
/// inputs the keyed all-rows demand happens to be exactly what the count
/// needs.
const INDEG_ROW: &str = r#"
    let edges = input 0 | key($0[0] ; $0[1]);
    export "result" = edges | key($1 ;) | count;
"#;
const INDEG_SHAPES: &[(usize, usize)] = &[(2, 0)];

/// Count outputs over duplicate-free inputs: sufficient today, because the
/// keyed lookup demands every input row at the key, which is precisely the
/// count's multiset.
#[test]
fn count_explanations_sufficient_small() {
    let sizes = assert_all_rows_sufficient(INDEG_ROW, INDEG_SHAPES, &[gen_edges(20, 22)]);
    assert!(!sizes.is_empty());
}

/// KNOWN GAP: demand-sets are *sets*, but count outputs depend on input
/// *multiplicities*. With a duplicated input row, indeg(5) = 3 needs the
/// row (1,5) twice, and the demand-set can only say it once — the replay
/// produces indeg(5) = 2 and the queried row is not regenerated. This is
/// the motivating case for a multiplicity ("diff") dimension on demand.
/// Asserts sufficiency, so it FAILS until that lands; un-ignore it then.
#[test]
#[ignore = "known gap: count needs multiplicity-aware demand"]
fn count_duplicate_inputs_known_gap() {
    let edges = vec![
        (row(&[1, 5]), Row::new()),
        (row(&[1, 5]), Row::new()),
        (row(&[2, 5]), Row::new()),
    ];
    let p = optimized(INDEG_ROW);
    let result = export_rows(&p, &[edges.clone()], "result");
    assert!(result.contains(&(row(&[5]), row(&[3]))), "expected indeg(5) = 3 in {:?}", result);
    let demand = demand_for(INDEG_ROW, INDEG_SHAPES, &[edges], &row(&[5]), &row(&[3]));
    let replay = export_rows(&p, &demand, "result");
    assert!(
        replay.contains(&(row(&[5]), row(&[3]))),
        "insufficient explanation for indeg(5) = 3: demanded {:?} regenerates only {:?}",
        demand, replay,
    );
}

/// Two queries seeded together (distinct q ids): the union demand-set must
/// regenerate both queried rows. Exercises the q-id plumbing through every
/// reverse rule — a mismatched q in a lookup would silently drop demand.
#[test]
fn two_simultaneous_queries_sufficient() {
    let edges = gen_edges(50, 55);
    let p = optimized(SCC_ROW);
    let result: Vec<(Row, Row)> = export_rows(&p, &[edges.clone()], "result").into_iter().collect();
    // Two edges from different components.
    let q0 = (row(&[7]), row(&[44]));
    let q1 = (row(&[16]), row(&[19]));
    assert!(result.contains(&q0) && result.contains(&q1));
    let demand = demand_for_queries(SCC_ROW, SCC_SHAPES, &[edges], &[q0.clone(), q1.clone()]);
    let replay = export_rows(&p, &demand, "result");
    assert!(replay.contains(&q0), "union demand fails first query: {:?}", replay);
    assert!(replay.contains(&q1), "union demand fails second query: {:?}", replay);
}

/// Seeded sufficiency sweep: many graphs, every output row queried. The
/// soundness bugs found so far surfaced only at particular scales/instances;
/// this is the standing net for the next one.
#[test]
#[ignore = "fuzz sweep; run with --release -- --ignored"]
fn fuzz_explanations_sufficient() {
    let mut queries = 0;
    for seed in 0..10u64 {
        queries += assert_all_rows_sufficient(SCC_ROW, SCC_SHAPES, &[gen_edges_seeded(seed, 80, 88)]).len();
        queries += assert_all_rows_sufficient(TC_ROW, TC_SHAPES, &[gen_edges_seeded(seed, 25, 27)]).len();
        let root = (seed % 60) as i64;
        let inputs = vec![gen_edges_seeded(seed, 60, 66), vec![(row(&[root]), Row::new())]];
        queries += assert_all_rows_sufficient(REACH_ROW, REACH_SHAPES, &inputs).len();
    }
    assert!(queries >= 100, "fuzz swept only {} queries — instances too sparse to mean much", queries);
}
