//! End-to-end semantic tests for the explanation rewrite, built on
//! `server::evaluate` (explicit inputs in, every export out). The
//! sufficiency properties use that row execution to evaluate the rewrite; a
//! final section cross-checks the behavior shared by the row and corgi
//! implementations for these programs.
//!
//! The central property is *sufficiency*: for a query against a program's
//! output, the original inputs *restricted to* the demand-sets the rewritten
//! program reports — keeping each demanded row at its original multiplicity,
//! exactly as the rewrite's own forward clone does via semijoin — must
//! regenerate the queried output row. Tests marked `#[ignore]` are heavier sweeps meant for
//! `cargo test --release -- --ignored`.

use std::collections::{BTreeMap, BTreeSet};

use interactive::scope_ir::Program;
use interactive::server;
use interactive::{explain, lower, parse};

/// SCC with the scc edge-set itself as the result, so individual output
/// edges are queryable. Mirrors `examples/programs/scc.ddp` minus the final
/// aggregation, with its nodes as one-field tuples. It stays a copy rather than an
/// `include_str!`: the tests query result edges by one-field keys (`row(&[773])`),
/// where `scc.ddp` keys bare integers and counts its result.
const SCC_ROW: &str = r#"
    let edges = input 0 : ((int, int), ()) | map(((src, dst), ()) -> ((src), (dst)));
    let trans = edges | map((src, dst) -> (dst, src));
    outer: {
        let scc = edges + trim;
        fwd: {
            let nodes = edges | map((_, dst) -> (dst, dst)) | enter_at((_, (n)) -> n);
            let labels = proposals + nodes | min;
            var proposals = labels | join(scc, (_, label, dst) -> (dst, label));
        }
        let trim_fwd = edges
            | join(fwd::labels, ((src), (dst), (l)) -> ((dst), (src, l)))
            | join(fwd::labels, (dst, (src, l), (m)) -> (dst, (src, l, m)))
            | filter((_, (_, l, m)) -> (l, m) eq)
            | map((dst, (src, _, _)) -> (dst, (src)));
        bwd: {
            let nodes = trans | map((_, dst) -> (dst, dst)) | enter_at((_, (n)) -> n);
            let labels = proposals + nodes | min;
            var proposals = labels | join(trim_fwd, (_, label, dst) -> (dst, label));
        }
        let trim_bwd = trans
            | join(bwd::labels, ((src), (dst), (l)) -> ((dst), (src, l)))
            | join(bwd::labels, (dst, (src, l), (m)) -> (dst, (src, l, m)))
            | filter((_, (_, l, m)) -> (l, m) eq)
            | map((dst, (src, _, _)) -> (dst, (src)));
        var trim = trim_bwd - edges;
    }
    export "result" = outer::scc;
"#;

/// Transitive closure, with the closure's pairs as the result.
const TC_ROW: &str = r#"
    let edges = input 0 : ((int, int), ()) | map(((src, dst), ()) -> ((src), (dst)));
    outer: {
        let tc = edges + more
            | map(((a), (b)) -> ((a, b), ()))
            | distinct
            | map(((a, b), ()) -> ((a), (b)));
        var more = tc
            | map((a, b) -> (b, a))
            | join(edges, (_, a, c) -> (a, c));
    }
    export "result" = outer::tc;
"#;

/// Reachability from roots (two inputs), the reached set as the result.
const REACH_ROW: &str = r#"
    let edges = input 0 : ((int, int), ()) | map(((src, dst), ()) -> ((src), (dst)));
    let roots = input 1 : ((int), ()) | map(((n), ()) -> ((n), ()));
    reach: {
        let proposals = reach | join(edges, (_, _, dst) -> (dst, ()));
        var reach = roots + proposals | distinct;
    }
    export "result" = reach::reach;
"#;

fn lowered(src: &str) -> Program {
    lower::lower_tree(parse::pipe::parse(src))
}

fn gen_edges(nodes: u64, edges: u64) -> Vec<(Row, Row)> {
    gen_edges_seeded(0, nodes, edges)
}

/// A different deterministic graph per seed (offset into the same hash space).
fn gen_edges_seeded(seed: u64, nodes: u64, edges: u64) -> Vec<(Row, Row)> {
    (0..edges)
        .map(|e| seed.wrapping_mul(1_000_003).wrapping_add(e))
        .map(|e| (row(&[0, 1].map(|col| interactive::server::gen_field_seeded(0, e, nodes, col))), unit()))
        .collect()
}

/// A row value: a tuple of integers.
fn row(fields: &[i64]) -> Row {
    format!("tuple({})", fields.iter().map(i64::to_string).collect::<Vec<_>>().join(","))
}

fn int(n: i64) -> Row { n.to_string() }
fn unit() -> Row { "tuple()".into() }

/// A row: a key or a value, as a feed writes it.
type Row = String;

/// Run `p` on `inputs` through the server: every export's consolidated rows, by name.
fn evaluate(p: &Program, inputs: &[Vec<(Row, Row)>]) -> BTreeMap<String, Vec<((Row, Row), i64)>> {
    let lines: Vec<Vec<String>> = inputs.iter().map(|rows| rows.iter().map(|(k, v)| format!("{k} val={v}")).collect()).collect();
    server::evaluate(timely::Config::process(1), p, &lines)
        .into_iter()
        .map(|(name, updates)| (name, updates.rows().into_iter().map(|(k, v, d)| ((k, v), d)).collect()))
        .collect()
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

/// The query input's rows: each query is a demand row `(K, (V, q))` — the
/// queried row, and its query id.
fn query_rows(queries: &[(Row, Row)]) -> Vec<(Row, Row)> {
    queries
        .iter()
        .enumerate()
        .map(|(q, (k, v))| (k.clone(), format!("tuple({v},{q})")))
        .collect()
}

/// The per-input demand-sets for a batch of query rows (q ids assigned in
/// order) against `src`'s first export, with `src` run on `inputs`.
fn demand_for_queries(
    src: &str,
    inputs: &[Vec<(Row, Row)>],
    queries: &[(Row, Row)],
) -> Vec<Vec<(Row, Row)>> {
    let tree = lowered(src);
    let mut ex = explain::explain(&tree);
    ex.optimize();
    let mut ex_inputs: Vec<Vec<(Row, Row)>> = inputs.to_vec();
    ex_inputs.push(query_rows(queries));
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
    inputs: &[Vec<(Row, Row)>],
    q_key: &Row,
    q_val: &Row,
) -> Vec<Vec<(Row, Row)>> {
    demand_for_queries(src, inputs, &[(q_key.clone(), q_val.clone())])
}

fn demand_total(demand: &[Vec<(Row, Row)>]) -> usize {
    demand.iter().map(|d| d.len()).sum()
}

/// Restrict `inputs` to the demanded rows, preserving original
/// multiplicities — the contract of demand ("this row, at its reference
/// count"), and what the rewrite's forward clone does via semijoin.
fn restrict(inputs: &[Vec<(Row, Row)>], demand: &[Vec<(Row, Row)>]) -> Vec<Vec<(Row, Row)>> {
    inputs
        .iter()
        .zip(demand)
        .map(|(rows, dem)| {
            let set: BTreeSet<&(Row, Row)> = dem.iter().collect();
            rows.iter().filter(|r| set.contains(r)).cloned().collect()
        })
        .collect()
}

/// Sufficiency for every row of `src`'s output on `inputs`: query it, take
/// the demand-sets, re-run the original program on the demand-sets alone,
/// and require the queried row in the output. Returns (row, total demand)
/// per query.
fn assert_all_rows_sufficient(
    src: &str,
    inputs: &[Vec<(Row, Row)>],
) -> Vec<((Row, Row), usize)> {
    let p = optimized(src);
    let result = export_rows(&p, inputs, "result");
    let mut sizes = Vec::new();
    for (k, v) in &result {
        let demand = demand_for(src, inputs, k, v);
        let replay = export_rows(&p, &restrict(inputs, &demand), "result");
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
    let sizes = assert_all_rows_sufficient(SCC_ROW, &[gen_edges(50, 55)]);
    assert_eq!(sizes.len(), 12, "expected 12 scc edges at 50/55");
}

/// Every pair of the 20-node / 22-edge transitive closure is explained by a
/// demand-set that regenerates it.
#[test]
fn tc_row_explanations_sufficient_small() {
    let sizes = assert_all_rows_sufficient(TC_ROW, &[gen_edges(20, 22)]);
    assert_eq!(sizes.len(), 34, "expected 34 tc pairs at 20/22");
}

/// Every node reached from root 0 in the 50-node / 55-edge instance is
/// explained by demand-sets (edges and roots) that regenerate it.
#[test]
fn reach_row_explanations_sufficient_small() {
    let inputs = vec![gen_edges(50, 55), vec![(row(&[0]), unit())]];
    let sizes = assert_all_rows_sufficient(REACH_ROW, &inputs);
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
    for set in demand_for(SCC_ROW, &[edges.clone()], k, v) {
        for row in set {
            assert!(edge_set.contains(&row), "demanded row {:?} is not an input row", row);
        }
    }
}

/// The full 100-node / 110-edge sweep: 22 scc edges, each sufficient.
#[test]
#[ignore = "heavier sweep; run with --release -- --ignored"]
fn scc_row_explanations_sufficient_100() {
    let sizes = assert_all_rows_sufficient(SCC_ROW, &[gen_edges(100, 110)]);
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
    let demand = demand_for(SCC_ROW, &[edges.clone()], &row(&[773]), &row(&[466]));
    let replay = export_rows(&p, &restrict(&[edges], &demand), "result");
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
    inputs: &[Vec<(Row, Row)>],
    demand: &[Vec<(Row, Row)>],
    target: &(Row, Row),
) -> Vec<Vec<(Row, Row)>> {
    let mut keep: Vec<Vec<(Row, Row)>> = demand.to_vec();
    for input in 0..keep.len() {
        let mut i = 0;
        while i < keep[input].len() {
            let mut trial = keep.clone();
            trial[input].remove(i);
            if export_rows(p, &restrict(inputs, &trial), "result").contains(target) {
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
    let cases: Vec<(&str, &str, Vec<Vec<(Row, Row)>>)> = vec![
        ("scc 50/55", SCC_ROW, vec![gen_edges(50, 55)]),
        ("scc 100/110", SCC_ROW, vec![gen_edges(100, 110)]),
        ("tc 20/22", TC_ROW, vec![gen_edges(20, 22)]),
        ("tc 50/55", TC_ROW, vec![gen_edges(50, 55)]),
        ("reach 50/55", REACH_ROW, vec![gen_edges(50, 55), vec![(row(&[0]), unit())]]),
    ];
    for (name, src, inputs) in cases {
        let p = optimized(src);
        let result = export_rows(&p, &inputs, "result");
        let (mut tot_d, mut tot_m, mut worst): (usize, usize, f64) = (0, 0, 1.0);
        for (k, v) in &result {
            let demand = demand_for(src, &inputs, k, v);
            let min = greedy_shrink(&p, &inputs, &demand, &(k.clone(), v.clone()));
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
    let edges = input 0 : ((int, int), ()) | map(((src, dst), ()) -> ((src), (dst)));
    export "result" = edges | map((_, dst) -> (dst, ())) | count;
"#;

/// A declared input the program never reads: no demand reaches it, so its demand set binds only
/// to itself, which gives it no shape, and the explained program would not install. The set
/// takes the input's declared shape, and stays empty. (Found by `difftest`.)
#[test]
fn unread_input_explanations_sufficient() {
    const UNREAD: &str = r#"
        let edges = input 0 : ((int, int), ()) | map(((src, dst), ()) -> ((src), (dst)));
        let unread = input 1 : ((int, int), ());
        export "result" = edges;
    "#;
    let sizes = assert_all_rows_sufficient(UNREAD, &[gen_edges(10, 12), vec![]]);
    assert!(!sizes.is_empty());
}

/// A loop the first export does not read: no query's demand reaches its variable, whose demand
/// then binds only to demand derived from itself, which gives it no shape, and the explained
/// program would not install. The variable's demand declares its shape. (Found by `difftest`.)
#[test]
fn unread_loop_explanations_sufficient() {
    const UNREAD: &str = r#"
        let edges = input 0 : ((int, int), ()) | map(((src, dst), ()) -> ((src), (dst)));
        let roots = edges | map((src, _) -> (src, ()));
        reach: {
            let step = x | join(edges, (_, (), dst) -> (dst, ()));
            var x = (roots + step) | distinct;
        }
        export "result" = edges;
        export "reach" = reach::x;
    "#;
    let sizes = assert_all_rows_sufficient(UNREAD, &[gen_edges(10, 12)]);
    assert!(!sizes.is_empty());
}

/// A scope that reads another scope's result. Their rounds are their own, so demand crossing from
/// one to the other keeps only the coordinates of the scopes they share, here none. It used to
/// carry the reader's round into the other scope as if it were that scope's own round, found no
/// rows there, and came back empty. (Found by `difftest`.)
#[test]
fn sibling_scope_explanations_sufficient() {
    const SIBLINGS: &str = r#"
        let edges = input 0 : ((int, int), ()) | map(((src, dst), ()) -> ((src), (dst)));
        let roots = edges | map((src, _) -> (src, ()));
        reach: {
            let step = x | join(edges, (_, (), dst) -> (dst, ()));
            var x = (roots + step) | distinct;
        }
        let reached = reach::x;
        again: {
            let step = y | join(edges, (_, (), dst) -> (dst, ()));
            var y = (reached + step) | distinct;
        }
        export "result" = again::y;
    "#;
    let sizes = assert_all_rows_sufficient(SIBLINGS, &[gen_edges(10, 12)]);
    assert!(!sizes.is_empty());
}

/// A flatmap inside a loop over a collection of the enclosing scope: its input's chain is shorter
/// than the demand's. The lookup assumed one depth for both, and built a join the explained program
/// could not type. (Found by `difftest`.)
#[test]
fn flatmap_of_a_parent_explanations_sufficient() {
    const PARENT: &str = r#"
        let edges = input 0 : ((int, int), ()) | map(((src, dst), ()) -> ((src), (dst)));
        let roots = edges | map((src, _) -> (src, ()));
        reach: {
            let hops = edges | flatmap((_, (dst)) -> (dst enlist, (dst, 1) add enlist) append) | map((src, (_, dst)) -> (src, (dst)));
            let step = x | join(hops, (_, (), (dst)) -> ((dst), ()));
            var x = (roots + step) | distinct;
        }
        export "result" = reach::x;
    "#;
    let sizes = assert_all_rows_sufficient(PARENT, &[gen_edges(10, 12)]);
    assert!(!sizes.is_empty());
}

/// Demand is a set: two paths into one collection must not cancel. `r2` holds a negated row, count
/// −1, which the join reads directly and through `r3`'s map; the demand arriving by the two paths
/// came back as +1 and −1 and cancelled, leaving the result row with an empty explanation. (Found
/// by `difftest`.)
#[test]
fn negated_fanout_explanations_sufficient() {
    const NEGATED: &str = r#"
        let i0 = input 0 : ((int, int), ());
        let r2 = i0 | negate | map(((a, b), ()) -> (b, b));
        let r3 = r2 | map((x, y) -> (y, x));
        export "result" = r3 | join(r2, (k, l, r) -> (k, (l, r)));
    "#;
    let sizes = assert_all_rows_sufficient(NEGATED, &[vec![(row(&[3, 0]), row(&[]))]]);
    assert!(!sizes.is_empty());
}

/// A `min` whose value decides a later key. Labels are 2→4, 4→1 and 1→1. Explaining label 4's
/// count demands node 2's rows; in the restricted run node 4 lacks the edge that gave it label 1,
/// takes label 4 from node 2, and joins label 4's count. The count's lookup demands node 4's row
/// from the restricted run, and the `min` must then demand the inputs achieving node 4's label in
/// the reference, not only those achieving its restricted label. (Found by `difftest`.)
#[test]
fn min_then_count_explanations_sufficient() {
    const MIN_COUNT: &str = r#"
        let i0 = input 0 : (int, int);
        s: { let step = x | join(i0, (k, l, e) -> (e, l)); var x = (i0 + step) | min; }
        export "result" = s::x | map((k, l) -> ((l), ())) | count;
    "#;
    let sizes = assert_all_rows_sufficient(MIN_COUNT, &[vec![(int(2), int(4)), (int(4), int(1))]]);
    assert!(!sizes.is_empty());
}

/// An input with several values at one key. The restricted run read the sources restricted to the
/// demanded rows by key alone, so demanding `(0, 2)` fed it every row at key 0, each once per row
/// at the key, and counted 3 where the demanded rows alone count 1. Its `min` then agreed with the
/// reference by accident, and the inputs that make the count as in the reference were never
/// demanded. (Found by `difftest`.)
#[test]
fn restricted_rows_keep_their_counts() {
    const COUNT_MIN: &str = r#"
        let i0 = input 0 : (int, int);
        let counts = i0 | count;
        let r = (counts - i0) | min;
        export "result" = r;
    "#;
    let inputs = vec![vec![(int(0), int(4)), (int(0), int(4)), (int(0), int(2))]];
    let sizes = assert_all_rows_sufficient(COUNT_MIN, &inputs);
    assert!(!sizes.is_empty());
}

/// Count outputs over duplicate-free inputs: sufficient today, because the
/// keyed lookup demands every input row at the key, which is precisely the
/// count's multiset.
#[test]
fn count_explanations_sufficient_small() {
    let sizes = assert_all_rows_sufficient(INDEG_ROW, &[gen_edges(20, 22)]);
    assert!(!sizes.is_empty());
}

/// Demand means "this row, at its reference count": with a duplicated
/// input row, indeg(5) = 3 demands the *rows* {(1,5), (2,5)}, and the
/// restriction keeps both copies of (1,5) — exactly as the rewrite's
/// forward clone consumes the demand-set via semijoin. (An earlier version
/// of this test replayed the demand-set itself at multiplicity 1 and
/// mistook the resulting indeg(5) = 2 for a soundness gap; the gap was in
/// that oracle, not the rewrite.)
#[test]
fn count_duplicate_inputs_sufficient() {
    let edges = vec![
        (row(&[1, 5]), unit()),
        (row(&[1, 5]), unit()),
        (row(&[2, 5]), unit()),
    ];
    let p = optimized(INDEG_ROW);
    let result = export_rows(&p, &[edges.clone()], "result");
    assert!(result.contains(&(row(&[5]), int(3))), "expected indeg(5) = 3 in {:?}", result);
    let demand = demand_for(INDEG_ROW, std::slice::from_ref(&edges), &row(&[5]), &int(3));
    let replay = export_rows(&p, &restrict(&[edges], &demand), "result");
    assert!(
        replay.contains(&(row(&[5]), int(3))),
        "insufficient explanation for indeg(5) = 3: demanded {:?} regenerates only {:?}",
        demand, replay,
    );
}

/// A reduction by a lambda: the sum of each node's in-neighbors. As for `count`, the keyed lookup
/// demands every input row at the key, which is what the lambda reads.
#[test]
fn reduce_lambda_explanations_sufficient_small() {
    const SUM_ROW: &str = r#"
        let edges = input 0 : ((int, int), ()) | map(((src, dst), ()) -> ((src), (dst)));
        export "result" = edges | map((src, dst) -> (dst, src))
            | reduce((_, vals) -> (((0, vals) fold ((acc, ((v), d)) -> (acc, (v, d) mul) add)), 1) enlist);
    "#;
    let sizes = assert_all_rows_sufficient(SUM_ROW, &[gen_edges(20, 22)]);
    assert!(!sizes.is_empty());
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
    let demand = demand_for_queries(SCC_ROW, &[edges.clone()], &[q0.clone(), q1.clone()]);
    let replay = export_rows(&p, &restrict(&[edges], &demand), "result");
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
        queries += assert_all_rows_sufficient(SCC_ROW, &[gen_edges_seeded(seed, 80, 88)]).len();
        queries += assert_all_rows_sufficient(TC_ROW, &[gen_edges_seeded(seed, 25, 27)]).len();
        let root = (seed % 60) as i64;
        let inputs = vec![gen_edges_seeded(seed, 60, 66), vec![(row(&[root]), unit())]];
        queries += assert_all_rows_sufficient(REACH_ROW, &inputs).len();
    }
    assert!(queries >= 100, "fuzz swept only {} queries — instances too sparse to mean much", queries);
}

/// FlatMap (UNNEST): each input edge `(a, b)` rekeys to `(a, [a, b])` and
/// explodes to `(a, (0, a))` and `(a, (1, b))`. The reverse rule must trace a
/// demanded exploded output back to the input edge that carried it. This is the
/// op that used to `panic!` in explain.
const FLATMAP_ROW: &str = r#"
    let rows = input 0 : ((int, int), ()) | map(((a, b), ()) -> ((a), ((a enlist, b enlist) append)));
    export "result" = rows | flatmap((_, (l)) -> l);
"#;

#[test]
fn flatmap_explanations_sufficient_small() {
    let sizes = assert_all_rows_sufficient(FLATMAP_ROW, &[gen_edges(20, 22)]);
    assert!(!sizes.is_empty(), "expected some exploded outputs to explain");
}

/// Lambdas read their rows whole, so each must see the user's row, not explain's, whose value
/// carries more after the user's fields: reach, with an always-true filter on the edges and a
/// flatmap that reads the key.
const LAMBDAS: &str = r#"
    let edges = input 0 : ((int, int), ()) | map(((src, dst), ()) -> ((src), (dst))) | filter((_, (d)) -> (d, d) eq);
    let roots = input 1 : ((int), ()) | map(((n), ()) -> ((n), ()));
    reach: {
        let proposals = reach | join(edges, (_, _, dst) -> (dst, ()));
        var reach = roots + proposals | distinct;
    }
    export "result" = reach::reach | map((k, _) -> (k, k)) | flatmap(((a), (b)) -> (a enlist, b enlist) append);
"#;

#[test]
fn lambda_explanations_sufficient_small() {
    let inputs = vec![gen_edges(50, 55), vec![(row(&[0]), unit())]];
    let sizes = assert_all_rows_sufficient(LAMBDAS, &inputs);
    assert_eq!(sizes.len(), 10, "expected 5 reached nodes, each in two positions");
}

/// Keys and values that are neither products nor units: a source of scalar
/// keys and values, a scalar value at the root (`value`), a list at the root
/// (`collect`) read by a flatmap, and scalar keys inside an iterative scope
/// (lifted host forms) read whole by a filter.
const SCALARS: &str = r#"
    let pairs = input 0 : (int, int);
    let counted = pairs | filter((_, v) -> (v, 3) gt) | count;
    let lists = pairs | value((_, v) -> (v, 2) mul) | collect;
    let flat = lists | flatmap((_, l) -> l) | value((_, (_, x)) -> x);
    inner: {
        var low = (flat | filter((_, x) -> (x, 15) lt)) + (low | filter((k, _) -> (k, k) eq)) | min;
    }
    export "result" = inner::low | join(counted, (k, l, c) -> (k, (l, c)));
"#;

fn scalar_pairs() -> Vec<(Row, Row)> {
    (0..30).map(|i| (int(i % 7), int((i * 5) % 11))).collect()
}

#[test]
fn scalar_explanations_sufficient_small() {
    let sizes = assert_all_rows_sufficient(SCALARS, &[scalar_pairs()]);
    assert!(!sizes.is_empty(), "expected some rows to explain");
}

/// Collect (NEST): `(a, b)` rows group to `(a, [b…])`. Reversing a demanded
/// list must demand all the members — which is exactly the non-min keyed
/// lookup ("demand all same-key inputs"), so no new rule is needed; this
/// confirms the existing path handles a `List`-valued reducer output.
const COLLECT_ROW: &str = r#"
    let rows = input 0 : ((int, int), ()) | map(((a, b), ()) -> ((a), (b)));
    export "result" = rows | collect;
"#;

#[test]
fn collect_explanations_sufficient_small() {
    let sizes = assert_all_rows_sufficient(COLLECT_ROW, &[gen_edges(20, 22)]);
    assert!(!sizes.is_empty(), "expected some collected lists to explain");
}

// ---------------------------------------------------------------------------
// Explanations of loops: `enter_at`, a `min` in a loop, and SCC's shape at one
// scope, with its feedback negated and not. `LinearOp::LiftIter` is synthesized
// by the rewrite (every `$host:` export is one), so explained programs are what
// exercise it.
// ---------------------------------------------------------------------------

/// `enter_at` through the rewrite, at depth 1.
const REACH_ENTER_AT: &str = r#"
    let edges = input 0 : ((int, int), ()) | map(((src, dst), ()) -> ((src), (dst)));
    let roots = input 1 : ((int), ()) | map(((n), ()) -> ((n), ()));
    reach: {
        let seeds = roots | enter_at(((n), ()) -> n);
        let proposals = reach | join(edges, (_, _, dst) -> (dst, ()));
        var reach = seeds + proposals | distinct;
    }
    export "result" = reach::reach;
"#;

#[test]
fn enter_at_explanations_sufficient() {
    let inputs = vec![gen_edges(50, 55), vec![(row(&[0]), unit())]];
    assert!(!assert_all_rows_sufficient(REACH_ENTER_AT, &inputs).is_empty());
}

/// A `min` reducer in an iterative loop, at depth 1.
const MIN_LOOP: &str = r#"
    let edges = input 0 : ((int, int), ()) | map(((src, dst), ()) -> ((src), (dst)));
    outer: {
        let nodes = edges | map((_, dst) -> (dst, dst));
        let labels = proposals + nodes | min;
        var proposals = labels | join(edges, (_, label, dst) -> (dst, label));
    }
    export "result" = outer::labels;
"#;

#[test]
fn min_loop_explanations_sufficient() {
    assert!(!assert_all_rows_sufficient(MIN_LOOP, &[gen_edges(50, 55)]).is_empty());
}

/// SCC's shape — depth-2 nesting, `min`, three joins, a filtered feedback — with
/// the feedback not negated; the twin below differs only in that line.
const SCC_ONE_SCOPE: &str = r#"
    let edges = input 0 : ((int, int), ()) | map(((src, dst), ()) -> ((src), (dst)));
    outer: {
        let scc = edges + trim;
        fwd: {
            let nodes = edges | map((_, dst) -> (dst, dst));
            let labels = proposals + nodes | min;
            var proposals = labels | join(scc, (_, label, dst) -> (dst, label));
        }
        let trim_fwd = edges
            | join(fwd::labels, ((src), (dst), (l)) -> ((dst), (src, l)))
            | join(fwd::labels, (dst, (src, l), (m)) -> (dst, (src, l, m)))
            | filter((_, (_, l, m)) -> (l, m) eq)
            | map((dst, (src, _, _)) -> (dst, (src)));
        var trim = trim_fwd | filter(((n), _) -> (n, 1000000) gt);
    }
    export "result" = outer::scc;
"#;

/// The same program with `var trim = trim_fwd - edges`.
const SCC_ONE_SCOPE_NEGATED: &str = r#"
    let edges = input 0 : ((int, int), ()) | map(((src, dst), ()) -> ((src), (dst)));
    outer: {
        let scc = edges + trim;
        fwd: {
            let nodes = edges | map((_, dst) -> (dst, dst));
            let labels = proposals + nodes | min;
            var proposals = labels | join(scc, (_, label, dst) -> (dst, label));
        }
        let trim_fwd = edges
            | join(fwd::labels, ((src), (dst), (l)) -> ((dst), (src, l)))
            | join(fwd::labels, (dst, (src, l), (m)) -> (dst, (src, l, m)))
            | filter((_, (_, l, m)) -> (l, m) eq)
            | map((dst, (src, _, _)) -> (dst, (src)));
        var trim = trim_fwd - edges;
    }
    export "result" = outer::scc;
"#;

#[test]
fn scc_one_scope_explanations_sufficient() {
    assert!(!assert_all_rows_sufficient(SCC_ONE_SCOPE, &[gen_edges(50, 55)]).is_empty());
}

#[test]
fn scc_one_scope_negated_explanations_sufficient() {
    assert!(!assert_all_rows_sufficient(SCC_ONE_SCOPE_NEGATED, &[gen_edges(50, 55)]).is_empty());
}

/// Two queries at once, so the query input carries more than one envelope: the joint
/// explanation regenerates both.
#[test]
fn two_query_explanation_sufficient() {
    let inputs = vec![gen_edges(50, 55)];
    let p = optimized(SCC_ROW);
    let qs: Vec<(Row, Row)> = export_rows(&p, &inputs, "result").into_iter().take(2).collect();
    assert_eq!(qs.len(), 2, "expected at least two scc edges to query");
    let demand = demand_for_queries(SCC_ROW, &inputs, &qs);
    let replay = export_rows(&p, &restrict(&inputs, &demand), "result");
    for q in &qs {
        assert!(replay.contains(q), "insufficient joint explanation for {q:?}");
    }
}

/// Declared sources give the query input its shape, and the rewritten program
/// installs with every shape known.
#[test]
fn declared_sources_shape_the_rewrite() {
    use corgi::Shape;
    let ex = explain::explain(&lowered(REACH_ROW));
    let query = ex.root.imports.iter().find(|imp| imp.name == "query").unwrap();
    assert_eq!(query.shape, Some((Shape::Prod(vec![Shape::Int]), Shape::Prod(vec![Shape::Unit, Shape::Int]))));
    // So does a rewrite through nested iterative scopes.
    for ex in [ex, explain::explain(&lowered(SCC_ROW))] {
        let (_, problems) = interactive::shapes::infer(&ex, &|_| None);
        assert!(problems.unknown.is_empty() && problems.conflicts.is_empty(), "{problems:?}");
    }
}
