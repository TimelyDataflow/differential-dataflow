//! End-to-end semantic tests for the explanation rewrite, built on
//! `backend::vec::evaluate` (explicit inputs in, every export out). The
//! sufficiency properties are checked against `vec`, the correctness reference;
//! a final section cross-checks that the rewritten programs render identically
//! on the corgi backend.
//!
//! The central property is *sufficiency*: for a query against a program's
//! output, the original inputs *restricted to* the demand-sets the rewritten
//! program reports — keeping each demanded row at its original multiplicity,
//! exactly as the rewrite's own forward clone does via semijoin — must
//! regenerate the queried output row. Tests marked `#[ignore]` are heavier sweeps meant for
//! `cargo test --release -- --ignored`.

use std::collections::BTreeSet;

use interactive::backend::corgi::evaluate as corgi_evaluate;
use interactive::backend::vec::{evaluate, Row};
use interactive::ir::Value;
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
    (0..edges).map(|e| interactive::gen_row(e, nodes, 2)).collect()
}

/// A different deterministic graph per seed (offset into the same hash space).
fn gen_edges_seeded(seed: u64, nodes: u64, edges: u64) -> Vec<(Row, Row)> {
    (0..edges)
        .map(|e| interactive::gen_row(seed.wrapping_mul(1_000_003).wrapping_add(e), nodes, 2))
        .collect()
}

/// A row value: a `Tuple` of `Int`s.
fn row(fields: &[i64]) -> Row {
    Value::Tuple(fields.iter().map(|&n| Value::Int(n)).collect())
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

/// The query input's rows: each query is the flat demand envelope
/// `(K ; Tuple([V…, chain…, q]))` — V's fields, then the chain (empty, since the
/// first export is at depth 0), then the query id.
fn query_rows(queries: &[(Row, Row)]) -> Vec<(Row, Row)> {
    queries
        .iter()
        .enumerate()
        .map(|(q, (k, v))| {
            let mut fields = match v { Value::Tuple(xs) => xs.clone(), other => vec![other.clone()] };
            fields.push(Value::Int(q as i64));
            (k.clone(), Value::Tuple(fields))
        })
        .collect()
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
    shapes: &[(usize, usize)],
    inputs: &[Vec<(Row, Row)>],
) -> Vec<((Row, Row), usize)> {
    let p = optimized(src);
    let result = export_rows(&p, inputs, "result");
    let mut sizes = Vec::new();
    for (k, v) in &result {
        let demand = demand_for(src, shapes, inputs, k, v);
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
    let inputs = vec![gen_edges(50, 55), vec![(row(&[0]), Value::unit())]];
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
    let demand = demand_for(SCC_ROW, SCC_SHAPES, &[edges.clone()], &row(&[773]), &row(&[466]));
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
    let cases: Vec<(&str, &str, &[(usize, usize)], Vec<Vec<(Row, Row)>>)> = vec![
        ("scc 50/55", SCC_ROW, SCC_SHAPES, vec![gen_edges(50, 55)]),
        ("scc 100/110", SCC_ROW, SCC_SHAPES, vec![gen_edges(100, 110)]),
        ("tc 20/22", TC_ROW, TC_SHAPES, vec![gen_edges(20, 22)]),
        ("tc 50/55", TC_ROW, TC_SHAPES, vec![gen_edges(50, 55)]),
        ("reach 50/55", REACH_ROW, REACH_SHAPES, vec![gen_edges(50, 55), vec![(row(&[0]), Value::unit())]]),
    ];
    for (name, src, shapes, inputs) in cases {
        let p = optimized(src);
        let result = export_rows(&p, &inputs, "result");
        let (mut tot_d, mut tot_m, mut worst): (usize, usize, f64) = (0, 0, 1.0);
        for (k, v) in &result {
            let demand = demand_for(src, shapes, &inputs, k, v);
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
        (row(&[1, 5]), Value::unit()),
        (row(&[1, 5]), Value::unit()),
        (row(&[2, 5]), Value::unit()),
    ];
    let p = optimized(INDEG_ROW);
    let result = export_rows(&p, &[edges.clone()], "result");
    assert!(result.contains(&(row(&[5]), row(&[3]))), "expected indeg(5) = 3 in {:?}", result);
    let demand = demand_for(INDEG_ROW, INDEG_SHAPES, &[edges.clone()], &row(&[5]), &row(&[3]));
    let replay = export_rows(&p, &restrict(&[edges], &demand), "result");
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
    let demand = demand_for_queries(SCC_ROW, SCC_SHAPES, &[edges.clone()], &[q0.clone(), q1.clone()]);
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
        queries += assert_all_rows_sufficient(SCC_ROW, SCC_SHAPES, &[gen_edges_seeded(seed, 80, 88)]).len();
        queries += assert_all_rows_sufficient(TC_ROW, TC_SHAPES, &[gen_edges_seeded(seed, 25, 27)]).len();
        let root = (seed % 60) as i64;
        let inputs = vec![gen_edges_seeded(seed, 60, 66), vec![(row(&[root]), Value::unit())]];
        queries += assert_all_rows_sufficient(REACH_ROW, REACH_SHAPES, &inputs).len();
    }
    assert!(queries >= 100, "fuzz swept only {} queries — instances too sparse to mean much", queries);
}

/// FlatMap (UNNEST): each input edge `(a, b)` rekeys to `(a ; [a, b])` and
/// explodes to `(a ; (0, a))` and `(a ; (1, b))`. The reverse rule must trace a
/// demanded exploded output back to the input edge that carried it. This is the
/// op that used to `panic!` in explain.
const FLATMAP_ROW: &str = r#"
    let rows = input 0 | key($0[0] ; list($0[0], $0[1]));
    export "result" = rows | flatmap($1[0]);
"#;
const FLATMAP_SHAPES: &[(usize, usize)] = &[(2, 0)];

#[test]
fn flatmap_explanations_sufficient_small() {
    let sizes = assert_all_rows_sufficient(FLATMAP_ROW, FLATMAP_SHAPES, &[gen_edges(20, 22)]);
    assert!(!sizes.is_empty(), "expected some exploded outputs to explain");
}

/// Collect (NEST): `(a ; b)` rows group to `(a ; [b…])`. Reversing a demanded
/// list must demand all the members — which is exactly the non-min keyed
/// lookup ("demand all same-key inputs"), so no new rule is needed; this
/// confirms the existing path handles a `List`-valued reducer output.
const COLLECT_ROW: &str = r#"
    let rows = input 0 | key($0[0] ; $0[1]);
    export "result" = rows | collect;
"#;
const COLLECT_SHAPES: &[(usize, usize)] = &[(2, 0)];

#[test]
fn collect_explanations_sufficient_small() {
    let sizes = assert_all_rows_sufficient(COLLECT_ROW, COLLECT_SHAPES, &[gen_edges(20, 22)]);
    assert!(!sizes.is_empty(), "expected some collected lists to explain");
}

// ---------------------------------------------------------------------------
// The corgi cross-check.
//
// `LinearOp::LiftIter` is SYNTHESIZED by the rewrite — every `$host:` export is
// a LiftIter Linear, and it panics if it appears in a user program — so an
// explained program is the only thing that exercises it. The rest of this file
// runs on `vec` alone, which left the whole explain surface unexercised on the
// columnar backend. These tests render the SAME rewritten dataflow on both and
// require every export to agree.
//
// Two of them are `#[ignore]`d against a divergence that PREDATES this coverage
// (reproduced at 78d75b05): on an explained program whose iterative feedback is
// NEGATED, corgi reports a strict subset of vec's `demand:input0`. The trigger
// is isolated below to the negation alone — `explained_scc_one_scope_negated`
// and `corgi_agrees_on_explained_scc_one_scope` differ in exactly that one line,
// and only the negated one fails. Ruled out along the way: `enter_at` (a depth-1
// reach with a delay agrees, and forcing the row-wise enter_at path changes
// nothing), `min` (a depth-1 min/join loop agrees), and depth-2 nesting on its
// own (the negation-free two-scope program agrees).
// ---------------------------------------------------------------------------

/// Run `src`'s explanation program on `inputs` + `queries` under both backends
/// and require identical exports.
fn assert_explained_backends_agree(
    src: &str,
    shapes: &[(usize, usize)],
    inputs: &[Vec<(Row, Row)>],
    queries: &[(Row, Row)],
) {
    let tree = lowered(src);
    let mut ex = explain::explain(&tree, shapes);
    ex.optimize();
    let mut ex_inputs: Vec<Vec<(Row, Row)>> = inputs.to_vec();
    ex_inputs.push(query_rows(queries));

    let by_vec = evaluate(&ex, &ex_inputs);
    let by_corgi = corgi_evaluate(&ex, &ex_inputs);
    assert_eq!(
        by_vec.keys().collect::<Vec<_>>(),
        by_corgi.keys().collect::<Vec<_>>(),
        "backends disagree on the export names"
    );
    for (name, rows) in &by_vec {
        assert_eq!(rows, &by_corgi[name], "export {name:?} differs between backends");
    }
}

/// The first output row of `src` on `inputs` — a query that makes the
/// explanation dataflow do real work.
fn first_output_row(src: &str, inputs: &[Vec<(Row, Row)>]) -> (Row, Row) {
    let p = optimized(src);
    export_rows(&p, inputs, "result").into_iter().next().expect("a row to query")
}

/// Two inputs (edges and roots), so the rewrite reports two demand exports.
#[test]
fn corgi_agrees_on_reach_explanation() {
    let inputs = vec![gen_edges(50, 55), vec![(row(&[0]), Value::unit())]];
    let q = first_output_row(REACH_ROW, &inputs);
    assert_explained_backends_agree(REACH_ROW, REACH_SHAPES, &inputs, &[q]);
}

/// Transitive closure: iteration with `distinct` rather than `min`.
#[test]
fn corgi_agrees_on_tc_explanation() {
    let inputs = vec![gen_edges(20, 22)];
    let q = first_output_row(TC_ROW, &inputs);
    assert_explained_backends_agree(TC_ROW, TC_SHAPES, &inputs, &[q]);
}

/// The list ops through the rewrite: `flatmap`'s reverse rule (intro + explode)
/// and `collect`'s (a List-valued reducer output).
#[test]
fn corgi_agrees_on_flatmap_explanation() {
    let inputs = vec![gen_edges(20, 22)];
    let q = first_output_row(FLATMAP_ROW, &inputs);
    assert_explained_backends_agree(FLATMAP_ROW, FLATMAP_SHAPES, &inputs, &[q]);
}

#[test]
fn corgi_agrees_on_collect_explanation() {
    let inputs = vec![gen_edges(20, 22)];
    let q = first_output_row(COLLECT_ROW, &inputs);
    assert_explained_backends_agree(COLLECT_ROW, COLLECT_SHAPES, &inputs, &[q]);
}

/// `enter_at` through the rewrite, at depth 1 so the negation trigger is absent.
const REACH_ENTER_AT: &str = r#"
    let edges = input 0 | key($0[0] ; $0[1]);
    let roots = input 1 | key($0[0] ;);
    reach: {
        let seeds = roots | enter_at($0[0]);
        let proposals = reach | join(edges, ($2 ;));
        var reach = seeds + proposals | distinct;
    }
    export "result" = reach::reach;
"#;

#[test]
fn corgi_agrees_on_enter_at_explanation() {
    let inputs = vec![gen_edges(50, 55), vec![(row(&[0]), Value::unit())]];
    let q = first_output_row(REACH_ENTER_AT, &inputs);
    assert_explained_backends_agree(REACH_ENTER_AT, REACH_SHAPES, &inputs, &[q]);
}

/// A `min` reducer in an iterative loop, at depth 1.
const MIN_LOOP: &str = r#"
    let edges = input 0 | key($0[0] ; $0[1]);
    outer: {
        let nodes = edges | key($1 ; $1);
        let labels = proposals + nodes | min;
        var proposals = labels | join(edges, ($2 ; $1));
    }
    export "result" = outer::labels;
"#;

#[test]
fn corgi_agrees_on_min_loop_explanation() {
    let inputs = vec![gen_edges(50, 55)];
    let q = first_output_row(MIN_LOOP, &inputs);
    assert_explained_backends_agree(MIN_LOOP, SCC_SHAPES, &inputs, &[q]);
}

/// SCC's shape — depth-2 nesting, `min`, three joins, a filtered feedback — with
/// the feedback NOT negated. Agrees; the twin below differs only in that line.
const SCC_ONE_SCOPE: &str = r#"
    let edges = input 0 | key($0[0] ; $0[1]);
    outer: {
        let scc = edges + trim;
        fwd: {
            let nodes = edges | key($1 ; $1);
            let labels = proposals + nodes | min;
            var proposals = labels | join(scc, ($2 ; $1));
        }
        let trim_fwd = edges
            | join(fwd::labels, ($1 ; $0, $2))
            | join(fwd::labels, ($0 ; $1, $2))
            | filter($1[1] == $1[2])
            | key($0 ; $1[0]);
        var trim = trim_fwd | filter($0[0] > 1000000);
    }
    export "result" = outer::scc;
"#;

/// The same program with `var trim = trim_fwd - edges` — the one-line minimal
/// reproducer for the demand divergence.
const SCC_ONE_SCOPE_NEGATED: &str = r#"
    let edges = input 0 | key($0[0] ; $0[1]);
    outer: {
        let scc = edges + trim;
        fwd: {
            let nodes = edges | key($1 ; $1);
            let labels = proposals + nodes | min;
            var proposals = labels | join(scc, ($2 ; $1));
        }
        let trim_fwd = edges
            | join(fwd::labels, ($1 ; $0, $2))
            | join(fwd::labels, ($0 ; $1, $2))
            | filter($1[1] == $1[2])
            | key($0 ; $1[0]);
        var trim = trim_fwd - edges;
    }
    export "result" = outer::scc;
"#;

#[test]
fn corgi_agrees_on_explained_scc_one_scope() {
    let inputs = vec![gen_edges(50, 55)];
    let q = first_output_row(SCC_ONE_SCOPE, &inputs);
    assert_explained_backends_agree(SCC_ONE_SCOPE, SCC_SHAPES, &inputs, &[q]);
}

#[test]
fn explained_scc_one_scope_negated() {
    let inputs = vec![gen_edges(50, 55)];
    let q = first_output_row(SCC_ONE_SCOPE_NEGATED, &inputs);
    assert_explained_backends_agree(SCC_ONE_SCOPE_NEGATED, SCC_SHAPES, &inputs, &[q]);
}

/// Iterative + `enter_at` + negated feedback: the real SCC.
#[test]
fn corgi_agrees_on_scc_explanation() {
    let inputs = vec![gen_edges(50, 55)];
    let q = first_output_row(SCC_ROW, &inputs);
    assert_explained_backends_agree(SCC_ROW, SCC_SHAPES, &inputs, &[q]);
}

/// Two queries at once, so the query input carries more than one envelope.
#[test]
fn corgi_agrees_on_two_query_explanation() {
    let inputs = vec![gen_edges(50, 55)];
    let p = optimized(SCC_ROW);
    let qs: Vec<(Row, Row)> = export_rows(&p, &inputs, "result").into_iter().take(2).collect();
    assert_eq!(qs.len(), 2, "expected at least two scc edges to query");
    assert_explained_backends_agree(SCC_ROW, SCC_SHAPES, &inputs, &qs);
}

