//! End-to-end semantic tests for the explanation rewrite, built on
//! `backend::vec::evaluate` (explicit inputs in, every export out).
//!
//! The central property is *sufficiency*: for a query against a program's
//! output, the demand-set the rewritten program reports must — fed back
//! through the original program as its only input — regenerate the queried
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

fn lowered(src: &str) -> Program {
    lower::lower_tree(parse::pipe::parse(src))
}

fn gen_edges(nodes: u64, edges: u64) -> Vec<(Row, Row)> {
    (0..edges).map(|e| interactive::gen_row::<Row>(e, nodes, 2)).collect()
}

/// Run `src` on `inputs` and return one export's rows (asserting positive
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

/// The demand-set for query row `(q_key, q_val)` (q id 0) against `src`'s
/// first export, with `src` run on `edges`.
fn demand_for(src: &str, edges: &[(Row, Row)], q_key: &[i64], q_val: &[i64]) -> Vec<(Row, Row)> {
    let tree = lowered(src);
    let shapes: Vec<(usize, usize)> = vec![(2, 0); tree.root.imports.len()];
    let mut ex = explain::explain(&tree, &shapes);
    ex.optimize();
    let mut query: Row = q_val.iter().copied().collect();
    query.push(0); // query id
    let demand = export_rows(
        &ex,
        &[edges.to_vec(), vec![(q_key.iter().copied().collect(), query)]],
        "demand:input0",
    );
    demand.into_iter().collect()
}

/// Sufficiency for every row of `src`'s output on `edges`: query it, take the
/// demand-set, re-run the original program on the demand-set alone, and
/// require the queried row in the output. Returns (row, |demand|) per query.
fn assert_all_rows_sufficient(src: &str, edges: &[(Row, Row)]) -> Vec<((Row, Row), usize)> {
    let p = optimized(src);
    let result = export_rows(&p, &[edges.to_vec()], "result");
    assert!(!result.is_empty(), "test instance has empty output; pick another");
    let mut sizes = Vec::new();
    for (k, v) in &result {
        let demand = demand_for(src, edges, k, v);
        let replay = export_rows(&p, &[demand.clone()], "result");
        assert!(
            replay.contains(&(k.clone(), v.clone())),
            "insufficient explanation for {:?}: {} demanded rows {:?} regenerate only {:?}",
            (k, v), demand.len(), demand, replay,
        );
        sizes.push(((k.clone(), v.clone()), demand.len()));
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
    let sizes = assert_all_rows_sufficient(SCC_ROW, &gen_edges(50, 55));
    assert_eq!(sizes.len(), 12, "expected 12 scc edges at 50/55");
}

/// A demand-set is grounded in actual input rows.
#[test]
fn demand_is_a_subset_of_the_input() {
    let edges = gen_edges(50, 55);
    let p = optimized(SCC_ROW);
    let result = export_rows(&p, &[edges.clone()], "result");
    let edge_set: BTreeSet<(Row, Row)> = edges.iter().cloned().collect();
    let (k, v) = result.iter().next().unwrap();
    for row in demand_for(SCC_ROW, &edges, k, v) {
        assert!(edge_set.contains(&row), "demanded row {:?} is not an input row", row);
    }
}

/// The full 100-node / 110-edge sweep: 22 scc edges, each sufficient.
#[test]
#[ignore = "heavier sweep; run with --release -- --ignored"]
fn scc_row_explanations_sufficient_100() {
    let sizes = assert_all_rows_sufficient(SCC_ROW, &gen_edges(100, 110));
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
    let demand = demand_for(SCC_ROW, &edges, &[773], &[466]);
    let replay = export_rows(&p, &[demand.clone()], "result");
    assert!(
        replay.contains(&(Row::from_slice(&[773]), Row::from_slice(&[466]))),
        "insufficient explanation for (773, 466): {} demanded rows regenerate only {:?}",
        demand.len(), replay,
    );
}
