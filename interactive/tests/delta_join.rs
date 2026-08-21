//! `delta_join` against the same query written as a chain of binary joins.
//!
//! The two share no machinery: one is a network of `half_join`s carrying a
//! payload time in data and deciding by a total order on times, the other is
//! `join_core` deciding by which batch arrived first. Agreement is therefore an
//! independent answer rather than a second opinion from the same method — the
//! check `dogsdogsdogs/examples/delta_query.rs` makes, expressed in DDIR.

use interactive::backend::vec;
use interactive::ir::{Diff, Value};
use interactive::{lower, parse};

type Rows = Vec<((Value, Value), Diff)>;

fn tup(fields: &[i64]) -> Value {
    Value::Tuple(fields.iter().map(|&n| Value::Int(n)).collect())
}

fn edge_rows(edges: &[(i64, i64)]) -> Vec<(Value, Value)> {
    edges.iter().map(|&(a, b)| (tup(&[a, b]), Value::unit())).collect()
}

/// Evaluate `src` on one edge input and return its sole export, sorted.
fn run(src: &str, edges: &[(i64, i64)]) -> Rows {
    let mut prog = lower::lower_tree(parse::pipe::parse(src));
    prog.optimize();
    let mut out = vec::evaluate(&prog, &[edge_rows(edges)])
        .remove("result")
        .expect("program must export \"result\"");
    out.sort();
    out
}

/// `Q(a, b, c) :- E(a, b), E(b, c), E(a, c)`, as two binary joins over an
/// intermediate collection.
const TRIANGLE_BINARY: &str = r#"
    let edges = input 0 | key($0[0] ; $0[1]);
    let rev   = edges | key($1 ; $0);
    let step  = rev | join(edges, ($1 ; $0, $2));
    export "result" = step
        | join(edges, ($0 ; $1, $2))
        | filter($1[1] == $1[2])
        | key($0 ; $1[0], $1[1]);
"#;

/// The same query as a rule body.
const TRIANGLE_DELTA: &str = r#"
    let edges = input 0 | key($0[0] ; $0[1]);
    export "result" = delta_join {
        edges(a, b),
        edges(b, c),
        edges(a, c),
    } => (a ; b, c);
"#;

/// A four-cycle, which forces a path whose second stage still extends the
/// binding rather than only testing it.
const CYCLE4_BINARY: &str = r#"
    let edges = input 0 | key($0[0] ; $0[1]);
    let rev   = edges | key($1 ; $0);
    let two   = rev | join(edges, ($1 ; $2));
    let fwd   = two | key($0, $1 ;);
    let bwd   = two | key($1, $0 ;);
    export "result" = fwd | join(bwd, ($0 ;)) | key($0[0] ; $0[1]);
"#;

fn triangles(edges: &[(i64, i64)]) -> (Rows, Rows) {
    (run(TRIANGLE_BINARY, edges), run(TRIANGLE_DELTA, edges))
}

#[test]
fn triangle_matches_the_binary_plan() {
    // A graph with two triangles, a chord, a self-loop and a dangling edge, so
    // the answer is neither empty nor everything.
    let edges = [(1, 2), (2, 3), (1, 3), (3, 4), (1, 4), (2, 4), (5, 5), (6, 7)];
    let (binary, delta) = triangles(&edges);
    assert!(!binary.is_empty(), "the fixture should contain triangles");
    assert_eq!(delta, binary);
}

#[test]
fn triangle_matches_on_a_dense_graph() {
    // Every ordered pair over five nodes: maximal fan-out, and every attribute
    // is bound many ways, so a plan that double-counts cannot hide.
    let edges: Vec<(i64, i64)> = (0..5).flat_map(|a| (0..5).map(move |b| (a, b))).collect();
    let (binary, delta) = triangles(&edges);
    assert_eq!(delta, binary);
}

#[test]
fn triangle_is_empty_on_a_triangle_free_graph() {
    // A path has no triangles; both plans must agree that the answer is nothing,
    // which catches a delta path that emits before validating.
    let edges = [(1, 2), (2, 3), (3, 4), (4, 5)];
    let (binary, delta) = triangles(&edges);
    assert!(binary.is_empty());
    assert!(delta.is_empty());
}

#[test]
fn multiplicities_survive_the_delta_plan() {
    // DDIR collections are bags. Feeding a duplicated edge makes every triangle
    // through it count twice, and the delta plan must scale the same way the
    // binary plan does — this is the difference between the delta join and the
    // worst-case-optimal join, which is only defined over sets.
    let edges = [(1, 2), (2, 3), (1, 3), (1, 2)];
    let (binary, delta) = triangles(&edges);
    assert!(binary.iter().any(|(_, d)| *d > 1), "the fixture should produce a multiplicity above one");
    assert_eq!(delta, binary);
}

#[test]
fn four_cycle_matches_the_binary_plan() {
    let edges = [(1, 2), (2, 3), (3, 4), (4, 1), (1, 3), (2, 4), (5, 6)];
    let binary = run(CYCLE4_BINARY, &edges);
    let delta = run(
        r#"
        let edges = input 0 | key($0[0] ; $0[1]);
        export "result" = delta_join {
            edges(a, b),
            edges(b, c),
            edges(c, d),
            edges(d, a),
        } => (a ; c);
        "#,
        &edges,
    );
    assert!(!binary.is_empty(), "the fixture should contain four-cycles");
    assert_eq!(delta, binary);
}

#[test]
fn plan_indexes_are_shared_across_paths() {
    // Three delta paths over one relation need exactly three arrangements of it
    // — forward, reverse, and the pair index — and `optimize` must find that.
    // A plan that arranged per stage would build six.
    use interactive::scope_ir::{Item, Node};
    let mut prog = lower::lower_tree(parse::pipe::parse(TRIANGLE_DELTA));
    prog.optimize();
    let indexes = prog.root.items.iter()
        .filter(|i| matches!(i, Item::Op(Node::DeltaIndex { .. })))
        .count();
    assert_eq!(indexes, 3, "forward, reverse and pair, each built once");
}

#[test]
fn delta_indexes_do_not_merge_with_ordinary_arrangements() {
    // A `half_join` holds logical compaction back on what it reads; an ordinary
    // join does not. Deduplicating the two together would impose the delta
    // join's retention on the ordinary join, so the two node kinds must stay
    // distinct even when they index the same collection in the same orientation.
    use interactive::scope_ir::{Item, Node};
    let src = r#"
        let edges = input 0 | key($0[0] ; $0[1]);
        let pairs = edges | join(edges, ($0 ; $1, $2));
        let tri = delta_join { edges(a, b), edges(b, c), edges(a, c) } => (a ; b, c);
        export "result" = pairs + tri;
    "#;
    let mut prog = lower::lower_tree(parse::pipe::parse(src));
    prog.optimize();
    let arranges = prog.root.items.iter().filter(|i| matches!(i, Item::Op(Node::Arrange(_)))).count();
    let indexes = prog.root.items.iter().filter(|i| matches!(i, Item::Op(Node::DeltaIndex { .. }))).count();
    assert_eq!(arranges, 1, "the ordinary join keeps its own arrangement of `edges`");
    assert_eq!(indexes, 3, "the delta join keeps its own three indexes");
}

/// `scc.ddp` and `scc_delta.ddp` differ only in how the two trim steps are
/// written. The delta version puts a three-atom rule body *inside an iterating
/// scope*, so the paths run under a time whose iteration coordinate advances
/// beneath them — the configuration DDIR programs actually live in.
#[test]
fn scc_trim_as_a_delta_join_matches_scc() {
    let dir = format!("{}/examples/programs", env!("CARGO_MANIFEST_DIR"));
    let load = |name: &str| std::fs::read_to_string(format!("{dir}/{name}.ddp")).unwrap();
    // A graph whose components are non-trivial: two cycles joined by a bridge,
    // plus a tail that trims away.
    let edges = [
        (1, 2), (2, 3), (3, 1),
        (3, 4),
        (4, 5), (5, 6), (6, 4),
        (6, 7),
        (8, 8),
    ];
    let binary = run(&load("scc"), &edges);
    let delta = run(&load("scc_delta"), &edges);
    assert!(!binary.is_empty(), "the fixture should have a non-empty condensation");
    assert_eq!(delta, binary);
}
