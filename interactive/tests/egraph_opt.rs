//! The e-graph optimizer against the hand-written one: on every corpus program, and on its
//! explanation rewrite (a large, mechanically generated, redundant IR), the e-graph-optimized
//! program must evaluate to the same outputs as the original, and reach an operator count no
//! worse than `Scope::optimize`'s.

use interactive::backend::vec;
use interactive::ir::Value;
use interactive::{egraph, explain, lower, parse, scope_ir};

fn tup(fields: &[i64]) -> Value {
    Value::Tuple(fields.iter().map(|&n| Value::Int(n)).collect())
}
fn rows(rs: &[&[i64]]) -> Vec<(Value, Value)> {
    rs.iter().map(|f| (tup(f), Value::unit())).collect()
}

/// Per-program inputs (as in `tests/corgi_backend.rs`).
fn inputs_for(prog: &str) -> Vec<Vec<(Value, Value)>> {
    let edges = rows(&[&[1, 2], &[2, 3], &[3, 4], &[5, 6], &[4, 2]]);
    match prog {
        "reach" => vec![edges, rows(&[&[1]])],
        "scc" => vec![edges],
        "stable" => vec![rows(&[&[1, 1, 10, 1], &[1, 2, 11, 1], &[2, 1, 10, 2], &[2, 2, 11, 2]])],
        "unnest" => vec![rows(&[&[1, 2], &[3, 4]])],
        "adt" => vec![edges.clone()],
        "ast" => vec![edges],
        "binders" => vec![rows(&[&[1, 2], &[3, 4]])],
        "tour" => vec![rows(&[&[1, 2], &[2, 3], &[3, 1], &[3, 4], &[5, 2]]), rows(&[&[1], &[5]])],
        other => panic!("no inputs configured for {other}"),
    }
}

fn load(prog: &str) -> scope_ir::Program {
    let path = format!("{}/examples/programs/{prog}.ddp", env!("CARGO_MANIFEST_DIR"));
    lower::lower_tree(parse::pipe::parse(&interactive::load_program(&path)))
}

/// The inputs' row widths, read off their first rows (an empty input has no known width).
fn widths_of(inputs: &[Vec<(Value, Value)>]) -> Vec<Option<(usize, usize)>> {
    inputs
        .iter()
        .map(|rows| rows.first().map(|(k, v)| (arity(k), arity(v))))
        .collect()
}
fn arity(v: &Value) -> usize {
    match v { Value::Tuple(xs) => xs.len(), _ => 1 }
}

/// Optimize `tree` both ways; check the e-graph result evaluates like the original and costs no
/// more than the hand-written optimizer's under the e-graph's own model. Returns
/// (original, rust, egraph) op counts, printing costs alongside.
fn check(name: &str, tree: &scope_ir::Program, inputs: &[Vec<(Value, Value)>]) -> (usize, usize, usize) {
    let widths = widths_of(inputs);
    let want = vec::evaluate(tree, inputs);
    let mut rust = tree.clone();
    rust.optimize();
    let eg = egraph::optimize(tree, &widths);
    assert_eq!(vec::evaluate(&eg, inputs), want, "{name}: the e-graph-optimized program changed the outputs");
    let costs = (egraph::cost(tree, &widths), egraph::cost(&rust, &widths), egraph::cost(&eg, &widths));
    assert!(costs.2 <= costs.1, "{name}: e-graph cost {} exceeds the hand-written optimizer's {}", costs.2, costs.1);
    println!("{name:>14}: cost {} -> rust {}, egraph {}", costs.0, costs.1, costs.2);
    (tree.op_count(), rust.op_count(), eg.op_count())
}

/// A reducer that reads no values gets a key-only projection pushed in front of it.
#[test]
fn demand_pushdown_narrows_before_count() {
    let src = "let rows = input 0 | key($0[0] ; $0[1], $0[0] * $0[1], $0[1] + 7);\n\
               let n = rows | count;\n\
               export \"n\" = n | arrange;\n";
    let tree = lower::lower_tree(parse::pipe::parse(src));
    let inputs = vec![rows(&[&[1, 2], &[1, 3], &[2, 5]])];
    let eg = egraph::optimize(&tree, &widths_of(&inputs));
    assert_eq!(vec::evaluate(&eg, &inputs), vec::evaluate(&tree, &inputs));
    // the count's input is a Linear whose last step keeps only the key
    let reduce_input = eg.root.items.iter().find_map(|it| match it {
        scope_ir::Item::Op(scope_ir::Node::Reduce { input, .. }) => Some(input.clone()),
        _ => None,
    }).expect("a count");
    let scope_ir::Ref::Local(i) = reduce_input else { panic!("count reads {reduce_input:?}") };
    let scope_ir::Item::Op(scope_ir::Node::Linear { ops, .. }) = &eg.root.items[i] else { panic!("count's input is not linear: {:#?}", eg.root) };
    let last = ops.last().expect("a step");
    assert!(matches!(last, interactive::ir::LinearOp::Project(p) if matches!(&p.val, parse::Term::Tuple(fs) if fs.is_empty())), "last step {last:?} keeps values");
}

/// A join that reads only some fields of an input's values gets that input narrowed to them.
#[test]
fn demand_pushdown_narrows_join_input() {
    let src = "let wide = input 0 | key($0[0] ; $0[1], $0[0] * $0[1], $0[1] + 7);\n\
               let left = input 1 | key($0[0] ; $0[1]);\n\
               let j = left | join(wide, ($0[0] ; $2[2], $1[0]));\n\
               export \"j\" = j | arrange;\n";
    let tree = lower::lower_tree(parse::pipe::parse(src));
    let inputs = vec![rows(&[&[1, 2], &[1, 3], &[2, 5]]), rows(&[&[1, 9], &[2, 8]])];
    let eg = egraph::optimize(&tree, &widths_of(&inputs));
    assert_eq!(vec::evaluate(&eg, &inputs), vec::evaluate(&tree, &inputs));
    let (right, projection) = eg.root.items.iter().find_map(|it| match it {
        scope_ir::Item::Op(scope_ir::Node::Join { right, projection, .. }) => Some((right.clone(), projection.clone())),
        _ => None,
    }).expect("a join");
    let scope_ir::Ref::Local(i) = right else { panic!("join reads {right:?}") };
    // through the arrange the join's input is held in
    let i = match &eg.root.items[i] {
        scope_ir::Item::Op(scope_ir::Node::Arrange(scope_ir::Ref::Local(j))) => *j,
        _ => i,
    };
    let scope_ir::Item::Op(scope_ir::Node::Linear { ops, .. }) = &eg.root.items[i] else { panic!("join's right input is not linear: {:#?}", eg.root) };
    let last = ops.last().expect("a step");
    assert!(matches!(last, interactive::ir::LinearOp::Project(p) if matches!(&p.val, parse::Term::Tuple(fs) if fs.len() == 1)), "last step {last:?} does not keep one field");
    // and the join now reads that one field at position 0
    assert_eq!(egraph::scalar::projection_demand(&projection, 2), egraph::scalar::Demand::Fields([0].into_iter().collect()));
}

/// The same join written both ways around is one join.
#[test]
fn join_commutativity_merges() {
    let src = "let a = input 0 | key($0[0] ; $0[1]);\n\
               let b = input 1 | key($0[0] ; $0[1]);\n\
               let j1 = a | join(b, ($0[0] ; $1[0], $2[0]));\n\
               let j2 = b | join(a, ($0[0] ; $2[0], $1[0]));\n\
               export \"j\" = (j1 + j2) | arrange;\n";
    let tree = lower::lower_tree(parse::pipe::parse(src));
    let inputs = vec![rows(&[&[1, 2], &[2, 5]]), rows(&[&[1, 9], &[2, 8]])];
    let eg = egraph::optimize(&tree, &widths_of(&inputs));
    assert_eq!(vec::evaluate(&eg, &inputs), vec::evaluate(&tree, &inputs));
    let joins = eg.root.items.iter().filter(|it| matches!(it, scope_ir::Item::Op(scope_ir::Node::Join { .. }))).count();
    assert_eq!(joins, 1, "{:#?}", eg.root);
}

#[test]
fn corpus_programs() {
    for prog in ["reach", "scc", "stable", "unnest", "adt", "ast", "binders", "tour"] {
        let tree = load(prog);
        let (o, r, e) = check(prog, &tree, &inputs_for(prog));
        println!("{prog:>8}: {o} ops -> rust {r}, egraph {e}");
    }
}

#[test]
fn explain_corpus() {
    // the explanation rewrite is the redundant corpus: reach and scc grow ten-fold.
    for prog in ["reach", "scc"] {
        let tree = load(prog);
        let inputs0 = inputs_for(prog);
        // each input's (key arity, value arity), read off its rows
        let shapes: Vec<(usize, usize)> = inputs0
            .iter()
            .map(|rows| match &rows[0].0 { Value::Tuple(xs) => (xs.len(), 0usize), _ => (1, 0) })
            .collect();
        let rewritten = explain::explain(&tree, &shapes);
        // the explained program has an extra (query) input; feed it nothing.
        let mut inputs = inputs0;
        while inputs.len() < rewritten.root.imports.len() {
            inputs.push(Vec::new());
        }
        let (o, r, e) = check(&format!("explain({prog})"), &rewritten, &inputs);
        println!("explain({prog}): {o} ops -> rust {r}, egraph {e}");
    }
}
