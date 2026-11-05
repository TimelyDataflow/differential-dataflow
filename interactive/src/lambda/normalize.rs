//! Shape-checked graph preparation shared by rewrites and printing.

use corgi::{Builder, Graph, NodeRef, NumOp, Op, OpLike, Shape, Value};

type G = Graph<NumOp>;

/// Rebuild checked patterns as identities after their shapes have been checked.
/// The caller must typecheck the original graph before dropping any checks.
pub(crate) fn prepare(graph: &G, input: &Shape) -> Result<G, String> {
    prepare_inner(graph, input, false)
}

/// Prepare a graph for a rewrite, including constant anchors and stable node order.
/// These extra rewrites change the stored graph; the printer does not apply them.
pub(crate) fn canonical(graph: &G, input: &Shape) -> Result<G, String> {
    prepare_inner(graph, input, true)
}

fn prepare_inner(graph: &G, input: &Shape, rewrite: bool) -> Result<G, String> {
    let mut builder = Builder::default();
    let root = builder.input();
    let mut ids = Vec::new();
    let mut shapes: Vec<Shape> = Vec::new();
    for node in graph.nodes() {
        let (id, shape) = match node {
            NodeRef::Input => (root, input.clone()),
            NodeRef::Tuple(fields) => (builder.tuple(fields.iter().map(|&i| ids[i]).collect()), Shape::Prod(fields.iter().map(|&i| shapes[i].clone()).collect())),
            NodeRef::Op(op, i) => {
                let shape = corgi::shape_of_value(&op.eval(Value::empty(&shapes[i]))?);
                let id = match op {
                    NumOp::Core(Op::Pattern(_)) => ids[i],
                    // A unit field and a written `()` carry the same row count.
                    NumOp::Core(Op::Field(_)) if rewrite && shape == Shape::Unit => builder.add(Op::Unit, vec![root]),
                    // Every node in a scalar stratum preserves its row count. Constants
                    // and unit need only that count, not the computed payload. Corgi's
                    // DCE otherwise keeps dead arithmetic through these anchor edges.
                    NumOp::Core(Op::Unit | Op::Lit(_)) if rewrite => builder.add(op.clone(), vec![root]),
                    _ => {
                        let op = match op {
                            NumOp::Core(Op::MapList(body)) => {
                                let Shape::List(elem) = &shapes[i] else { return Err("map input is not a list".into()) };
                                Op::MapList(Box::new(prepare_inner(body, elem, rewrite)?)).into()
                            }
                            NumOp::Core(Op::Fold(body) | Op::FoldScan(body)) => {
                                let Shape::Prod(pair) = &shapes[i] else { return Err("fold input is not a pair".into()) };
                                let Shape::List(elem) = &pair[1] else { return Err("fold input has no list".into()) };
                                let body = Box::new(prepare_inner(body, &Shape::Prod(vec![pair[0].clone(), *elem.clone()]), rewrite)?);
                                if matches!(op, NumOp::Core(Op::Fold(_))) { Op::Fold(body).into() } else { Op::FoldScan(body).into() }
                            }
                            NumOp::Core(Op::MapSum(arms)) => {
                                let Shape::Sum(lanes) = &shapes[i] else { return Err("match input is not a sum".into()) };
                                Op::MapSum(arms.iter().map(|(tag, body)| Ok((*tag, prepare_inner(body, &lanes[*tag], rewrite)?))).collect::<Result<_, String>>()?).into()
                            }
                            other => other.clone(),
                        };
                        builder.add(op, vec![ids[i]])
                    }
                };
                (id, shape)
            }
        };
        ids.push(id);
        shapes.push(shape);
    }
    let graph = corgi::optimize(&builder.finish(ids[graph.output()]));
    Ok(if rewrite { ordered(&graph) } else { graph })
}

/// Number live nodes by their first use from the output. Equal wiring can otherwise
/// differ only in the order independent nodes were built during composition.
fn ordered(graph: &G) -> G {
    let nodes: Vec<_> = graph.nodes().collect();
    let mut builder = Builder::default();
    let mut ids = vec![None; nodes.len()];
    let mut pending = vec![(graph.output(), false)];
    while let Some((i, ready)) = pending.pop() {
        if ids[i].is_some() { continue; }
        if !ready {
            pending.push((i, true));
            match &nodes[i] {
                NodeRef::Input => {},
                NodeRef::Tuple(fields) => pending.extend(fields.iter().rev().map(|&j| (j, false))),
                NodeRef::Op(_, input) => pending.push((*input, false)),
            }
            continue;
        }
        let id = match &nodes[i] {
            NodeRef::Input => builder.input(),
            NodeRef::Tuple(fields) => builder.tuple(fields.iter().map(|&j| ids[j].unwrap()).collect()),
            NodeRef::Op(op, input) => {
                let op = match op {
                    NumOp::Core(Op::MapList(body)) => Op::MapList(Box::new(ordered(body))).into(),
                    NumOp::Core(Op::Fold(body)) => Op::Fold(Box::new(ordered(body))).into(),
                    NumOp::Core(Op::FoldScan(body)) => Op::FoldScan(Box::new(ordered(body))).into(),
                    NumOp::Core(Op::MapSum(arms)) => Op::MapSum(arms.iter().map(|(tag, body)| (*tag, ordered(body))).collect()).into(),
                    other => (*other).clone(),
                };
                builder.add(op, vec![ids[*input].unwrap()])
            }
        };
        ids[i] = Some(id);
    }
    builder.finish(ids[graph.output()].unwrap())
}
