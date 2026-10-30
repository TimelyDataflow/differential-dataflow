//! Prove when coalescing equal projected values preserves a reducer's answer.
//! This is an analysis only: it never changes a graph or a scope.

use corgi::{ArithOp, BinOp, Bounds, Demand, Graph, NodeRef, NumOp, Op, Red, Shape, Value};

#[derive(Clone, Copy, PartialEq, Eq)]
enum Term { Neutral, Diff, Linear, Bad }

/// A map element must be `diff * g(value)` (with `g = 1` for the bare diff).
/// Each node is examined once, including tuple aliases.
fn weighted(body: &Graph<NumOp>) -> bool {
    let nodes: Vec<_> = body.nodes().collect();
    let mut terms = Vec::with_capacity(nodes.len());
    for node in &nodes {
        let term = match node {
            NodeRef::Input => Term::Bad,
            NodeRef::Tuple(fields) => {
                if fields.iter().any(|&i| terms[i] != Term::Neutral) { Term::Bad } else { Term::Neutral }
            }
            NodeRef::Op(NumOp::Core(Op::Field(field)), i) => match &nodes[*i] {
                NodeRef::Input => match field { 0 => Term::Neutral, 1 => Term::Diff, _ => Term::Bad },
                NodeRef::Tuple(fields) => fields.get(*field).map(|&j| terms[j]).unwrap_or(Term::Bad),
                _ if terms[*i] == Term::Neutral => Term::Neutral,
                _ => Term::Bad,
            },
            NodeRef::Op(NumOp::Arith(ArithOp::Bin(BinOp::Mul)), i) => match &nodes[*i] {
                NodeRef::Input => Term::Linear,
                NodeRef::Tuple(fields) if fields.len() == 2 => match (terms[fields[0]], terms[fields[1]]) {
                    (Term::Diff | Term::Linear, Term::Neutral) | (Term::Neutral, Term::Diff | Term::Linear) => Term::Linear,
                    _ => Term::Bad,
                },
                _ => Term::Bad,
            },
            NodeRef::Op(NumOp::Core(Op::Unit | Op::Lit(_)), _) => Term::Neutral,
            NodeRef::Op(_, i) => if terms[*i] == Term::Neutral { Term::Neutral } else { Term::Bad },
        };
        terms.push(term);
    }
    matches!(terms[body.output()], Term::Diff | Term::Linear)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ListUse { Input, Vals, Mapped, Other }

/// Prove that the reducer reads values only through diff-weighted sums.
/// Return its value demand, or `None` when an entry boundary can matter.
/// The compiled input must be `(key, List<(value, Int-diff)>)`.
pub(crate) fn projection(compiled: &corgi::Compiled) -> Option<Demand> {
    let input = compiled.input();
    let Shape::Prod(root_shape) = input else { return None };
    let [key, Shape::List(element)] = root_shape.as_slice() else { return None };
    let Shape::Prod(pair_shape) = &**element else { return None };
    let [val, Shape::Int] = pair_shape.as_slice() else { return None };
    let graph = compiled.graph();
    let nodes: Vec<_> = graph.nodes().collect();
    let mut uses = Vec::with_capacity(nodes.len());
    let mut maps = 0;
    let mut sums = 0;
    for node in &nodes {
        let use_ = match node {
            NodeRef::Input => ListUse::Input,
            NodeRef::Tuple(fields) => {
                if fields.iter().any(|&i| uses[i] != ListUse::Other) { return None; }
                ListUse::Other
            }
            NodeRef::Op(NumOp::Core(Op::Field(field)), i) if uses[*i] == ListUse::Input => {
                match field { 0 => ListUse::Other, 1 => ListUse::Vals, _ => return None }
            }
            NodeRef::Op(NumOp::Core(Op::MapList(body)), i) if uses[*i] == ListUse::Vals => {
                if !weighted(body) { return None; }
                maps += 1;
                ListUse::Mapped
            }
            NodeRef::Op(NumOp::Arith(ArithOp::Reduce(Red::Add)), i) if uses[*i] == ListUse::Mapped => {
                sums += 1;
                ListUse::Other
            }
            NodeRef::Op(NumOp::Core(Op::Unit | Op::Lit(_)), _) => ListUse::Other,
            NodeRef::Op(_, i) => {
                if uses[*i] != ListUse::Other { return None; }
                ListUse::Other
            }
        };
        uses.push(use_);
    }
    if maps != 1 || sums != 1 || uses[graph.output()] != ListUse::Other { return None; }

    // A projected group can disappear when all its values cancel. The reducer must
    // emit nothing for an empty list, or coalescing would change key presence.
    let empty = Value::List(Bounds::offsets(vec![0]), Box::new(Value::empty(&Shape::Prod(vec![val.clone(), Shape::Int]))));
    let argument = Value::Prod(vec![zero_row(key)?, empty]);
    let empty_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        corgi::eval_graph(graph, argument).into_list("empty reduce").ok().map(|(bounds, _)| bounds.to_vec())
    })).ok()??;
    if empty_result != [0] { return None; }

    let analysis = corgi::analyze_demand(graph, input, &Demand::All).ok()?;
    let Demand::Prod(root) = analysis.input_demand() else { return None };
    // The empty-list check uses one representative key. No key read means that
    // result is the same for every key, including after a whole group cancels.
    if root[0] != Demand::None { return None; }
    let Demand::List(element) = &root[1] else { return None };
    let Demand::Prod(pair) = &**element else { return None };
    Some(pair[0].clone())
}

fn zero_row(shape: &Shape) -> Option<Value> {
    Some(match shape {
        Shape::Unit => Value::Unit(1),
        Shape::Int => Value::i64(vec![0]),
        Shape::Float => Value::f64(vec![0.0]),
        Shape::Prod(parts) => Value::Prod(parts.iter().map(zero_row).collect::<Option<_>>()?),
        Shape::List(element) => Value::List(Bounds::offsets(vec![0]), Box::new(Value::empty(element))),
        Shape::Sum(parts) if !parts.is_empty() => {
            let lanes = parts.iter().enumerate().map(|(i, p)| if i == 0 { zero_row(p) } else { Some(Value::empty(p)) }).collect::<Option<_>>()?;
            Value::sum(vec![0], lanes)
        }
        _ => return None,
    })
}
