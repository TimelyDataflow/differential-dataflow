//! Compare final stages with a bijection of items, imports, variables, and child exports.
//! Scalar equality is equality of graphs compiled at the known input type, not sampled values.

use crate::ir::{Lambda, LinearOp, Reducer};
use crate::scope_ir::{Item, Node, Ref, Scope, Source};
use crate::shapes::{ItemShapes, ScopeShapes};
use super::Report;

impl Report {
    /// Differences after renaming scope-local items. Exported trace names remain observable.
    /// Collection topology is compared as given; scalar compilation proves pattern checks.
    pub fn compare_final(&self, other: &Report) -> Vec<String> {
        let mut differences = Vec::new();
        if !self.unresolved.is_empty() || !other.unresolved.is_empty() {
            differences.push("cannot compare unresolved imports".into());
        }
        let (a, b) = (self.stages.last().unwrap(), other.stages.last().unwrap());
        if !a.problems.is_empty() || !b.problems.is_empty() { differences.push("cannot compare invalid shapes".into()); }
        compare(&a.program.root, &a.shapes, &b.program.root, &b.shapes, None, "root", &mut differences);
        differences
    }
}

struct Mapping {
    imports: Vec<usize>,
    items: Vec<usize>,
    exports: Vec<Vec<usize>>,
}

impl Mapping {
    fn same(&self, a: &Ref, b: &Ref) -> bool {
        match (a, b) {
            (Ref::Import(a), Ref::Import(b)) => self.imports.get(*a) == Some(b),
            (Ref::Local(a), Ref::Local(b)) => self.items.get(*a) == Some(b),
            (Ref::Var(a), Ref::Var(b)) => a == b,
            (Ref::ChildExport(a, x), Ref::ChildExport(b, y)) => self.items.get(*a) == Some(b) && self.exports.get(*a).and_then(|e| e.get(*x)) == Some(y),
            _ => false,
        }
    }
}

fn lambda(a: &Lambda, b: &Lambda, input: Option<&corgi::Shape>) -> bool {
    let Some(input) = input else { return false };
    let compile = |lambda: &Lambda| corgi::compile(lambda.graph()?.as_ref().clone(), input);
    match (compile(a), compile(b)) {
        (Ok(a), Ok(b)) => a.graph() == b.graph(),
        _ => false,
    }
}

/// Input types for each scalar slot, including each step of a linear chain.
fn input_shapes(node: &Node, shapes: &ScopeShapes) -> Option<Vec<corgi::Shape>> {
    use corgi::Shape;
    Some(match node {
        Node::Linear { input, ops } => {
            let shape = shapes.of(input)?;
            let mut steps = crate::shapes::linear_shapes(ops, shape.key, shape.val, shape.time - 1).ok()?;
            steps.pop();
            steps.into_iter().map(|(k, v)| Shape::Prod(vec![k, v])).collect()
        }
        Node::Join { left, right, .. } => {
            let (l, r) = (shapes.of(left)?, shapes.of(right)?);
            vec![Shape::Prod(vec![l.key, l.val, r.val])]
        }
        Node::HalfJoin { stream, arrangement, .. } => {
            let (s, a) = (shapes.of(stream)?, shapes.of(arrangement)?);
            let Shape::Prod(fields) = s.val else { return None };
            vec![Shape::Prod(vec![s.key, fields.first()?.clone(), a.val])]
        }
        Node::Reduce { input, .. } => {
            let shape = shapes.of(input)?;
            vec![Shape::Prod(vec![shape.key, Shape::List(Box::new(Shape::Prod(vec![shape.val, Shape::Int])))])]
        }
        _ => Vec::new(),
    })
}

fn linear(a: &LinearOp, b: &LinearOp, input: Option<&corgi::Shape>) -> bool {
    match (a, b) {
        (LinearOp::Project(a), LinearOp::Project(b)) | (LinearOp::Filter(a), LinearOp::Filter(b))
        | (LinearOp::EnterAt(a), LinearOp::EnterAt(b)) | (LinearOp::FlatMap(a), LinearOp::FlatMap(b))
        | (LinearOp::JoinFunction(a), LinearOp::JoinFunction(b)) => lambda(a, b, input),
        (LinearOp::Negate, LinearOp::Negate) | (LinearOp::Lift, LinearOp::Lift) | (LinearOp::LiftIter, LinearOp::LiftIter) => true,
        _ => false,
    }
}

fn reducer(a: &Reducer, b: &Reducer, input: Option<&corgi::Shape>) -> bool {
    match (a, b) {
        (Reducer::Expr(a), Reducer::Expr(b)) => lambda(a, b, input),
    }
}

fn node(a: &Node, b: &Node, map: &Mapping, scalar: bool, inputs: &[corgi::Shape]) -> bool {
    match (a, b) {
        (Node::Linear { input: a, ops: x }, Node::Linear { input: b, ops: y }) => map.same(a, b) && (!scalar || (x.len() == y.len() && x.iter().zip(y).enumerate().all(|(i, (a, b))| linear(a, b, inputs.get(i))))),
        (Node::Concat(a), Node::Concat(b)) => a.len() == b.len() && a.iter().zip(b).all(|(a, b)| map.same(a, b)),
        (Node::Arrange(a), Node::Arrange(b)) => map.same(a, b),
        (Node::Join { left: a, right: b, projection: p }, Node::Join { left: c, right: d, projection: q }) => map.same(a, c) && map.same(b, d) && (!scalar || lambda(p, q, inputs.first())),
        (Node::HalfJoin { stream: a, arrangement: b, strict: s, projection: p }, Node::HalfJoin { stream: c, arrangement: d, strict: t, projection: q }) => map.same(a, c) && map.same(b, d) && s == t && (!scalar || lambda(p, q, inputs.first())),
        (Node::Reduce { input: a, reducer: x }, Node::Reduce { input: b, reducer: y }) => map.same(a, b) && (!scalar || reducer(x, y, inputs.first())),
        (Node::Inspect { input: a, label: x }, Node::Inspect { input: b, label: y }) => map.same(a, b) && x == y,
        _ => false,
    }
}

fn compare(a: &Scope, ash: &ScopeShapes, b: &Scope, bsh: &ScopeShapes, parent: Option<&Mapping>, path: &str, diff: &mut Vec<String>) -> Vec<usize> {
    if a.items.len() != b.items.len() { diff.push(format!("{path}: item count {} -> {}", a.items.len(), b.items.len())); }
    if a.imports.len() != b.imports.len() { diff.push(format!("{path}: import count {} -> {}", a.imports.len(), b.imports.len())); }
    if ash.vars != bsh.vars { diff.push(format!("{path}: feedback variable shapes or order changed")); }
    let mut map = Mapping { imports: vec![usize::MAX; a.imports.len()], items: vec![usize::MAX; a.items.len()], exports: vec![Vec::new(); a.items.len()] };
    let mut used = vec![false; b.imports.len()];
    for (i, imp) in a.imports.iter().enumerate() {
        let found = b.imports.iter().enumerate().position(|(j, other)| !used[j] && ash.imports[i] == bsh.imports[j] && match (&imp.from, &other.from) {
            (Source::Input(a), Source::Input(b)) => a == b,
            (Source::Trace(a), Source::Trace(b)) => a == b,
            (Source::Parent(a), Source::Parent(b)) => parent.is_some_and(|m| m.same(a, b)),
            _ => false,
        });
        if let Some(j) = found { map.imports[i] = j; used[j] = true; }
        else { diff.push(format!("{path}: import {} has no matching source and shape", imp.name)); }
    }
    let mut used = vec![false; b.items.len()];
    for (i, item) in a.items.iter().enumerate() {
        let label = a.labels.get(i).map(String::as_str).unwrap_or("item");
        let at = format!("{path}/{label}[{i}]");
        match item {
            Item::Op(op) => {
                let inputs = input_shapes(op, ash).unwrap_or_default();
                let find = |scalar| b.items.iter().enumerate().position(|(j, other)| !used[j] && match other {
                    Item::Op(other) => node(op, other, &map, scalar, &inputs), _ => false,
                });
                let exact = find(true);
                let found = exact.or_else(|| find(false));
                if let Some(j) = found {
                    map.items[i] = j; used[j] = true;
                    if exact.is_none() {
                        let detail = if let (Node::Linear { ops: x, .. }, Item::Op(Node::Linear { ops: y, .. })) = (op, &b.items[j]) {
                            if x.len() != y.len() { format!("linear chain length {} -> {}", x.len(), y.len()) }
                            else { format!("compiled scalar graph changed at slots {:?}", x.iter().zip(y).enumerate().filter(|(i, (x, y))| !linear(x, y, inputs.get(*i))).map(|(i, _)| i).collect::<Vec<_>>()) }
                        } else { "compiled scalar graph or reducer changed".into() };
                        diff.push(format!("{at}: {detail}"));
                    }
                    if ash.of(&Ref::Local(i)) != bsh.of(&Ref::Local(j)) { diff.push(format!("{at}: output shape changed")); }
                } else { diff.push(format!("{at}: no item with matching operator and input edges")); }
            }
            Item::Sub(child) => {
                // Scope order is stable under lowering. Names are only a hint for diagnostics.
                let found = b.items.iter().enumerate().find(|(j, item)| !used[*j] && matches!(item, Item::Sub(c) if c.name == child.name))
                    .or_else(|| b.items.iter().enumerate().find(|(j, item)| !used[*j] && matches!(item, Item::Sub(_))));
                if let Some((j, Item::Sub(other))) = found {
                    let (ItemShapes::Sub(x), ItemShapes::Sub(y)) = (&ash.items[i], &bsh.items[j]) else { unreachable!() };
                    map.items[i] = j; used[j] = true;
                    map.exports[i] = compare(child, x, other, y, Some(&map), &format!("{path}/{}", child.name), diff);
                } else { diff.push(format!("{at}: no matching child scope")); }
            }
        }
    }
    for (j, used) in used.iter().enumerate() {
        if !used { diff.push(format!("{path}: extra item {}[{j}]", b.labels.get(j).map(String::as_str).unwrap_or("item"))); }
    }
    if a.binds.len() != b.binds.len() || a.binds.iter().any(|x| !b.binds.iter().any(|y| x.var == y.var && map.same(&x.value, &y.value))) {
        diff.push(format!("{path}: feedback bindings changed"));
    }
    let mut exports = vec![usize::MAX; a.exports.len()];
    let mut used = vec![false; b.exports.len()];
    for (i, export) in a.exports.iter().enumerate() {
        if let Some(j) = b.exports.iter().enumerate().position(|(j, other)| !used[j] && map.same(&export.value, &other.value) && (parent.is_some() || export.name == other.name)) {
            exports[i] = j; used[j] = true;
        } else { diff.push(format!("{path}: export {:?} changed", export.name)); }
    }
    if used.iter().any(|used| !used) { diff.push(format!("{path}: extra exports")); }
    exports
}
