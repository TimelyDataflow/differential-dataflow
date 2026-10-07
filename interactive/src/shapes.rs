//! Static shapes for a program's collections.
//!
//! Every collection has one shape for all of its updates: a shape for its data, its time, and
//! its diff. The data shape is a `(key, val)` pair of corgi shapes, while the IR keeps that split.
//! The time is a product of integers, one for the outer coordinate and one per scope depth.
//! The diff is an integer.
//!
//! Shapes start at the program's sources, from their ascriptions or from the caller, and follow
//! from each operator's rule. Scalar terms are typed by corgi's typer (`corgi::logic::shape_of_term`),
//! so a shape here is the shape the corgi backend would compile against. Feedback variables take
//! the shape of what is bound to them, found by iterating to a fixed point, unless they declare
//! one. A declared variable or export is checked against the shape inferred for its value.
//!
//! A collection whose shape cannot be determined is `None`, and the reason is recorded as
//! *unknown*: some source upstream has no shape. Shapes that are known but disagree (a term
//! that does not type, a join of different keys, a variable bound to another shape than it is
//! used at) are *conflicts*: the program has no meaning at those shapes.

use corgi::Shape;

use crate::corgi::logic::shape_of_term;
use crate::ir::{LinearOp, Reducer};
use crate::scope_ir as st;

/// The shape of a collection's updates.
#[derive(Clone, Debug, PartialEq)]
pub struct CollShape {
    pub key: Shape,
    pub val: Shape,
    /// The number of integer coordinates in a time: the outer one, plus one per scope depth.
    pub time: usize,
    pub diff: Shape,
}

impl CollShape {
    fn new(key: Shape, val: Shape, depth: usize) -> Self {
        CollShape { key, val, time: depth + 1, diff: Shape::Prim(64) }
    }
}

/// The shapes of one scope's collections, indexed as the scope indexes them.
#[derive(Clone, Debug, Default)]
pub struct ScopeShapes {
    pub imports: Vec<Option<CollShape>>,
    pub vars: Vec<Option<CollShape>>,
    pub items: Vec<ItemShapes>,
    pub exports: Vec<Option<CollShape>>,
}

#[derive(Clone, Debug)]
pub enum ItemShapes {
    Op(Option<CollShape>),
    Sub(ScopeShapes),
}

impl ScopeShapes {
    /// The shape of `r`, a reference within this scope.
    pub fn of(&self, r: &st::Ref) -> Option<CollShape> {
        match r {
            st::Ref::Local(i) => match &self.items[*i] { ItemShapes::Op(s) => s.clone(), ItemShapes::Sub(_) => None },
            st::Ref::Import(i) => self.imports[*i].clone(),
            st::Ref::Var(v) => self.vars[*v].clone(),
            st::Ref::ChildExport(i, e) => match &self.items[*i] { ItemShapes::Sub(c) => c.exports[*e].clone(), ItemShapes::Op(_) => None },
        }
    }
    /// The scope at `path` (indices of `Sub` items from this one).
    pub fn at(&self, path: &[usize]) -> &ScopeShapes {
        match path.split_first() {
            None => self,
            Some((c, rest)) => match &self.items[*c] { ItemShapes::Sub(s) => s.at(rest), ItemShapes::Op(_) => panic!("not a scope") },
        }
    }
}

/// What inference could not settle.
#[derive(Clone, Debug, Default)]
pub struct Problems {
    /// Collections without a shape, because a source upstream has none.
    pub unknown: Vec<String>,
    /// Shapes that disagree: the program has no meaning at them.
    pub conflicts: Vec<String>,
}

/// Infer the shapes of `program`'s collections. `source` supplies the shape of a root source
/// that carries no ascription.
pub fn infer(program: &st::Program, source: &dyn Fn(&st::Source) -> Option<(Shape, Shape)>) -> (ScopeShapes, Problems) {
    let imports: Vec<Option<CollShape>> = program.root.imports.iter().map(|imp| {
        imp.shape.clone().or_else(|| source(&imp.from)).map(|(k, v)| CollShape::new(k, v, 0))
    }).collect();
    let mut problems = Problems::default();
    for (imp, s) in program.root.imports.iter().zip(&imports) {
        if s.is_none() { problems.unknown.push(format!("root: source `{}` has no shape", imp.name)); }
    }
    let shapes = infer_scope(&program.root, imports, 0, "root", &mut problems);
    (shapes, problems)
}

fn infer_scope(s: &st::Scope, imports: Vec<Option<CollShape>>, depth: usize, at: &str, problems: &mut Problems) -> ScopeShapes {
    // A declared variable starts at its declared shape; any other takes the shape bound to it.
    let mut vars: Vec<Option<CollShape>> = s.vars.iter().map(|v| v.shape.clone().map(|(k, v)| CollShape::new(k, v, depth))).collect();
    // Iterate until the variables' shapes stop changing; report only the last pass's errors.
    loop {
        let mut pass = Problems::default();
        let mut shapes = ScopeShapes { imports: imports.clone(), vars: vars.clone(), items: Vec::new(), exports: Vec::new() };
        for (i, item) in s.items.iter().enumerate() {
            let here = format!("{at}/n{i}");
            let shape = match item {
                st::Item::Op(node) => ItemShapes::Op(infer_node(node, &shapes, depth, &here, &mut pass)),
                st::Item::Sub(child) => {
                    let child_imports = child.imports.iter().map(|imp| match &imp.from {
                        st::Source::Parent(r) => shapes.of(r).map(|c| CollShape::new(c.key, c.val, depth + 1)),
                        other => { pass.conflicts.push(format!("{here}: a nested scope imports {other:?}")); None }
                    }).collect();
                    let name = format!("{at}/{}", child.name);
                    let c = infer_scope(child, child_imports, depth + 1, &name, &mut pass);
                    ItemShapes::Sub(c)
                }
            };
            shapes.items.push(shape);
        }
        let mut changed = false;
        for b in &s.binds {
            let value = shapes.of(&b.value);
            match (&vars[b.var], value) {
                (None, Some(v)) => { vars[b.var] = Some(v); changed = true; }
                (Some(old), Some(v)) if *old != v => pass.conflicts.push(format!("{at}: variable `{}` is bound at {} ; {} but declared or used at {} ; {}", s.vars[b.var].name, v.key, v.val, old.key, old.val)),
                (None, None) => pass.unknown.push(format!("{at}: variable `{}` has no shape", s.vars[b.var].name)),
                _ => {}
            }
        }
        if changed { continue; }
        // Exports leave to the parent's depth. A declared export must match its value's shape,
        // and supplies it where the value's is unknown.
        shapes.exports = s.exports.iter().map(|e| {
            let value = shapes.of(&e.value);
            let (k, v) = match (value, &e.shape) {
                (Some(c), Some((k, v))) if c.key != *k || c.val != *v => {
                    pass.conflicts.push(format!("{at}: export `{}` is declared ({k} ; {v}) but is ({} ; {})", e.name, c.key, c.val));
                    (c.key, c.val)
                }
                (Some(c), _) => (c.key, c.val),
                (None, Some((k, v))) => (k.clone(), v.clone()),
                (None, None) => return None,
            };
            Some(CollShape::new(k, v, depth.saturating_sub(1)))
        }).collect();
        problems.unknown.extend(pass.unknown);
        problems.conflicts.extend(pass.conflicts);
        return shapes;
    }
}

/// The shapes a linear chain sees: before each op, then after the last.
pub fn linear_shapes(ops: &[LinearOp], key: Shape, val: Shape) -> Result<Vec<(Shape, Shape)>, String> {
    let (mut k, mut v) = (key, val);
    let mut steps = Vec::with_capacity(ops.len() + 1);
    for op in ops {
        steps.push((k.clone(), v.clone()));
        let env = [k.clone(), v.clone()];
        match op {
            LinearOp::Project(p) => {
                let nk = shape_of_term(&p.key, &env, None).map_err(|e| format!("map key: {e}"))?;
                let nv = shape_of_term(&p.val, &env, None).map_err(|e| format!("map val: {e}"))?;
                (k, v) = (nk, nv);
            }
            LinearOp::Filter(t) | LinearOp::EnterAt(t) => match shape_of_term(t, &env, None)? {
                Shape::Prim(64) => {}
                s => return Err(format!("predicate or delay of shape {s}, not an integer")),
            },
            LinearOp::Negate => {}
            LinearOp::FlatMap(t) => match shape_of_term(t, &env, None)? {
                Shape::List(e) => v = Shape::Prod(vec![Shape::Prim(64), *e]),
                s => return Err(format!("flatmap of shape {s}, not a list")),
            },
            // As `backend::vec::append_iter`: extend a tuple, or pair any other value.
            LinearOp::LiftIter => v = match v {
                Shape::Prod(mut fs) => { fs.push(Shape::Prim(64)); Shape::Prod(fs) }
                Shape::Unit => Shape::Prod(vec![Shape::Prim(64)]),
                other => Shape::Prod(vec![other, Shape::Prim(64)]),
            },
        }
    }
    steps.push((k, v));
    Ok(steps)
}

fn infer_node(node: &st::Node, shapes: &ScopeShapes, depth: usize, at: &str, problems: &mut Problems) -> Option<CollShape> {
    let mut fail = |e: String| { problems.conflicts.push(format!("{at}: {e}")); None };
    match node {
        st::Node::Linear { input, ops } => {
            let c = shapes.of(input)?;
            match linear_shapes(ops, c.key, c.val) {
                Ok(mut steps) => { let (k, v) = steps.pop().unwrap(); Some(CollShape::new(k, v, depth)) }
                Err(e) => fail(e),
            }
        }
        st::Node::Concat(refs) => {
            let known: Vec<CollShape> = refs.iter().filter_map(|r| shapes.of(r)).collect();
            let first = known.first()?.clone();
            if let Some(other) = known.iter().find(|c| **c != first) {
                return fail(format!("concat of {} ; {} and {} ; {}", first.key, first.val, other.key, other.val));
            }
            Some(first)
        }
        st::Node::Arrange(r) | st::Node::Inspect { input: r, .. } => shapes.of(r),
        st::Node::Join { left, right, projection } => {
            let (l, r) = (shapes.of(left)?, shapes.of(right)?);
            if l.key != r.key { return fail(format!("join keys {} and {}", l.key, r.key)); }
            let env = [l.key, l.val, r.val];
            let k = match shape_of_term(&projection.key, &env, None) { Ok(s) => s, Err(e) => return fail(format!("join key: {e}")) };
            let v = match shape_of_term(&projection.val, &env, None) { Ok(s) => s, Err(e) => return fail(format!("join val: {e}")) };
            Some(CollShape::new(k, v, depth))
        }
        st::Node::Reduce { input, reducer } => {
            let c = shapes.of(input)?;
            let v = match reducer {
                Reducer::Min => c.val,
                Reducer::Distinct => Shape::Unit,
                Reducer::Count => Shape::Prod(vec![Shape::Prim(64)]),
                Reducer::Collect => Shape::List(Box::new(c.val)),
            };
            Some(CollShape::new(c.key, v, depth))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{lower, parse};

    fn shapes_of(src: &str) -> (ScopeShapes, Problems) {
        let mut p = lower::lower_tree(parse::pipe::parse(src));
        p.optimize();
        infer(&p, &|_| None)
    }

    const REACH: &str = r#"
        let edges = input 0 : ((int, int) ; ()) | key($0[0] ; $0[1]);
        let roots = input 1 : ((int) ; ()) | key($0[0] ;);
        reach: {
            let proposals = reach | join(edges, ($2 ;));
            var reach = roots + proposals | distinct;
        }
        export "result" = reach::reach | key(;) | arrange;
    "#;

    #[test]
    fn declared_sources_shape_every_collection() {
        let (shapes, problems) = shapes_of(REACH);
        assert!(problems.unknown.is_empty() && problems.conflicts.is_empty(), "{problems:?}");
        let ItemShapes::Sub(reach) = &shapes.items.iter().find(|i| matches!(i, ItemShapes::Sub(_))).unwrap() else { unreachable!() };
        let var = reach.vars[0].clone().unwrap();
        assert_eq!((var.key, var.val, var.time), (Shape::Prod(vec![Shape::Prim(64)]), Shape::Unit, 2));
    }

    #[test]
    fn undeclared_sources_leave_collections_unshaped() {
        let (_, problems) = shapes_of(&REACH.replace(" : ((int, int) ; ())", "").replace(" : ((int) ; ())", ""));
        assert!(problems.conflicts.is_empty(), "{problems:?}");
        assert!(problems.unknown.iter().any(|e| e.contains("`input0` has no shape")), "{problems:?}");
        assert!(problems.unknown.iter().any(|e| e.contains("variable `reach` has no shape")), "{problems:?}");
    }

    #[test]
    fn declared_variables_and_exports_are_checked() {
        let declared = REACH
            .replace("var reach =", "var reach : ((int) ; ()) =")
            .replace("export \"result\" =", "export \"result\" : (() ; ()) =");
        let (_, problems) = shapes_of(&declared);
        assert!(problems.unknown.is_empty() && problems.conflicts.is_empty(), "{problems:?}");

        let (_, problems) = shapes_of(&REACH.replace("var reach =", "var reach : ((int, int) ; ()) ="));
        assert!(problems.conflicts.iter().any(|e| e.contains("variable `reach`")), "{problems:?}");
        let (_, problems) = shapes_of(&REACH.replace("export \"result\" =", "export \"result\" : ((int) ; ()) ="));
        assert!(problems.conflicts.iter().any(|e| e.contains("export `result`")), "{problems:?}");
    }

    #[test]
    fn a_declared_export_supplies_an_unknown_shape() {
        let (shapes, problems) = shapes_of(r#"export "out" : ((int) ; ()) = input 0;"#);
        assert!(problems.conflicts.is_empty(), "{problems:?}");
        let out = shapes.exports[0].clone().unwrap();
        assert_eq!((out.key, out.val), (Shape::Prod(vec![Shape::Prim(64)]), Shape::Unit));
    }
}
