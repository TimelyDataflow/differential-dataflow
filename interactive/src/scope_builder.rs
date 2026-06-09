//! Lifetime-bounded scope builder — a checked IR facade.
//!
//! Two guarantees, both at compile or construction time:
//!
//! **Scoping discipline (lifetimes).** A `scope(...)` introduces a fresh,
//! *invariant* brand `'r`; collections built inside are `Coll<'r>` and leave
//! only through `leave` (whose `Left<'r>` is the sole value a scope body may
//! return). The borrow checker then rejects, at compile time, both mistakes
//! behind the explanation rewrite's *scope* errors: forgetting to leave, and
//! smuggling an inner collection out (see the `compile_fail` doctests).
//!
//! **Shape correctness (by construction).** Each `Coll` carries its `Shape`
//! `{k, v}`, computed at every op via the same width-summing logic the IR uses
//! (`explain::arities`). A projection field is a *whole-row* ref of width =
//! that row's arity, not one column — the miscount that made SCC's explanation
//! unsound. Building through this facade can't reintroduce it: see
//! `join_arity_sums_field_widths`.
//!
//! Outer collections are brought into a scope as explicit `imports`,
//! re-branded to the inner `'c` (sound: `Coll` has no public constructor from
//! a raw id). `ports_reach` builds the full reach program this way.
//!
//! Not yet covered (next increments): fold-depth typing for the time-as-data
//! `user_chain` (the other unsound miscount), and the purely-structural
//! (no-coord) scope variant.

use std::marker::PhantomData;
use std::collections::BTreeMap;

use crate::explain::arities::{apply_ops_arity, proj_arity};
use crate::ir::{Id, LinearOp, Node, Program};
use crate::parse::{Projection, Reducer};

/// The (key arity, value arity) of a collection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Shape {
    pub k: usize,
    pub v: usize,
}

/// Invariant brand for a scope's lifetime: fresh per `scope`/`build` call.
type Brand<'r> = PhantomData<fn(&'r ()) -> &'r ()>;

/// A collection handle confined to scope `'r`, carrying its shape.
#[derive(Clone, Copy)]
pub struct Coll<'r> {
    id: Id,
    shape: Shape,
    _brand: Brand<'r>,
}

impl<'r> Coll<'r> {
    /// The underlying IR node id (read-only; no public constructor from an id).
    pub fn id(self) -> Id {
        self.id
    }
    /// The collection's `(k, v)` shape.
    pub fn shape(self) -> Shape {
        self.shape
    }
}

/// Proof that a `Coll<'r>` was surrendered via `leave` — the only value a
/// scope body may return. Carries the shape so `scope` can re-stamp it.
pub struct Left<'r> {
    id: Id,
    shape: Shape,
    _brand: Brand<'r>,
}

/// A scope brand token (Copy), threaded into constructors.
#[derive(Clone, Copy)]
pub struct Scope<'r> {
    _brand: Brand<'r>,
}

/// IR builder: owns the node map and hands out scope-branded, shaped handles.
pub struct Builder {
    nodes: BTreeMap<Id, Node>,
    next: Id,
}

impl Builder {
    fn push(&mut self, n: Node) -> Id {
        let id = self.next;
        self.next += 1;
        self.nodes.insert(id, n);
        id
    }

    fn wrap<'r>(id: Id, shape: Shape) -> Coll<'r> {
        Coll { id, shape, _brand: PhantomData }
    }

    /// A positional input of declared shape `(k, v)`.
    pub fn input<'r>(&mut self, _r: Scope<'r>, i: usize, k: usize, v: usize) -> Coll<'r> {
        let id = self.push(Node::Input(i));
        Self::wrap(id, Shape { k, v })
    }

    /// An iteration variable of declared shape `(k, v)` (its `bind` must match).
    pub fn variable<'r>(&mut self, _r: Scope<'r>, k: usize, v: usize) -> Coll<'r> {
        let id = self.push(Node::Variable);
        Self::wrap(id, Shape { k, v })
    }

    /// A linear transform; shape follows the ops (width-summing for Project).
    pub fn linear<'r>(&mut self, _r: Scope<'r>, c: Coll<'r>, ops: Vec<LinearOp>) -> Coll<'r> {
        let (k, v) = apply_ops_arity((c.shape.k, c.shape.v), &ops);
        let id = self.push(Node::Linear { input: c.id, ops });
        Self::wrap(id, Shape { k, v })
    }

    /// Concatenation; all inputs must share a shape.
    pub fn concat<'r>(&mut self, _r: Scope<'r>, cs: &[Coll<'r>]) -> Coll<'r> {
        assert!(!cs.is_empty(), "concat: needs at least one input");
        let shape = cs[0].shape;
        assert!(cs.iter().all(|c| c.shape == shape), "concat: inputs must share a shape");
        let id = self.push(Node::Concat(cs.iter().map(|c| c.id).collect()));
        Self::wrap(id, shape)
    }

    /// Arrange a collection (shape-preserving).
    pub fn arrange<'r>(&mut self, _r: Scope<'r>, c: Coll<'r>) -> Coll<'r> {
        let id = self.push(Node::Arrange(c.id));
        Self::wrap(id, c.shape)
    }

    /// Join two collections; their key arities must match. Output shape is the
    /// projection over input rows `[key, left_val, right_val]` (width-summed).
    pub fn join<'r>(&mut self, r: Scope<'r>, left: Coll<'r>, right: Coll<'r>, projection: Projection) -> Coll<'r> {
        assert_eq!(left.shape.k, right.shape.k, "join: key arities must match");
        let rows = [left.shape.k, left.shape.v, right.shape.v];
        let shape = Shape {
            k: proj_arity(&projection.key, &rows),
            v: proj_arity(&projection.val, &rows),
        };
        let l = self.arrange(r, left);
        let rt = self.arrange(r, right);
        let id = self.push(Node::Join { left: l.id, right: rt.id, projection });
        Self::wrap(id, shape)
    }

    /// Reduce a collection; output value arity follows the reducer.
    pub fn reduce<'r>(&mut self, r: Scope<'r>, c: Coll<'r>, reducer: Reducer) -> Coll<'r> {
        let v = match reducer {
            Reducer::Distinct => 0,
            Reducer::Min => c.shape.v,
            Reducer::Count => 1,
        };
        let shape = Shape { k: c.shape.k, v };
        let a = self.arrange(r, c);
        let id = self.push(Node::Reduce { input: a.id, reducer });
        Self::wrap(id, shape)
    }

    /// Close the loop: bind `variable` to `value`; their shapes must match.
    pub fn bind<'r>(&mut self, _r: Scope<'r>, variable: Coll<'r>, value: Coll<'r>) {
        assert_eq!(variable.shape, value.shape, "bind: variable and value shapes must match");
        self.push(Node::Bind { variable: variable.id, value: value.id });
    }

    /// Surrender a scope collection so the scope body can return it.
    pub fn leave<'r>(&mut self, _r: Scope<'r>, c: Coll<'r>) -> Left<'r> {
        Left { id: c.id, shape: c.shape, _brand: PhantomData }
    }

    /// Open a nested scope. `imports` are outer collections to use inside,
    /// each re-branded to the body's fresh `'c`. Whatever the body `leave`s
    /// returns as a collection in the parent scope `'r`.
    pub fn scope<'r, F>(&mut self, _parent: Scope<'r>, imports: &[Coll<'r>], body: F) -> Coll<'r>
    where
        F: for<'c> FnOnce(&mut Builder, Scope<'c>, Vec<Coll<'c>>) -> Left<'c>,
    {
        let imports: Vec<(Id, Shape)> = imports.iter().map(|c| (c.id, c.shape)).collect();
        self.push(Node::Scope);
        // Import handles are minted here, at the body's `'c` (inferred from the
        // closure's parameter type) — the only place an outer id crosses in.
        let inner = body(
            self,
            Scope { _brand: PhantomData },
            imports.iter().map(|&(id, shape)| Coll { id, shape, _brand: PhantomData }).collect(),
        );
        self.push(Node::EndScope);
        let id = self.push(Node::Leave(inner.id, 1));
        Self::wrap(id, inner.shape)
    }
}

/// Build a program; the body runs in the root scope and returns the result,
/// exported as `"$result"`.
pub fn build<F>(body: F) -> Program
where
    F: for<'root> FnOnce(&mut Builder, Scope<'root>) -> Coll<'root>,
{
    let mut b = Builder { nodes: BTreeMap::new(), next: 0 };
    let result = body(&mut b, Scope { _brand: PhantomData });
    Program { nodes: b.nodes, export: vec![("$result".into(), result.id())] }
}

/// Discipline check: forgetting to `leave` is a type error.
///
/// ```compile_fail
/// use interactive::scope_builder::build;
/// let _p = build(|b, root| {
///     let _ = b.scope(root, &[], |b, r, _imp| {
///         let v = b.variable(r, 1, 0);
///         v                     // ERROR: expected `Left<'_>`, found `Coll<'_>`
///     });
///     b.input(root, 0, 2, 0)
/// });
/// ```
///
/// Discipline check: an inner collection can't escape to an outer scope.
///
/// ```compile_fail
/// use interactive::scope_builder::build;
/// let _p = build(|b, root| {
///     let mut escaped = None;
///     let _ = b.scope(root, &[], |b, r, _imp| {
///         let v = b.variable(r, 1, 0);
///         escaped = Some(v);    // ERROR: `v`'s scope `'c` would escape here
///         b.leave(r, v)
///     });
///     let _ = escaped;
///     b.input(root, 0, 2, 0)
/// });
/// ```
#[cfg(doctest)]
struct DisciplineDoctests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse::FieldExpr;

    #[test]
    fn builds_nested_iterative_scope() {
        let p = build(|b, root| {
            let e = b.input(root, 0, 1, 0);
            let out = b.scope(root, &[], |b, r, _imp| {
                let v = b.variable(r, 1, 0);
                let step = b.linear(r, v, vec![]);
                b.bind(r, v, step);
                b.leave(r, v)
            });
            b.concat(root, &[e, out])
        });

        let scopes = p.scopes();
        assert_eq!(scopes.parent, vec![None, Some(0)]);
        assert_eq!(scopes.children[0], vec![1]);
        assert_eq!(scopes.iterative, vec![false, true]);
        let (_, result) = &p.export[0];
        assert_eq!(scopes.of_node[result], 0);
        assert!(matches!(p.nodes[result], Node::Concat(_)));
    }

    #[test]
    fn join_arity_sums_field_widths() {
        // The bug-1 scenario: a join projection whose val uses whole-row `Pos`
        // refs to a multi-column side. Width-summing must give v = 1 + 2 = 3,
        // not the field-expr count of 2 (the miscount that broke SCC explain).
        let _p = build(|b, root| {
            let l = b.input(root, 0, 1, 1); // shape {k:1, v:1}
            let r = b.input(root, 1, 1, 2); // shape {k:1, v:2}
            let j = b.join(root, l, r, Projection {
                key: vec![FieldExpr::Pos(0)],
                val: vec![FieldExpr::Pos(1), FieldExpr::Pos(2)],
            });
            assert_eq!(j.shape(), Shape { k: 1, v: 3 });
            j
        });
    }

    #[test]
    fn ports_reach() {
        let key_kv = Projection { key: vec![FieldExpr::Index(0, 0)], val: vec![FieldExpr::Index(0, 1)] };
        let key_k = Projection { key: vec![FieldExpr::Index(0, 0)], val: vec![] };
        let join_proj = Projection { key: vec![FieldExpr::Pos(2)], val: vec![] };

        let p = build(|b, root| {
            let in0 = b.input(root, 0, 2, 0);
            let edges = b.linear(root, in0, vec![LinearOp::Project(key_kv.clone())]); // {1,1}
            let in1 = b.input(root, 1, 2, 0);
            let roots = b.linear(root, in1, vec![LinearOp::Project(key_k.clone())]); // {1,0}
            b.scope(root, &[edges, roots], |b, r, imp| {
                let edges = imp[0];
                let roots = imp[1];
                let reach = b.variable(r, 1, 0);
                let proposals = b.join(r, reach, edges, join_proj.clone()); // {1,0}
                let body = b.concat(r, &[roots, proposals]); // {1,0}
                let next = b.reduce(r, body, Reducer::Distinct); // {1,0}
                b.bind(r, reach, next); // shapes match
                b.leave(r, reach)
            })
        });

        let scopes = p.scopes();
        assert_eq!(scopes.parent, vec![None, Some(0)]);
        assert_eq!(scopes.iterative, vec![false, true]);
        let (_, result) = &p.export[0];
        assert_eq!(scopes.of_node[result], 0);
        assert!(matches!(p.nodes[result], Node::Leave(_, _)));
        let in_scope_1 = |pred: fn(&Node) -> bool| {
            p.nodes.iter().any(|(id, n)| scopes.of_node[id] == 1 && pred(n))
        };
        assert!(in_scope_1(|n| matches!(n, Node::Variable)));
        assert!(in_scope_1(|n| matches!(n, Node::Join { .. })));
    }
}
