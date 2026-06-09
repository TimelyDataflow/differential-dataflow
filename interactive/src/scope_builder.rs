//! Lifetime-bounded scope builder — a checked IR facade.
//!
//! Compile-time scoping discipline. A `scope(...)` introduces a fresh,
//! *invariant* scope brand `'r`; collections built inside are `Coll<'r>` and
//! can leave the scope only through `leave`, whose result `Left<'r>` is the
//! only value a scope body may return. The borrow checker then rejects, at
//! compile time, the two mistakes behind the explanation rewrite's level
//! errors:
//!
//! - **forgetting to leave** — returning a `Coll<'r>` from the body is a type
//!   error (the body must return `Left<'r>`);
//! - **smuggling an inner collection out** — a `Coll<'r>` cannot be stored in
//!   or returned to an outer scope, because `'r` is generative and invariant.
//!
//! Both are proven by the `compile_fail` doctests below.
//!
//! ### Importing outer collections (`enter`)
//!
//! Real programs reference outer collections inside a scope (e.g. SCC's `fwd`
//! joins the outer `edges`). `scope` takes an explicit `imports: &[Coll<'r>]`
//! list; each is re-branded to the inner `'c` and handed to the body. This is
//! sound — there is no public way to mint a `Coll` from a raw id, so the
//! re-brand can only happen here, on collections the caller already holds at
//! the outer scope. (For an iterative scope an outer reference needs no IR
//! node; the import is a pure re-brand.) The `ports_reach` test below builds
//! the full reach program this way.
//!
//! Scope of this increment: the scope maps to the existing iterative scope IR
//! (`Scope`/`EndScope` + a `Leave` at exit). The purely-structural
//! (timestamp-unchanged, no-`Leave`) variant needs backend support for
//! non-iterating scopes and is the next focused change.

use std::collections::BTreeMap;
use std::marker::PhantomData;

use crate::ir::{Id, LinearOp, Node, Program};
use crate::parse::{Projection, Reducer};

/// Invariant brand for a scope's lifetime: fresh per `scope`/`build` call,
/// un-unifiable with any other, so a handle can't silently cross scopes.
type Brand<'r> = PhantomData<fn(&'r ()) -> &'r ()>;

/// A collection handle confined to scope `'r` (wraps an IR node id).
#[derive(Clone, Copy)]
pub struct Coll<'r> {
    id: Id,
    _brand: Brand<'r>,
}

impl<'r> Coll<'r> {
    /// The underlying IR node id. Read-only: there is no public constructor
    /// from an id, so a raw id can't be re-branded into a scope.
    pub fn id(self) -> Id {
        self.id
    }
}

/// Proof that a `Coll<'r>` was surrendered via `leave` — the only value a
/// scope body is allowed to return.
pub struct Left<'r> {
    id: Id,
    _brand: Brand<'r>,
}

/// A scope brand token (Copy), threaded into constructors so their results
/// are branded with this scope.
#[derive(Clone, Copy)]
pub struct Scope<'r> {
    _brand: Brand<'r>,
}

/// IR builder: owns the node map and hands out scope-branded handles.
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

    fn wrap<'r>(id: Id) -> Coll<'r> {
        Coll { id, _brand: PhantomData }
    }

    /// A positional input.
    pub fn input<'r>(&mut self, _r: Scope<'r>, i: usize) -> Coll<'r> {
        let id = self.push(Node::Input(i));
        Self::wrap(id)
    }

    /// An iteration variable (the feedback edge of a `var`).
    pub fn variable<'r>(&mut self, _r: Scope<'r>) -> Coll<'r> {
        let id = self.push(Node::Variable);
        Self::wrap(id)
    }

    /// A linear (map/filter/…) transform of one collection.
    pub fn linear<'r>(&mut self, _r: Scope<'r>, c: Coll<'r>, ops: Vec<LinearOp>) -> Coll<'r> {
        let id = self.push(Node::Linear { input: c.id, ops });
        Self::wrap(id)
    }

    /// Concatenation of collections (all in the same scope).
    pub fn concat<'r>(&mut self, _r: Scope<'r>, cs: &[Coll<'r>]) -> Coll<'r> {
        let id = self.push(Node::Concat(cs.iter().map(|c| c.id).collect()));
        Self::wrap(id)
    }

    /// Arrange a collection (the IR's explicit arrangement node).
    pub fn arrange<'r>(&mut self, _r: Scope<'r>, c: Coll<'r>) -> Coll<'r> {
        let id = self.push(Node::Arrange(c.id));
        Self::wrap(id)
    }

    /// Join two collections (arranges both, as the IR requires).
    pub fn join<'r>(&mut self, r: Scope<'r>, left: Coll<'r>, right: Coll<'r>, projection: Projection) -> Coll<'r> {
        let l = self.arrange(r, left);
        let rt = self.arrange(r, right);
        let id = self.push(Node::Join { left: l.id, right: rt.id, projection });
        Self::wrap(id)
    }

    /// Reduce a collection (arranges its input).
    pub fn reduce<'r>(&mut self, r: Scope<'r>, c: Coll<'r>, reducer: Reducer) -> Coll<'r> {
        let a = self.arrange(r, c);
        let id = self.push(Node::Reduce { input: a.id, reducer });
        Self::wrap(id)
    }

    /// Close the loop: bind an iteration `variable` to its `value`.
    pub fn bind<'r>(&mut self, _r: Scope<'r>, variable: Coll<'r>, value: Coll<'r>) {
        self.push(Node::Bind { variable: variable.id, value: value.id });
    }

    /// Surrender a scope collection so the scope body can return it.
    pub fn leave<'r>(&mut self, _r: Scope<'r>, c: Coll<'r>) -> Left<'r> {
        Left { id: c.id, _brand: PhantomData }
    }

    /// Open a nested scope. `imports` are outer collections to use inside;
    /// each is re-branded to the body's fresh `'c`. Whatever the body `leave`s
    /// comes back as a collection in the parent scope `'r`. The `for<'c>`
    /// quantification makes `'c` generative — a `Coll<'c>` cannot escape.
    pub fn scope<'r, F>(&mut self, _parent: Scope<'r>, imports: &[Coll<'r>], body: F) -> Coll<'r>
    where
        F: for<'c> FnOnce(&mut Builder, Scope<'c>, Vec<Coll<'c>>) -> Left<'c>,
    {
        let import_ids: Vec<Id> = imports.iter().map(|c| c.id).collect();
        self.push(Node::Scope);
        // The import `Coll`s are minted here, at the body's `'c` (inferred from
        // the closure's parameter type) — the only place an outer id crosses in.
        let inner = body(
            self,
            Scope { _brand: PhantomData },
            import_ids.iter().map(|&id| Coll { id, _brand: PhantomData }).collect(),
        );
        self.push(Node::EndScope);
        // Leave one nesting level. (For an iterative scope this strips the
        // iteration coordinate; the exact level is recomputed during a real
        // lowering. scopes() doesn't depend on it.)
        let id = self.push(Node::Leave(inner.id, 1));
        Self::wrap(id)
    }
}

/// Build a program. The body runs in the root scope and returns the result,
/// which is exported as `"$result"`.
pub fn build<F>(body: F) -> Program
where
    F: for<'root> FnOnce(&mut Builder, Scope<'root>) -> Coll<'root>,
{
    let mut b = Builder { nodes: BTreeMap::new(), next: 0 };
    let result = body(&mut b, Scope { _brand: PhantomData });
    Program { nodes: b.nodes, export: vec![("$result".into(), result.id())] }
}

/// Discipline check: forgetting to `leave` is a type error — the scope body
/// must return `Left<'c>`, not a bare `Coll<'c>`.
///
/// ```compile_fail
/// use interactive::scope_builder::build;
/// let _p = build(|b, root| {
///     let _ = b.scope(root, &[], |b, r, _imp| {
///         let v = b.variable(r);
///         v                     // ERROR: expected `Left<'_>`, found `Coll<'_>`
///     });
///     b.input(root, 0)
/// });
/// ```
///
/// Discipline check: an inner collection cannot escape to an outer scope —
/// `'c` is generative, so it can't be stored in an outer-scoped variable.
///
/// ```compile_fail
/// use interactive::scope_builder::build;
/// let _p = build(|b, root| {
///     let mut escaped = None;
///     let _ = b.scope(root, &[], |b, r, _imp| {
///         let v = b.variable(r);
///         escaped = Some(v);    // ERROR: `v`'s scope `'c` would escape here
///         b.leave(r, v)
///     });
///     let _ = escaped;
///     b.input(root, 0)
/// });
/// ```
#[cfg(doctest)]
struct DisciplineDoctests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse::{FieldExpr, Projection, Reducer};

    #[test]
    fn builds_nested_iterative_scope() {
        // root: input e; scope { var v; v' = linear(v); bind v <- v'; leave v }
        //       result = concat(e, leaved)
        let p = build(|b, root| {
            let e = b.input(root, 0);
            let out = b.scope(root, &[], |b, r, _imp| {
                let v = b.variable(r);
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
    fn ports_reach() {
        // The reach program, built through the scope facade — outer `edges`
        // and `roots` are imported into the loop, the body can't leak, and the
        // result must be `leave`d. (Projections are well-formed but unexercised;
        // this checks structure, not execution.)
        let key0 = Projection { key: vec![FieldExpr::Index(0, 0)], val: vec![FieldExpr::Index(0, 1)] };
        let key0_only = Projection { key: vec![FieldExpr::Index(0, 0)], val: vec![] };
        let join_proj = Projection { key: vec![FieldExpr::Pos(2)], val: vec![] };

        let p = build(|b, root| {
            let in0 = b.input(root, 0);
            let edges = b.linear(root, in0, vec![LinearOp::Project(key0.clone())]);
            let in1 = b.input(root, 1);
            let roots = b.linear(root, in1, vec![LinearOp::Project(key0_only.clone())]);
            b.scope(root, &[edges, roots], |b, r, imp| {
                let edges = imp[0];
                let roots = imp[1];
                let reach = b.variable(r);
                let proposals = b.join(r, reach, edges, join_proj.clone());
                let body = b.concat(r, &[roots, proposals]);
                let next = b.reduce(r, body, Reducer::Distinct);
                b.bind(r, reach, next);
                b.leave(r, reach)
            })
        });

        let scopes = p.scopes();
        // One iterative scope (holds `reach`); edges/roots/Scope/Leave at root.
        assert_eq!(scopes.parent, vec![None, Some(0)]);
        assert_eq!(scopes.iterative, vec![false, true]);
        // The exported result is the Leave of the loop, sitting at the root.
        let (_, result) = &p.export[0];
        assert_eq!(scopes.of_node[result], 0);
        assert!(matches!(p.nodes[result], Node::Leave(_, _)));
        // The reach Variable and the join live inside scope 1.
        let in_scope_1 = |pred: fn(&Node) -> bool| {
            p.nodes.iter().any(|(id, n)| scopes.of_node[id] == 1 && pred(n))
        };
        assert!(in_scope_1(|n| matches!(n, Node::Variable)));
        assert!(in_scope_1(|n| matches!(n, Node::Join { .. })));
    }
}
