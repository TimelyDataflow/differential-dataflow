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
//! See the `compile_fail` doctests below for both, made executable.
//!
//! Scope of this increment: it establishes the *discipline*. The scope maps
//! to the existing iterative scope IR (`Scope`/`EndScope` + a `Leave` at
//! exit); the purely-structural (timestamp-unchanged, no-`Leave`) variant and
//! importing an outer collection *into* a scope (`enter`, which needs a
//! borrow-based lifetime relation rather than a generative one) are the next
//! refinements. The constructs built here are self-contained, so no `enter`
//! is required yet.

use std::collections::BTreeMap;
use std::marker::PhantomData;

use crate::ir::{Id, LinearOp, Node, Program};

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
    /// The underlying IR node id. (Read-only; minting a `Coll` is the
    /// builder's job, so a raw id can't be re-branded into a scope.)
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

    /// Close the loop: bind an iteration `variable` to its `value`.
    pub fn bind<'r>(&mut self, _r: Scope<'r>, variable: Coll<'r>, value: Coll<'r>) {
        self.push(Node::Bind { variable: variable.id, value: value.id });
    }

    /// Surrender a scope collection so the scope body can return it.
    pub fn leave<'r>(&mut self, _r: Scope<'r>, c: Coll<'r>) -> Left<'r> {
        Left { id: c.id, _brand: PhantomData }
    }

    /// Open a nested scope. The body receives a fresh brand `'c`; whatever it
    /// `leave`s comes back as a collection in the parent scope `'r`. The
    /// `for<'c>` quantification is what makes `'c` generative — a `Coll<'c>`
    /// cannot escape the closure.
    pub fn scope<'r, F>(&mut self, _parent: Scope<'r>, body: F) -> Coll<'r>
    where
        F: for<'c> FnOnce(&mut Builder, Scope<'c>) -> Left<'c>,
    {
        self.push(Node::Scope);
        let inner = body(self, Scope { _brand: PhantomData });
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
///     let _ = b.scope(root, |b, r| {
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
///     let _ = b.scope(root, |b, r| {
///         let v = b.variable(r);
///         escaped = Some(v);    // ERROR: `v`'s scope `'c` would escape here
///         b.leave(r, v)
///     });
///     let _ = escaped;
///     b.input(root, 0)
/// });
/// ```
///
/// And the well-formed version compiles and runs (see the unit test below).
#[cfg(doctest)]
struct DisciplineDoctests;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_nested_iterative_scope() {
        // root: input e; scope { var v; v' = linear(v); bind v <- v'; leave v }
        //       result = concat(e, leaved)
        let p = build(|b, root| {
            let e = b.input(root, 0);
            let out = b.scope(root, |b, r| {
                let v = b.variable(r);
                let step = b.linear(r, v, vec![]);
                b.bind(r, v, step);
                b.leave(r, v)
            });
            b.concat(root, &[e, out])
        });

        // Structure: one nested scope, tagged iterative (it holds the var).
        let scopes = p.scopes();
        assert_eq!(scopes.parent, vec![None, Some(0)]);
        assert_eq!(scopes.children[0], vec![1]);
        assert_eq!(scopes.iterative, vec![false, true]);

        // The exported result is the top-level Concat (in the root scope).
        let (_, result) = &p.export[0];
        assert_eq!(scopes.of_node[result], 0);
        assert!(matches!(p.nodes[result], Node::Concat(_)));
    }
}
