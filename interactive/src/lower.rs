//! Lowering from AST to IR.
//!
//! Statement order within a scope does not affect semantics. At each scope
//! level we:
//!
//!   1. Bucket statements (and error on duplicate names).
//!   2. Pre-push `Variable` placeholders for every `var` so that anything in
//!      the scope can refer to them.
//!   3. Topologically lower `let` bindings and child scopes by dependency:
//!      each item is lowered once all the names it transitively needs at this
//!      level are bound. A cycle among `let`s is an error (use a `var` to
//!      introduce recursion).
//!   4. Lower the `export` expressions (root scope only).
//!   5. Lower each `var`'s body and emit a `Bind` from the placeholder to the
//!      resulting value.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use crate::parse::*;
use crate::ir::{Node, LinearOp, Id, Program};

struct Lowering {
    nodes: BTreeMap<Id, Node>,
    next_id: Id,
    /// Stack of value-name scopes; innermost last.
    scopes: Vec<HashMap<String, Id>>,
    /// Inner environments of named scopes, keyed by scope name; the `usize`
    /// records the scope's nesting depth (used by `Node::Leave`).
    named_scopes: HashMap<String, (usize, HashMap<String, Id>)>,
    level: usize,
}

impl Lowering {
    fn new() -> Self {
        Lowering {
            nodes: BTreeMap::new(),
            next_id: 0,
            scopes: vec![HashMap::new()],
            named_scopes: HashMap::new(),
            level: 0,
        }
    }

    fn push(&mut self, node: Node) -> Id { let id = self.next_id; self.next_id += 1; self.nodes.insert(id, node); id }
    fn bind_name(&mut self, name: String, id: Id) { self.scopes.last_mut().unwrap().insert(name, id); }
    fn resolve_name(&self, name: &str) -> Id {
        for scope in self.scopes.iter().rev() {
            if let Some(&id) = scope.get(name) { return id; }
        }
        panic!("Unresolved name: {}", name)
    }

    fn lower_program(mut self, stmts: Vec<Stmt>) -> Program {
        let mut exports = Vec::new();
        self.lower_stmts(stmts, &mut exports);
        if exports.is_empty() {
            panic!("Program has no `export` statement");
        }
        Program { nodes: self.nodes, export: exports }
    }

    fn lower_stmts(&mut self, stmts: Vec<Stmt>, exports: &mut Vec<(String, Id)>) {
        // ---- 1. Bucket statements; reject duplicate names. ----
        // `order` records the original textual order so the topological pass
        // is deterministic when several items are simultaneously ready.
        let mut vars: Vec<(String, Expr)> = Vec::new();
        let mut lets: HashMap<String, Expr> = HashMap::new();
        let mut scopes: HashMap<String, Vec<Stmt>> = HashMap::new();
        let mut order: Vec<(ItemKind, String)> = Vec::new();
        // Exports in declaration order (root scope only — rejected below if nested).
        let mut local_exports: Vec<(String, Expr)> = Vec::new();
        let mut seen: BTreeSet<String> = BTreeSet::new();
        for stmt in stmts {
            match stmt {
                Stmt::Let(name, expr) => {
                    if !seen.insert(name.clone()) { panic!("Duplicate name in scope: {}", name); }
                    order.push((ItemKind::Let, name.clone()));
                    lets.insert(name, expr);
                },
                Stmt::Var(name, expr) => {
                    if !seen.insert(name.clone()) { panic!("Duplicate name in scope: {}", name); }
                    vars.push((name, expr));
                },
                Stmt::Scope(name, body) => {
                    if !seen.insert(name.clone()) { panic!("Duplicate name in scope: {}", name); }
                    order.push((ItemKind::Scope, name.clone()));
                    scopes.insert(name, body);
                },
                Stmt::Export(name, expr) => {
                    // Exports are the program's output interface and only make
                    // sense at the root; reject nested ones rather than silently
                    // dropping them.
                    if self.level > 0 {
                        panic!("`export {:?}` is nested; exports are only allowed at the root scope", name);
                    }
                    local_exports.push((name, expr));
                },
            }
        }
        // Reject duplicate export names (root-only, so this is the whole
        // program's output interface).
        {
            let mut names: BTreeSet<&str> = BTreeSet::new();
            for (n, _) in &local_exports {
                if !names.insert(n) {
                    panic!("Duplicate export name: {:?}", n);
                }
            }
        }

        // ---- 2. Pre-bind `Variable` placeholders. ----
        for (name, _) in &vars {
            let id = self.push(Node::Variable);
            self.bind_name(name.clone(), id);
        }

        // ---- 3. Topologically lower lets and child scopes. ----
        // Deps for a let: free names of its expression that are themselves
        // defined as let/scope at this level (vars are already bound).
        // Deps for a scope: free names that escape the scope body, restricted
        // similarly.
        let defined_topo: BTreeSet<&str> = lets.keys().chain(scopes.keys()).map(String::as_str).collect();
        let mut remaining_deps: HashMap<String, BTreeSet<String>> = HashMap::new();
        for (name, expr) in &lets {
            remaining_deps.insert(name.clone(), expr_deps(expr, &defined_topo, name));
        }
        for (name, body) in &scopes {
            remaining_deps.insert(name.clone(), scope_body_deps(body, &defined_topo, name));
        }
        drop(defined_topo);

        // Greedy topo: scan `order` for an item with no remaining deps; lower
        // it and remove it from every other item's dep set. Repeat until done.
        let mut pending: Vec<(ItemKind, String)> = order;
        while !pending.is_empty() {
            let pick = pending.iter().position(|(_, n)| remaining_deps[n].is_empty());
            let Some(idx) = pick else {
                let stuck: Vec<String> = pending.iter().map(|(_, n)| n.clone()).collect();
                panic!("Cyclic dependency among let/scope bindings: {:?}. Use `var` to introduce recursion.", stuck);
            };
            let (kind, name) = pending.remove(idx);
            remaining_deps.remove(&name);
            for deps in remaining_deps.values_mut() { deps.remove(&name); }

            match kind {
                ItemKind::Let => {
                    let expr = lets.remove(&name).unwrap();
                    let id = self.lower_expr(expr);
                    self.bind_name(name, id);
                },
                ItemKind::Scope => {
                    let body = scopes.remove(&name).unwrap();
                    self.push(Node::Scope);
                    self.level += 1;
                    self.scopes.push(HashMap::new());
                    // Exports are root-only (lower_stmts rejects nested ones),
                    // so this stays empty.
                    let mut inner_exports = Vec::new();
                    self.lower_stmts(body, &mut inner_exports);
                    let inner_scope = self.scopes.pop().unwrap();
                    let scope_level = self.level;
                    self.named_scopes.insert(name, (scope_level, inner_scope));
                    self.level -= 1;
                    self.push(Node::EndScope);
                },
            }
        }

        // ---- 4. Lower export expressions (if any) and record them. ----
        for (name, expr) in local_exports {
            let id = self.lower_expr(expr);
            exports.push((name, id));
        }

        // ---- 5. Lower var bodies and emit Bind nodes. ----
        for (name, expr) in vars {
            let var_id = self.resolve_name(&name);
            let value_id = self.lower_expr(expr);
            self.push(Node::Bind { variable: var_id, value: value_id });
        }
    }

    fn lower_expr(&mut self, expr: Expr) -> Id {
        match expr {
            Expr::Input(n) => self.push(Node::Input(n)),
            Expr::Import(name) => self.push(Node::Import { name }),
            Expr::Name(name) => self.resolve_name(&name),
            Expr::Qualified(scope_name, name) => {
                let (scope_level, inner_id) = {
                    let (lvl, scope) = self.named_scopes.get(&scope_name).unwrap_or_else(|| panic!("Unknown scope: {}", scope_name));
                    (*lvl, *scope.get(&name).unwrap_or_else(|| panic!("Unknown name {}::{}", scope_name, name)))
                };
                self.push(Node::Leave(inner_id, scope_level))
            },
            Expr::Map(input, proj) => { let id = self.lower_expr(*input); self.push(Node::Linear { input: id, ops: vec![LinearOp::Project(proj)] }) },
            Expr::Join(left, right, proj) => {
                let l = self.lower_expr(*left);
                let l = self.push(Node::Arrange(l));
                let r = self.lower_expr(*right);
                let r = self.push(Node::Arrange(r));
                self.push(Node::Join { left: l, right: r, projection: proj })
            },
            Expr::Reduce(input, reducer) => {
                let id = self.lower_expr(*input);
                let id = self.push(Node::Arrange(id));
                self.push(Node::Reduce { input: id, reducer })
            },
            Expr::Filter(input, cond)  => { let id = self.lower_expr(*input); self.push(Node::Linear { input: id, ops: vec![LinearOp::Filter(cond)] }) },
            Expr::Negate(input)        => { let id = self.lower_expr(*input); self.push(Node::Linear { input: id, ops: vec![LinearOp::Negate] }) },
            Expr::EnterAt(input, fld)  => { let id = self.lower_expr(*input); self.push(Node::Linear { input: id, ops: vec![LinearOp::EnterAt(fld)] }) },
            Expr::LiftIter(input)      => { let id = self.lower_expr(*input); self.push(Node::Linear { input: id, ops: vec![LinearOp::LiftIter] }) },
            Expr::Inspect(input, lab)  => { let id = self.lower_expr(*input); self.push(Node::Inspect { input: id, label: lab }) },
            Expr::Concat(exprs) => { let ids: Vec<Id> = exprs.into_iter().map(|e| self.lower_expr(e)).collect(); self.push(Node::Concat(ids)) },
            Expr::Arrange(input) => { let id = self.lower_expr(*input); self.push(Node::Arrange(id)) },
        }
    }
}

#[derive(Clone, Copy)]
enum ItemKind { Let, Scope }

/// Free names of `expr` that are defined at this scope level, excluding the
/// item's own name (a let referencing its own name is just an unresolved
/// reference for the current pass — not a self-dep).
fn expr_deps(expr: &Expr, defined: &BTreeSet<&str>, self_name: &str) -> BTreeSet<String> {
    let mut free = BTreeSet::new();
    expr_free_names(expr, &mut free);
    free.into_iter()
        .filter(|n| *n != self_name && defined.contains(n))
        .map(String::from)
        .collect()
}

/// Free names of a scope body (names referenced inside that aren't bound
/// inside), restricted to names defined at the enclosing level.
fn scope_body_deps(body: &[Stmt], defined: &BTreeSet<&str>, self_name: &str) -> BTreeSet<String> {
    let mut free = BTreeSet::new();
    collect_body_free_names(body, &mut free);
    free.into_iter()
        .filter(|n| *n != self_name && defined.contains(n))
        .map(String::from)
        .collect()
}

/// Names this expression refers to that the surrounding scope must resolve.
/// For `name`, the name itself; for `scope::field`, the scope name (the
/// field is resolved within that scope's environment, not the enclosing one).
fn expr_free_names<'a>(expr: &'a Expr, out: &mut BTreeSet<&'a str>) {
    match expr {
        Expr::Input(_) | Expr::Import(_) => {},
        Expr::Name(n) => { out.insert(n.as_str()); },
        Expr::Qualified(scope, _) => { out.insert(scope.as_str()); },
        Expr::Map(e, _) | Expr::Reduce(e, _) | Expr::Filter(e, _)
            | Expr::Negate(e) | Expr::EnterAt(e, _) | Expr::LiftIter(e)
            | Expr::Inspect(e, _) | Expr::Arrange(e) => expr_free_names(e, out),
        Expr::Join(l, r, _) => { expr_free_names(l, out); expr_free_names(r, out); },
        Expr::Concat(es) => { for e in es { expr_free_names(e, out); } },
    }
}

/// Recursively collect names referenced in `body`'s expressions that aren't
/// bound somewhere within `body` itself. The recursion descends through
/// nested scopes, masking out their local bindings as it goes.
fn collect_body_free_names<'a>(body: &'a [Stmt], out: &mut BTreeSet<&'a str>) {
    let mut local: BTreeSet<&'a str> = BTreeSet::new();
    for stmt in body {
        match stmt {
            Stmt::Let(n, _) | Stmt::Var(n, _) | Stmt::Scope(n, _) => { local.insert(n.as_str()); },
            Stmt::Export(_, _) => {},
        }
    }
    let mut inner: BTreeSet<&'a str> = BTreeSet::new();
    for stmt in body {
        match stmt {
            Stmt::Let(_, e) | Stmt::Var(_, e) | Stmt::Export(_, e) => expr_free_names(e, &mut inner),
            Stmt::Scope(_, b) => collect_body_free_names(b, &mut inner),
        }
    }
    for n in inner { if !local.contains(n) { out.insert(n); } }
}

pub fn lower(stmts: Vec<Stmt>) -> Program {
    let program = Lowering::new().lower_program(stmts);
    program.validate_lift_iter().unwrap_or_else(|e| panic!("{}", e));
    program
}
