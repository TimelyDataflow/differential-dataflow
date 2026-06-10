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

// ===== Scope-tree lowering (AST -> scope_ir) =====
//
// Produces the tree IR (see `scope_ir`): each `{ .. }` becomes an
// owned child scope, cross-scope flow becomes explicit import/export edges,
// and feedback vars are first-class. `input`/`import` external sources are
// accepted at the root scope only. Shapes are not stored on the tree; a
// shape pass derives them when a consumer needs them.

use crate::scope_ir as st;

pub fn lower_tree(stmts: Vec<Stmt>) -> st::Program {
    let empty = HashMap::new();
    st::Program { root: lower_scope_tree("root", &stmts, &empty, &[], true) }
}

/// Expr-lowering state for one scope. Resolves names to scope-local `Ref`s and
/// accumulates this scope's nodes and (input/trace) imports.
struct ScopeLower {
    is_root: bool,
    items: Vec<st::Item>,
    imports: Vec<st::Import>,
    input_import: HashMap<usize, usize>, // input n  -> import idx (root, deduped)
    trace_import: HashMap<String, usize>, // trace name -> import idx (root, deduped)
    env: HashMap<String, st::Ref>,        // local names + pre-created imports -> Ref
    child_idx: HashMap<String, usize>,    // child scope name -> children index
    child_exports: HashMap<String, Vec<String>>, // child name -> its export field order
}

impl ScopeLower {
    fn push(&mut self, n: st::Node) -> st::Ref {
        let i = self.items.len();
        self.items.push(st::Item::Op(n));
        st::Ref::Local(i)
    }
    fn input_ref(&mut self, n: usize) -> st::Ref {
        assert!(self.is_root, "`input {}` used outside the root scope (not yet supported)", n);
        if let Some(&i) = self.input_import.get(&n) { return st::Ref::Import(i); }
        let i = self.imports.len();
        self.imports.push(st::Import { name: format!("input{}", n), from: st::Source::Input(n) });
        self.input_import.insert(n, i);
        st::Ref::Import(i)
    }
    fn trace_ref(&mut self, name: &str) -> st::Ref {
        assert!(self.is_root, "`import {:?}` used outside the root scope (not yet supported)", name);
        if let Some(&i) = self.trace_import.get(name) { return st::Ref::Import(i); }
        let i = self.imports.len();
        self.imports.push(st::Import { name: name.to_string(), from: st::Source::Trace(name.to_string()) });
        self.trace_import.insert(name.to_string(), i);
        st::Ref::Import(i)
    }
    fn qualified_ref(&self, scope: &str, field: &str) -> st::Ref {
        let cidx = *self.child_idx.get(scope).unwrap_or_else(|| panic!("unknown scope `{}`", scope));
        let pos = self.child_exports[scope].iter().position(|f| f == field)
            .unwrap_or_else(|| panic!("scope `{}` does not export `{}`", scope, field));
        st::Ref::ChildExport(cidx, pos)
    }
    fn lower_expr(&mut self, e: &Expr) -> st::Ref {
        match e {
            Expr::Input(n) => self.input_ref(*n),
            Expr::Import(name) => self.trace_ref(name),
            Expr::Name(name) => self.env.get(name).cloned()
                .unwrap_or_else(|| panic!("unresolved name `{}`", name)),
            Expr::Qualified(s, f) => self.qualified_ref(s, f),
            Expr::Map(e, p)     => { let r = self.lower_expr(e); self.push(st::Node::Linear { input: r, ops: vec![LinearOp::Project(p.clone())] }) },
            Expr::Filter(e, c)  => { let r = self.lower_expr(e); self.push(st::Node::Linear { input: r, ops: vec![LinearOp::Filter(c.clone())] }) },
            Expr::Negate(e)     => { let r = self.lower_expr(e); self.push(st::Node::Linear { input: r, ops: vec![LinearOp::Negate] }) },
            Expr::EnterAt(e, f) => { let r = self.lower_expr(e); self.push(st::Node::Linear { input: r, ops: vec![LinearOp::EnterAt(f.clone())] }) },
            Expr::LiftIter(e)   => { let r = self.lower_expr(e); self.push(st::Node::Linear { input: r, ops: vec![LinearOp::LiftIter] }) },
            Expr::Arrange(e)    => { let r = self.lower_expr(e); self.push(st::Node::Arrange(r)) },
            // Join/Reduce consume arrangements; arrange their inputs explicitly
            // (as the flat lowering does) so identical arrangements are visible
            // to `optimize`'s within-scope dedup and shared at render.
            Expr::Join(l, r, p) => {
                let lr = self.lower_expr(l); let la = self.push(st::Node::Arrange(lr));
                let rr = self.lower_expr(r); let ra = self.push(st::Node::Arrange(rr));
                self.push(st::Node::Join { left: la, right: ra, projection: p.clone() })
            },
            Expr::Reduce(e, red)=> {
                let r = self.lower_expr(e); let a = self.push(st::Node::Arrange(r));
                self.push(st::Node::Reduce { input: a, reducer: red.clone() })
            },
            Expr::Inspect(e, l) => { let r = self.lower_expr(e); self.push(st::Node::Inspect { input: r, label: l.clone() }) },
            Expr::Concat(es)    => { let rs: Vec<st::Ref> = es.iter().map(|e| self.lower_expr(e)).collect(); self.push(st::Node::Concat(rs)) },
        }
    }
}

fn lower_scope_tree(
    scope_name: &str,
    body: &[Stmt],
    parent_env: &HashMap<String, st::Ref>,
    exports_wanted: &[String],
    is_root: bool,
) -> st::Scope {
    let mut s = ScopeLower {
        is_root,
        items: Vec::new(),
        imports: Vec::new(),
        input_import: HashMap::new(),
        trace_import: HashMap::new(),
        env: HashMap::new(),
        child_idx: HashMap::new(),
        child_exports: HashMap::new(),
    };

    // Pre-create imports from this scope's transitive free names (names used
    // here or in a descendant but not defined here). Doing it up front handles
    // cross-level references: an outer name a grandchild needs is imported into
    // every scope on the path.
    if !is_root {
        let mut free: BTreeSet<&str> = BTreeSet::new();
        collect_body_free_names(body, &mut free);
        for name in free {
            let from = parent_env.get(name).cloned()
                .unwrap_or_else(|| panic!("scope references unknown outer name `{}`", name));
            let i = s.imports.len();
            s.imports.push(st::Import { name: name.to_string(), from: st::Source::Parent(from) });
            s.env.insert(name.to_string(), st::Ref::Import(i));
        }
    }

    // Bucket statements.
    let mut lets: HashMap<String, &Expr> = HashMap::new();
    let mut var_list: Vec<(&str, &Expr)> = Vec::new();
    let mut child_bodies: HashMap<String, &[Stmt]> = HashMap::new();
    let mut export_stmts: Vec<(&str, &Expr)> = Vec::new();
    let mut order: Vec<(ItemKind, String)> = Vec::new();
    for stmt in body {
        match stmt {
            Stmt::Let(n, e)  => { lets.insert(n.clone(), e); order.push((ItemKind::Let, n.clone())); },
            Stmt::Var(n, e)  => { var_list.push((n.as_str(), e)); },
            Stmt::Scope(n, b)=> { child_bodies.insert(n.clone(), b.as_slice()); order.push((ItemKind::Scope, n.clone())); },
            Stmt::Export(n, e) => { export_stmts.push((n.as_str(), e)); },
        }
    }

    // Pre-declare feedback variables so anything in the scope can refer to them.
    let mut vars: Vec<st::Var> = Vec::new();
    let mut var_id: HashMap<&str, usize> = HashMap::new();
    for (name, _) in &var_list {
        let idx = vars.len();
        vars.push(st::Var { name: (*name).to_string() });
        s.env.insert((*name).to_string(), st::Ref::Var(idx));
        var_id.insert(*name, idx);
    }

    // Topologically lower lets and child scopes (deps restricted to local
    // let/scope names; vars are already bound).
    let defined: BTreeSet<&str> = lets.keys().map(|k| k.as_str())
        .chain(child_bodies.keys().map(|k| k.as_str())).collect();
    let mut deps: HashMap<String, BTreeSet<String>> = HashMap::new();
    for (n, e) in &lets { deps.insert(n.clone(), expr_deps(e, &defined, n)); }
    for (n, b) in &child_bodies { deps.insert(n.clone(), scope_body_deps(b, &defined, n)); }
    drop(defined);

    let mut pending = order;
    while !pending.is_empty() {
        let pos = pending.iter().position(|(_, n)| deps[n].is_empty())
            .unwrap_or_else(|| panic!("Cyclic let/scope dependency; use `var` for recursion: {:?}",
                pending.iter().map(|(_, n)| n.clone()).collect::<Vec<_>>()));
        let (kind, name) = pending.remove(pos);
        deps.remove(&name);
        for d in deps.values_mut() { d.remove(&name); }
        match kind {
            ItemKind::Let => {
                let e = lets.remove(&name).unwrap();
                let r = s.lower_expr(e);
                s.env.insert(name, r);
            },
            ItemKind::Scope => {
                let b = child_bodies.remove(&name).unwrap();
                let wanted = qualified_fields(body, &name);
                let child = lower_scope_tree(&name, b, &s.env, &wanted, false);
                let cidx = s.items.len();
                s.items.push(st::Item::Sub(child));
                s.child_idx.insert(name.clone(), cidx);
                s.child_exports.insert(name, wanted);
            },
        }
    }

    // Lower var bodies and emit binds.
    let mut binds: Vec<st::Bind> = Vec::new();
    for (name, e) in &var_list {
        let r = s.lower_expr(e);
        binds.push(st::Bind { var: var_id[*name], value: r });
    }

    // Exports: the root takes its `export` statements; a child surrenders the
    // fields its parent asked for.
    let mut exports: Vec<st::Export> = Vec::new();
    if is_root {
        for (name, e) in &export_stmts {
            let r = s.lower_expr(e);
            exports.push(st::Export { name: (*name).to_string(), value: r });
        }
    } else {
        for field in exports_wanted {
            let r = s.env.get(field).cloned()
                .unwrap_or_else(|| panic!("scope export `{}` is not a local name", field));
            exports.push(st::Export { name: field.clone(), value: r });
        }
    }

    st::Scope { name: scope_name.to_string(), imports: s.imports, vars, items: s.items, binds, exports }
}

/// The fields of child scope `scope` that this body references via `scope::field`.
fn qualified_fields(body: &[Stmt], scope: &str) -> Vec<String> {
    let mut out = Vec::new();
    for stmt in body {
        match stmt {
            Stmt::Let(_, e) | Stmt::Var(_, e) | Stmt::Export(_, e) => collect_qualified(e, scope, &mut out),
            Stmt::Scope(_, _) => {} // a child's `scope::field`s reference *its* children, not ours
        }
    }
    out
}

fn collect_qualified(e: &Expr, scope: &str, out: &mut Vec<String>) {
    match e {
        Expr::Qualified(s, f) => if s == scope && !out.contains(f) { out.push(f.clone()); },
        Expr::Map(e, _) | Expr::Filter(e, _) | Expr::Negate(e) | Expr::EnterAt(e, _)
        | Expr::LiftIter(e) | Expr::Reduce(e, _) | Expr::Inspect(e, _) | Expr::Arrange(e) => collect_qualified(e, scope, out),
        Expr::Join(l, r, _) => { collect_qualified(l, scope, out); collect_qualified(r, scope, out); },
        Expr::Concat(es) => for e in es { collect_qualified(e, scope, out); },
        Expr::Input(_) | Expr::Import(_) | Expr::Name(_) => {}
    }
}

#[cfg(test)]
mod tree_tests {
    use super::*;
    use crate::scope_ir::{Ref, Source};

    fn parse(src: &str) -> Vec<Stmt> { crate::parse::pipe::parse(src) }
    fn subs(s: &st::Scope) -> Vec<&st::Scope> {
        s.items.iter().filter_map(|i| match i { st::Item::Sub(c) => Some(c), _ => None }).collect()
    }
    fn reads_a_child_export(s: &st::Scope) -> bool {
        s.items.iter().any(|i| matches!(i, st::Item::Op(st::Node::Linear { input: Ref::ChildExport(..), .. })))
    }

    #[test]
    fn lowers_reach_to_tree() {
        let src = "\
            let edges = input 0 | key($0[0] ; $0[1]);\n\
            let roots = input 1 | key($0[0] ;);\n\
            reach: {\n\
                let proposals = reach | join(edges, ($2 ;));\n\
                var reach = roots + proposals | distinct;\n\
            }\n\
            export \"result\" = reach::reach | key(;) | arrange | inspect(total);";
        let prog = lower_tree(parse(src));

        // root: two Input imports, one child (reach), one export.
        assert_eq!(prog.root.imports.len(), 2);
        assert!(prog.root.imports.iter().all(|i| matches!(i.from, Source::Input(_))));
        assert_eq!(subs(&prog.root).len(), 1);
        assert_eq!(prog.root.exports.len(), 1);
        assert_eq!(prog.root.exports[0].name, "result");

        // the reach scope: one feedback var, two Parent imports, one bind,
        // and one export named "reach".
        let reach = subs(&prog.root)[0];
        assert_eq!(reach.vars.len(), 1);
        assert_eq!(reach.imports.len(), 2);
        assert!(reach.imports.iter().all(|i| matches!(i.from, Source::Parent(_))));
        assert_eq!(reach.binds.len(), 1);
        assert_eq!(reach.exports.len(), 1);
        assert_eq!(reach.exports[0].name, "reach");

        // the root export chain consumes the child's export.
        assert!(reads_a_child_export(&prog.root), "root should read reach::reach as a child export");
    }

    #[test]
    fn lowers_two_level_nesting_with_transitive_import() {
        // root -> outer -> inner, with `i` (a root input) used only deep in inner.
        let src = "\
            let i = input 0 | key($0[0] ; $0[1]);\n\
            outer: {\n\
                inner: {\n\
                    var x = i | distinct;\n\
                }\n\
                let y = inner::x;\n\
            }\n\
            export \"result\" = outer::y;";
        let prog = lower_tree(parse(src));
        assert_eq!(subs(&prog.root).len(), 1);              // outer
        let outer = subs(&prog.root)[0];
        assert_eq!(subs(outer).len(), 1);                   // inner
        let inner = subs(outer)[0];
        assert_eq!(inner.vars.len(), 1);                    // x
        // `i` is threaded as an import through both levels.
        assert!(outer.imports.iter().any(|im| im.name == "i"));
        assert!(inner.imports.iter().any(|im| im.name == "i"));
    }

    #[test]
    fn lowers_scc_depth_two() {
        let src = r#"
            let edges = input 0 | key($0[0] ; $0[1]);
            let trans = edges | key($1 ; $0);
            outer: {
                let scc = edges + trim;
                fwd: {
                    let nodes = edges | key($1 ; $1) | enter_at($1[0]);
                    let labels = proposals + nodes | min;
                    var proposals = labels | join(scc, ($2 ; $1));
                }
                let trim_fwd = edges
                    | join(fwd::labels, ($1 ; $0, $2))
                    | join(fwd::labels, ($0 ; $1, $2))
                    | filter($1[1] == $1[2])
                    | key($0 ; $1[0]);
                bwd: {
                    let nodes = trans | key($1 ; $1) | enter_at($1[0]);
                    let labels = proposals + nodes | min;
                    var proposals = labels | join(trim_fwd, ($2 ; $1));
                }
                let trim_bwd = trans
                    | join(bwd::labels, ($1 ; $0, $2))
                    | join(bwd::labels, ($0 ; $1, $2))
                    | filter($1[1] == $1[2])
                    | key($0 ; $1[0]);
                var trim = trim_bwd - edges;
            }
            export "result" = outer::scc | map(;) | arrange | inspect(total);
        "#;
        let prog = lower_tree(parse(src));
        assert_eq!(subs(&prog.root).len(), 1); // outer
        let outer = subs(&prog.root)[0];
        assert_eq!(subs(outer).len(), 2); // fwd, bwd (depth 2)
        assert_eq!(outer.vars.len(), 1); // trim
        assert!(outer.exports.iter().any(|e| e.name == "scc"));
        for child in subs(outer) {
            assert_eq!(child.vars.len(), 1); // proposals
            assert!(child.exports.iter().any(|e| e.name == "labels"));
        }
        assert!(reads_a_child_export(&prog.root)); // root reads outer::scc
    }
}

#[cfg(test)]
mod optimize_tests {
    use super::*;
    use crate::scope_ir::{Item, Node, Ref};

    fn parse(src: &str) -> Vec<Stmt> { crate::parse::pipe::parse(src) }
    fn ops(s: &st::Scope) -> Vec<&Node> {
        s.items.iter().filter_map(|i| match i { Item::Op(n) => Some(n), _ => None }).collect()
    }

    #[test]
    fn fuses_linear_chains() {
        let src = "let a = input 0 | key($0[0] ; $0[1]) | filter($0[0] != $1[0]) | negate;\nexport \"result\" = a;";
        let mut p = lower_tree(parse(src));
        assert_eq!(p.op_count(), 3); // three single-op Linears
        p.optimize();
        assert_eq!(p.op_count(), 1, "chain should fuse to one Linear");
        let Node::Linear { ops, .. } = ops(&p.root)[0] else { panic!("expected a Linear") };
        assert_eq!(ops.len(), 3, "fused Linear carries all three ops in order");
        assert!(matches!(ops[0], LinearOp::Project(_)));
        assert!(matches!(ops[2], LinearOp::Negate));
    }

    #[test]
    fn shares_arrangements_across_joins() {
        // `a` feeds two joins; its two implicit Arranges must dedup to one.
        let src = "\
            let a = input 0 | key($0[0] ; $0[1]);\n\
            let b = input 0 | key($0[1] ; $0[0]);\n\
            let j1 = a | join(b, ($1 ; $2));\n\
            let j2 = a | join(b, ($2 ; $1));\n\
            export \"result\" = j1 + j2;";
        let mut p = lower_tree(parse(src));
        p.optimize();
        let arranges = ops(&p.root).iter().filter(|n| matches!(n, Node::Arrange(_))).count();
        assert_eq!(arranges, 2, "one shared arrangement per distinct input (a, b)");
    }

    #[test]
    fn collapses_arrange_of_reduce() {
        // distinct produces an arrangement; joining on it must not re-arrange.
        let src = "\
            let a = input 0 | key($0[0] ; $0[1]) | distinct;\n\
            let b = input 0 | key($0[0] ; $0[1]);\n\
            export \"result\" = a | join(b, ($1 ;));";
        let mut p = lower_tree(parse(src));
        p.optimize();
        // No Arrange may target a Reduce item.
        for n in ops(&p.root) {
            if let Node::Arrange(Ref::Local(j)) = n {
                assert!(!matches!(&p.root.items[*j], Item::Op(Node::Reduce { .. })),
                    "Arrange of a Reduce should have collapsed");
            }
        }
    }

    #[test]
    fn optimizes_within_nested_scopes() {
        // The fusible chain sits inside a scope; optimize must recurse.
        let src = "\
            let e = input 0 | key($0[0] ; $0[1]);\n\
            s: {\n\
                var x = e | key($0 ; $1) | filter($0[0] != $1[0]) | distinct;\n\
            }\n\
            export \"result\" = s::x;";
        let mut p = lower_tree(parse(src));
        p.optimize();
        let Item::Sub(child) = p.root.items.iter().find(|i| matches!(i, Item::Sub(_))).unwrap() else { unreachable!() };
        let linears: Vec<usize> = ops(child).iter().filter_map(|n| match n { Node::Linear { ops, .. } => Some(ops.len()), _ => None }).collect();
        assert_eq!(linears, vec![2], "the key|filter chain fuses inside the child scope");
    }
}
