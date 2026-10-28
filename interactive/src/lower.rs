//! Lowering from AST to the scope-tree IR.
//!
//! Statement order within a scope does not affect semantics: each scope
//! buckets its statements (rejecting duplicate names), pre-declares its
//! `set` variables so anything in the scope can refer to them, topologically lowers
//! `let`s and child scopes by dependency (a cycle among `let`s is an error;
//! use `set` for recursion), then lowers exports and the `set` bodies
//! (each emitting a `Bind`).

use std::collections::{BTreeSet, HashMap, HashSet};

use crate::parse::*;
use crate::ir::LinearOp;


#[derive(Clone, Copy)]
enum ItemKind { Let, Scope }

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
enum RefName {
    Plain(String),
    Path(PathKey),
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct PathKey { anchor: Vec<String>, target: Vec<String>, field: String }

impl PathKey {
    fn display(&self) -> String {
        if self.target.is_empty() { format!("::{}", self.field) }
        else { format!("::{}::{}", self.target.join("::"), self.field) }
    }
}

#[derive(Default)]
struct ScopeNames { bindings: BTreeSet<String>, children: BTreeSet<String> }

#[derive(Default)]
struct PathPlan {
    imports: HashMap<Vec<String>, BTreeSet<PathKey>>,
    exports: HashMap<Vec<String>, BTreeSet<PathKey>>,
    deps: HashMap<Vec<String>, HashMap<String, BTreeSet<String>>>,
    path_edges: HashMap<Vec<String>, HashMap<(String, String), BTreeSet<String>>>,
}

fn scope_label(path: &[String]) -> &str { path.last().map(String::as_str).unwrap_or("root") }

fn scope_address(path: &[String]) -> String {
    if path.is_empty() { "::".to_string() }
    else { format!("::{}", path.join("::")) }
}

fn export_lookup(key: &PathKey, address: &[String]) -> RefName {
    if key.target == address { RefName::Plain(key.field.clone()) }
    else { RefName::Path(key.clone()) }
}

fn export_slots(wanted: &[PathKey], address: &[String]) -> Vec<usize> {
    let mut slots = HashMap::new();
    wanted.iter().map(|key| {
        let next = slots.len();
        *slots.entry(export_lookup(key, address)).or_insert(next)
    }).collect()
}

fn collect_scopes(body: &[Stmt], path: &[String], visible: &mut HashMap<String, Vec<String>>,
                  scopes: &mut HashMap<Vec<String>, ScopeNames>) {
    let mut names = ScopeNames::default();
    let mut all = BTreeSet::new();
    for stmt in body {
        let name = match stmt {
            Stmt::Let(n, _) | Stmt::Set(n, _) | Stmt::Scope(n, _) => n,
            Stmt::Var(..) | Stmt::Export(..) => continue,
        };
        // Duplicate and conflicting definitions are reported while lowering.
        all.insert(name.clone());
        match stmt {
            Stmt::Scope(..) => { names.children.insert(name.clone()); }
            _ => { names.bindings.insert(name.clone()); }
        }
    }
    for child in &names.children {
        if let Some(other) = visible.get(child) {
            panic!("scope `{child}` in `{}` ({}) shadows visible scope `{}` in `{}` ({})",
                scope_label(path), scope_address(path), child, scope_label(other), scope_address(other));
        }
        let mut address = path.to_vec();
        address.push(child.clone());
        visible.insert(child.clone(), address);
    }
    scopes.insert(path.to_vec(), names);
    for stmt in body {
        if let Stmt::Scope(name, nested) = stmt {
            let mut address = path.to_vec();
            address.push(name.clone());
            collect_scopes(nested, &address, visible, scopes);
        }
    }
    // Siblings share the same visible names. Remove only this scope's additions
    // when returning to its parent, rather than cloning the whole map per child.
    for child in &scopes[path].children { visible.remove(child); }
}

fn resolve_path(parts: &[String], absolute: bool, here: &[String],
                scopes: &HashMap<Vec<String>, ScopeNames>) -> (Vec<String>, PathKey, String) {
    let written = format!("{}{}", if absolute { "::" } else { "" }, parts.join("::"));
    let (anchor, skip) = if absolute { (Vec::new(), 0) } else {
        let first = &parts[0];
        let mut found = None;
        for depth in (1..=here.len()).rev() {
            let candidate = here[..depth].to_vec();
            if scope_label(&candidate) == first {
                found = Some((candidate, 1));
                break;
            }
        }
        if let Some(found) = found { found } else {
            let mut found = None;
            for depth in (0..=here.len()).rev() {
                let candidate = here[..depth].to_vec();
                if scopes[&candidate].children.contains(first) {
                    found = Some((candidate, 0));
                    break;
                }
            }
            found.unwrap_or_else(|| {
                if (0..=here.len()).any(|depth| scopes[&here[..depth]].bindings.contains(first)) {
                    panic!("path `{written}` needs a scope at `{first}`, but `{first}` is a binding visible from `{}` ({})",
                        scope_label(here), scope_address(here));
                }
                panic!("path `{written}` has unknown anchor `{first}` from scope `{}` ({})",
                    scope_label(here), scope_address(here));
            })
        }
    };
    assert!(parts.len() > skip, "path `{written}` in `{}` needs a final binding", scope_label(here));
    let mut target = anchor.clone();
    for component in &parts[skip..parts.len()-1] {
        let names = &scopes[&target];
        if !names.children.contains(component) {
            if names.bindings.contains(component) {
                panic!("path `{written}` needs a scope at `{component}` in `{}`, but it is a binding (at {})",
                    scope_label(&target), scope_address(&target));
            }
            panic!("path `{written}` has unknown child scope `{component}` in `{}` ({})",
                scope_label(&target), scope_address(&target));
        }
        target.push(component.clone());
        if here.starts_with(&target) {
            let suffix = parts[skip + target.len() - anchor.len()..].join("::");
            panic!("path `{written}` descends into enclosing scope `{}` from `{}`; inside `{}`, write `{suffix}` ({} from {})",
                scope_label(&target), scope_label(&anchor), scope_label(&target), scope_address(&target), scope_address(&anchor));
        }
    }
    let field = parts.last().unwrap();
    let names = &scopes[&target];
    if names.children.contains(field) {
        panic!("path `{written}` needs a binding at `{field}` in `{}`, but it is a scope ({})",
            scope_label(&target), scope_address(&target));
    }
    if !names.bindings.contains(field) {
        panic!("path `{written}`: scope `{}` does not define field `{field}` ({})",
            scope_label(&target), scope_address(&target));
    }
    (anchor.clone(), PathKey { anchor, target, field: field.clone() }, written)
}

fn resolve_expr(expr: &mut Expr, here: &[String], item: Option<&str>,
                scopes: &HashMap<Vec<String>, ScopeNames>, plan: &mut PathPlan) {
    match expr {
        Expr::Path(parts, absolute) => {
            let (anchor, key, written) = resolve_path(parts, *absolute, here, scopes);
            for depth in anchor.len()+1..=key.target.len() {
                plan.exports.entry(key.target[..depth].to_vec()).or_default().insert(key.clone());
            }
            for depth in anchor.len()+1..=here.len() {
                plan.imports.entry(here[..depth].to_vec()).or_default().insert(key.clone());
            }
            let needed = key.target.get(anchor.len()).cloned().unwrap_or_else(|| key.field.clone());
            let dependent = if here == anchor { item.map(str::to_string) }
                else { here.get(anchor.len()).cloned() };
            if let Some(dependent) = dependent {
                if dependent != needed {
                    plan.deps.entry(anchor.clone()).or_default().entry(dependent.clone()).or_default().insert(needed.clone());
                    plan.path_edges.entry(anchor).or_default().entry((dependent, needed)).or_default().insert(written);
                }
            }
            *expr = Expr::ResolvedPath { anchor: key.anchor, target: key.target, field: key.field };
        }
        Expr::TypedSource(e, _, _) | Expr::Map(e, _) | Expr::Reduce(e, _) | Expr::Filter(e, _)
        | Expr::Negate(e) | Expr::EnterAt(e, _) | Expr::LiftIter(e) | Expr::FlatMap(e, _)
        | Expr::Lift(e) | Expr::JoinFunction(e, _) | Expr::Inspect(e, _) | Expr::Arrange(e) =>
            resolve_expr(e, here, item, scopes, plan),
        Expr::Join(l, r, _) | Expr::HalfJoin(l, r, _, _) => {
            resolve_expr(l, here, item, scopes, plan);
            resolve_expr(r, here, item, scopes, plan);
        }
        Expr::Concat(parts) => for part in parts { resolve_expr(part, here, item, scopes, plan); },
        Expr::Input(_) | Expr::Import(_) | Expr::Name(_) | Expr::ResolvedPath { .. } => {},
    }
}

fn resolve_body(body: &mut [Stmt], here: &[String], scopes: &HashMap<Vec<String>, ScopeNames>, plan: &mut PathPlan) {
    for stmt in body {
        match stmt {
            Stmt::Let(name, expr) => resolve_expr(expr, here, Some(name), scopes, plan),
            Stmt::Set(_, expr) | Stmt::Export(_, _, expr) => resolve_expr(expr, here, None, scopes, plan),
            Stmt::Var(..) => {},
            Stmt::Scope(name, nested) => {
                let mut address = here.to_vec();
                address.push(name.clone());
                resolve_body(nested, &address, scopes, plan);
            }
        }
    }
}

/// Free names of `expr` that are defined at this scope level, excluding the
/// item's own name (a let referencing its own name is just an unresolved
/// reference for the current pass — not a self-dep).
fn expr_deps(expr: &Expr, defined: &BTreeSet<&str>, self_name: &str) -> BTreeSet<String> {
    let mut free = BTreeSet::new();
    expr_free_names(expr, &mut free);
    dependency_names(free, defined, self_name)
}

/// Free names of a scope body (names referenced inside that aren't bound
/// inside), restricted to names defined at the enclosing level.
fn scope_body_deps(body: &[Stmt], defined: &BTreeSet<&str>, self_name: &str) -> BTreeSet<String> {
    let mut free = BTreeSet::new();
    collect_body_free_names(body, &mut free);
    dependency_names(free, defined, self_name)
}

fn dependency_names(free: BTreeSet<RefName>, defined: &BTreeSet<&str>, self_name: &str) -> BTreeSet<String> {
    free.into_iter().filter_map(|name| match name {
        RefName::Plain(n) if n != self_name && defined.contains(n.as_str()) => Some(n),
        _ => None,
    }).collect()
}

/// Names this expression refers to that the surrounding scope must resolve.
fn expr_free_names(expr: &Expr, out: &mut BTreeSet<RefName>) {
    match expr {
        Expr::Input(_) | Expr::Import(_) => {},
        Expr::Name(n) => { out.insert(RefName::Plain(n.clone())); },
        Expr::ResolvedPath { .. } => {},
        Expr::Path(..) => unreachable!("paths must be resolved before lowering"),
        Expr::TypedSource(e, _, _) | Expr::Map(e, _) | Expr::Reduce(e, _) | Expr::Filter(e, _)
            | Expr::Negate(e) | Expr::EnterAt(e, _) | Expr::LiftIter(e)
            | Expr::FlatMap(e, _) | Expr::Lift(e) | Expr::JoinFunction(e, _)
            | Expr::Inspect(e, _) | Expr::Arrange(e) => expr_free_names(e, out),
        Expr::Join(l, r, _) | Expr::HalfJoin(l, r, _, _) => { expr_free_names(l, out); expr_free_names(r, out); },
        Expr::Concat(es) => { for e in es { expr_free_names(e, out); } },
    }
}

/// Recursively collect names referenced in `body`'s expressions that aren't
/// bound somewhere within `body` itself. The recursion descends through
/// nested scopes, masking out their local bindings as it goes.
fn collect_body_free_names(body: &[Stmt], out: &mut BTreeSet<RefName>) {
    let mut local = BTreeSet::new();
    for stmt in body {
        match stmt {
            Stmt::Let(n, _) | Stmt::Var(n, _) | Stmt::Set(n, _) | Stmt::Scope(n, _) => { local.insert(n.as_str()); },
            Stmt::Export(_, _, _) => {},
        }
    }
    let mut inner = BTreeSet::new();
    collect_body_references(body, &mut inner);
    for name in inner {
        let bound = match &name {
            RefName::Plain(n) => local.contains(n.as_str()),
            RefName::Path(_) => false,
        };
        if !bound { out.insert(name); }
    }
}

/// References from this body, after each descendant masks its own bindings.
fn collect_body_references(body: &[Stmt], out: &mut BTreeSet<RefName>) {
    for stmt in body {
        match stmt {
            Stmt::Let(_, e) | Stmt::Set(_, e) | Stmt::Export(_, _, e) => expr_free_names(e, out),
            Stmt::Var(_, _) => {},
            Stmt::Scope(_, b) => collect_body_free_names(b, out),
        }
    }
}

// ===== Scope-tree lowering (AST -> scope_ir) =====
//
// Produces the tree IR (see `scope_ir`): each `{ .. }` becomes an
// owned child scope, cross-scope flow becomes explicit import/export edges,
// and feedback vars are first-class. `input`/`import` external sources are
// accepted at the root scope only. Shapes are not stored on the tree (beyond
// an import's optional contract); they are derived from the data where needed.

use crate::scope_ir as st;

pub fn lower_tree(mut stmts: Vec<Stmt>) -> st::Program {
    let mut scopes = HashMap::new();
    // The program root has no source-level name; `::` is its only explicit anchor.
    let mut visible = HashMap::new();
    collect_scopes(&stmts, &[], &mut visible, &mut scopes);
    let mut plan = PathPlan::default();
    resolve_body(&mut stmts, &[], &scopes, &mut plan);
    let empty = HashMap::new();
    let mut distinct = HashMap::new();
    let mut program = st::Program { root: lower_scope_tree(&[], &stmts, &empty, &[], &plan, true, &mut distinct) };
    let (shapes, _) = crate::shapes::infer(&program, &|_| None);
    crate::reducers::expand_distinct(&mut program.root, &shapes, &distinct, &mut Vec::new());
    program
}

/// Expr-lowering state for one scope. Resolves names to scope-local `Ref`s and
/// accumulates this scope's nodes and (input/trace) imports.
struct ScopeLower {
    distinct: Vec<usize>,
    is_root: bool,
    items: Vec<st::Item>,
    /// Each item's statement name (see `st::Scope::labels`).
    labels: Vec<String>,
    /// The statement being lowered, which names the items it pushes.
    statement: String,
    imports: Vec<st::Import>,
    input_import: HashMap<usize, usize>, // input n  -> import idx (root, deduped)
    trace_import: HashMap<String, usize>, // trace name -> import idx (root, deduped)
    env: HashMap<RefName, st::Ref>,        // local names, qualified child exports, and imports
}

impl ScopeLower {
    fn push(&mut self, n: st::Node) -> st::Ref {
        let i = self.items.len();
        self.items.push(st::Item::Op(n));
        self.labels.push(self.statement.clone());
        st::Ref::Local(i)
    }
    fn input_ref(&mut self, n: usize) -> st::Ref {
        assert!(self.is_root, "`input {}` used outside the root scope (not yet supported)", n);
        if let Some(&i) = self.input_import.get(&n) { return st::Ref::Import(i); }
        let i = self.imports.len();
        self.imports.push(st::Import { name: format!("input{}", n), from: st::Source::Input(n), shape: None });
        self.input_import.insert(n, i);
        st::Ref::Import(i)
    }
    fn trace_ref(&mut self, name: &str) -> st::Ref {
        assert!(self.is_root, "`import {:?}` used outside the root scope (not yet supported)", name);
        if let Some(&i) = self.trace_import.get(name) { return st::Ref::Import(i); }
        let i = self.imports.len();
        self.imports.push(st::Import { name: name.to_string(), from: st::Source::Trace(name.to_string()), shape: None });
        self.trace_import.insert(name.to_string(), i);
        st::Ref::Import(i)
    }
    fn lower_expr(&mut self, e: &Expr) -> st::Ref {
        match e {
            Expr::Input(n) => self.input_ref(*n),
            Expr::Import(name) => self.trace_ref(name),
            Expr::TypedSource(source, key, val) => {
                let r = self.lower_expr(source);
                let st::Ref::Import(i) = r else { panic!("a shape ascription requires an external source") };
                assert!(self.is_root, "external shape ascriptions belong at the root");
                let shape = (key.clone(), val.clone());
                if let Some(previous) = &self.imports[i].shape {
                    assert_eq!(previous, &shape, "conflicting source shape ascriptions");
                }
                self.imports[i].shape = Some(shape);
                r
            }
            Expr::Name(name) => self.env.get(&RefName::Plain(name.clone())).cloned()
                .unwrap_or_else(|| panic!("unresolved name `{}`", name)),
            Expr::ResolvedPath { anchor, target, field } => self.env.get(&RefName::Path(PathKey { anchor: anchor.clone(), target: target.clone(), field: field.clone() })).cloned()
                .unwrap_or_else(|| panic!("unresolved path `::{}::{}`", target.join("::"), field)),
            Expr::Path(..) => unreachable!("paths must be resolved before lowering"),
            Expr::Map(e, p)     => { let r = self.lower_expr(e); self.push(st::Node::Linear { input: r, ops: vec![LinearOp::Project(p.clone())] }) },
            Expr::Filter(e, c)  => { let r = self.lower_expr(e); self.push(st::Node::Linear { input: r, ops: vec![LinearOp::Filter(c.clone())] }) },
            Expr::Negate(e)     => { let r = self.lower_expr(e); self.push(st::Node::Linear { input: r, ops: vec![LinearOp::Negate] }) },
            Expr::EnterAt(e, f) => { let r = self.lower_expr(e); self.push(st::Node::Linear { input: r, ops: vec![LinearOp::EnterAt(f.clone())] }) },
            Expr::FlatMap(e, t) => { let r = self.lower_expr(e); self.push(st::Node::Linear { input: r, ops: vec![LinearOp::FlatMap(t.clone())] }) },
            Expr::LiftIter(e)   => { let r = self.lower_expr(e); self.push(st::Node::Linear { input: r, ops: vec![LinearOp::LiftIter] }) },
            Expr::Lift(e)       => { let r = self.lower_expr(e); self.push(st::Node::Linear { input: r, ops: vec![LinearOp::Lift] }) },
            Expr::JoinFunction(e, t) => { let r = self.lower_expr(e); self.push(st::Node::Linear { input: r, ops: vec![LinearOp::JoinFunction(t.clone())] }) },
            Expr::Arrange(e)    => { let r = self.lower_expr(e); self.push(st::Node::Arrange(r)) },
            // Join/Reduce consume arrangements; arrange their inputs explicitly
            // so identical arrangements are visible to `optimize`'s within-scope
            // dedup and shared at render.
            Expr::Join(l, r, p) => {
                let lr = self.lower_expr(l); let la = self.push(st::Node::Arrange(lr));
                let rr = self.lower_expr(r); let ra = self.push(st::Node::Arrange(rr));
                self.push(st::Node::Join { left: la, right: ra, projection: p.clone() })
            },
            // A half join reads its stream as updates and only its second input arranged.
            Expr::HalfJoin(s, r, strict, p) => {
                let stream = self.lower_expr(s);
                let rr = self.lower_expr(r); let ra = self.push(st::Node::Arrange(rr));
                self.push(st::Node::HalfJoin { stream, arrangement: ra, strict: *strict, projection: p.clone() })
            },
            Expr::Reduce(e, red)=> {
                let r = self.lower_expr(e);
                let a = if matches!(red, Reducer::Distinct) {
                    // Once shapes are known, lower distinct by key or by whole record.
                    self.distinct.push(self.items.len());
                    r
                } else { self.push(st::Node::Arrange(r)) };
                self.push(st::Node::Reduce { input: a, reducer: crate::ir::Reducer::Expr(red.lambda()) })
            },
            Expr::Inspect(e, l) => { let r = self.lower_expr(e); self.push(st::Node::Inspect { input: r, label: l.clone() }) },
            Expr::Concat(es)    => { let rs: Vec<st::Ref> = es.iter().map(|e| self.lower_expr(e)).collect(); self.push(st::Node::Concat(rs)) },
        }
    }
}

fn lower_scope_tree(
    address: &[String],
    body: &[Stmt],
    parent_env: &HashMap<RefName, st::Ref>,
    exports_wanted: &[PathKey],
    plan: &PathPlan,
    is_root: bool,
    distinct: &mut HashMap<Vec<String>, Vec<usize>>,
) -> st::Scope {
    let scope_name = scope_label(address);
    let mut s = ScopeLower {
        distinct: Vec::new(),
        is_root,
        items: Vec::new(),
        labels: Vec::new(),
        statement: String::new(),
        imports: Vec::new(),
        input_import: HashMap::new(),
        trace_import: HashMap::new(),
        env: HashMap::new(),
    };

    // Pre-create imports from this scope's transitive free names (names used
    // here or in a descendant but not defined here). Doing it up front handles
    // cross-level references: an outer name a grandchild needs is imported into
    // every scope on the path.
    if !is_root {
        let mut free = BTreeSet::new();
        collect_body_free_names(body, &mut free);
        free.extend(plan.imports.get(address).into_iter().flatten().cloned().map(RefName::Path));
        for name in free {
            let from = parent_env.get(&name).cloned().unwrap_or_else(|| match &name {
                RefName::Plain(n) => panic!("scope references unknown outer name `{}`", n),
                RefName::Path(key) => panic!("path `{}` is unavailable in scope `{scope_name}`", key.display()),
            });
            let i = s.imports.len();
            s.imports.push(st::Import { name: match &name {
                RefName::Plain(n) => n.clone(),
                RefName::Path(key) => key.display(),
            }, from: st::Source::Parent(from), shape: None });
            s.env.insert(name, st::Ref::Import(i));
        }
    }

    // Bucket statements.
    let mut lets: HashMap<String, &Expr> = HashMap::new();
    let mut sets: Vec<(&str, &Expr)> = Vec::new();
    let mut set_names = HashSet::new();
    let mut declarations: HashMap<&str, &Option<st::RowShape>> = HashMap::new();
    let mut child_bodies: HashMap<String, &[Stmt]> = HashMap::new();
    let mut export_stmts: Vec<(&str, &Option<st::RowShape>, &Expr)> = Vec::new();
    let mut order: Vec<(ItemKind, String)> = Vec::new();
    for stmt in body {
        match stmt {
            Stmt::Let(n, e)  => { assert!(lets.insert(n.clone(), e).is_none(), "let `{n}` defined twice"); order.push((ItemKind::Let, n.clone())); },
            Stmt::Var(n, s) => { assert!(declarations.insert(n, s).is_none(), "var `{n}` declared twice"); },
            Stmt::Set(n, e) => {
                assert!(!is_root, "set `{n}` is only valid in a named scope");
                assert!(set_names.insert(n.as_str()), "set `{n}` defined twice");
                sets.push((n, e));
            },
            Stmt::Scope(n, b)=> { assert!(child_bodies.insert(n.clone(), b.as_slice()).is_none(), "scope `{n}` defined twice"); order.push((ItemKind::Scope, n.clone())); },
            Stmt::Export(n, s, e) => { export_stmts.push((n.as_str(), s, e)); },
        }
    }

    for name in declarations.keys() {
        assert!(set_names.contains(name), "var `{name}` has no set in this scope");
    }
    for (name, _) in &sets {
        assert!(!lets.contains_key(*name) && !child_bodies.contains_key(*name),
            "set `{name}` conflicts with a let or child scope in this scope");
    }
    for name in lets.keys() {
        assert!(!child_bodies.contains_key(name), "`{name}` is both a let and child scope");
    }

    // Pre-declare feedback variables so anything in the scope can refer to them.
    let mut vars: Vec<st::Var> = Vec::new();
    let mut var_id: HashMap<&str, usize> = HashMap::new();
    for (name, _) in &sets {
        let idx = vars.len();
        let shape = declarations.get(name).and_then(|shape| (*shape).clone());
        vars.push(st::Var { name: (*name).to_string(), shape });
        s.env.insert(RefName::Plain((*name).to_string()), st::Ref::Var(idx));
        s.env.insert(RefName::Path(PathKey { anchor: address.to_vec(), target: address.to_vec(), field: (*name).to_string() }), st::Ref::Var(idx));
        var_id.insert(*name, idx);
    }

    // Topologically lower lets and child scopes (deps restricted to local
    // let/scope names; vars are already bound).
    let defined: BTreeSet<&str> = lets.keys().map(|k| k.as_str())
        .chain(child_bodies.keys().map(|k| k.as_str())).collect();
    let mut deps: HashMap<String, BTreeSet<String>> = HashMap::new();
    for (n, e) in &lets { deps.insert(n.clone(), expr_deps(e, &defined, n)); }
    for (n, b) in &child_bodies { deps.insert(n.clone(), scope_body_deps(b, &defined, n)); }
    if let Some(path_deps) = plan.deps.get(address) {
        for (name, needed) in path_deps {
            if let Some(deps) = deps.get_mut(name) {
                deps.extend(needed.iter().filter(|n| defined.contains(n.as_str())).cloned());
            }
        }
    }
    drop(defined);

    // Kahn's algorithm, taking the earliest ready item in `order` each step: the same order as
    // repeatedly scanning for the first item with no pending deps, in O((n + e) log n).
    let mut pending: Vec<Option<(ItemKind, String)>> = order.into_iter().map(Some).collect();
    let index: HashMap<String, usize> = pending.iter().enumerate()
        .map(|(i, it)| (it.as_ref().unwrap().1.clone(), i)).collect();
    let mut users: Vec<Vec<usize>> = vec![Vec::new(); pending.len()];
    let mut remaining: Vec<usize> = vec![0; pending.len()];
    for (i, it) in pending.iter().enumerate() {
        for d in &deps[&it.as_ref().unwrap().1] {
            remaining[i] += 1;
            if let Some(&j) = index.get(d) { users[j].push(i); }
        }
    }
    drop(deps);
    let mut names = Vec::new();
    let mut ready: BTreeSet<usize> = (0..pending.len()).filter(|&i| remaining[i] == 0).collect();
    while let Some(pos) = ready.pop_first() {
        for &u in &users[pos] {
            remaining[u] -= 1;
            if remaining[u] == 0 { ready.insert(u); }
        }
        let (kind, name) = pending[pos].take().unwrap();
        match kind {
            ItemKind::Let => {
                let e = lets.remove(&name).unwrap();
                s.statement = name.clone();
                let r = s.lower_expr(e);
                names.push((name.clone(), r.clone()));
                s.env.insert(RefName::Plain(name.clone()), r.clone());
                s.env.insert(RefName::Path(PathKey { anchor: address.to_vec(), target: address.to_vec(), field: name }), r);
            },
            ItemKind::Scope => {
                let b = child_bodies.remove(&name).unwrap();
                let mut child_address = address.to_vec();
                child_address.push(name.clone());
                let wanted: Vec<PathKey> = plan.exports.get(&child_address).into_iter().flatten().cloned().collect();
                let child = lower_scope_tree(&child_address, b, &s.env, &wanted, plan, false, distinct);
                let slots = export_slots(&wanted, &child_address);
                let cidx = s.items.len();
                s.items.push(st::Item::Sub(child));
                s.labels.push(name.clone());
                for (key, pos) in wanted.into_iter().zip(slots) {
                    s.env.insert(RefName::Path(key), st::Ref::ChildExport(cidx, pos));
                }
            },
        }
    }

    let rest: Vec<String> = pending.into_iter().flatten().map(|(_, n)| n).collect();
    if !rest.is_empty() {
        let names: BTreeSet<_> = rest.iter().cloned().collect();
        let paths: BTreeSet<_> = plan.path_edges.get(address).into_iter().flat_map(HashMap::iter)
            .filter(|((from, to), _)| names.contains(from) && names.contains(to))
            .flat_map(|(_, paths)| paths.iter().cloned()).collect();
        panic!("cycle between scopes or lets in `{scope_name}` ({}): {:?}; paths {:?}; use `set` for recursion",
            scope_address(address), rest, paths);
    }

    // Lower set bodies and emit binds.
    let mut binds: Vec<st::Bind> = Vec::new();
    for (name, e) in &sets {
        s.statement = (*name).to_string();
        let r = s.lower_expr(e);
        binds.push(st::Bind { var: var_id[*name], value: r });
    }

    // Exports: the root takes its `export` statements; a child surrenders the
    // fields its parent asked for.
    let mut exports: Vec<st::Export> = Vec::new();
    if is_root {
        for (name, shape, e) in &export_stmts {
            s.statement = format!("export {name}");
            let r = s.lower_expr(e);
            exports.push(st::Export { name: (*name).to_string(), value: r, shape: (*shape).clone() });
        }
    } else {
        let mut seen = BTreeSet::new();
        for key in exports_wanted {
            let lookup = export_lookup(key, address);
            if !seen.insert(lookup.clone()) { continue; }
            let r = s.env.get(&lookup).cloned()
                .unwrap_or_else(|| panic!("path `{}` is unavailable in scope `{scope_name}`", key.display()));
            let name = if key.target == address { key.field.clone() } else { key.display() };
            exports.push(st::Export { name, value: r, shape: None });
        }
    }

    distinct.insert(address.to_vec(), s.distinct);
    st::Scope { name: scope_name.to_string(), imports: s.imports, vars, items: s.items, labels: s.labels, names, binds, exports }
}

#[cfg(test)]
mod tree_tests {
    use super::*;
    use crate::scope_ir::{Ref, Source};

    fn parse(src: &str) -> Vec<Stmt> { crate::parse::pipe::parse(src) }
    fn subs(s: &st::Scope) -> Vec<&st::Scope> {
        s.items.iter().filter_map(|i| match i { st::Item::Sub(c) => Some(c), _ => None }).collect()
    }
    fn child<'a>(scope: &'a st::Scope, name: &str) -> &'a st::Scope {
        subs(scope).into_iter().find(|c| c.name == name).unwrap()
    }
    fn reads_a_child_export(s: &st::Scope) -> bool {
        s.items.iter().any(|i| matches!(i, st::Item::Op(st::Node::Linear { input: Ref::ChildExport(..), .. })))
    }

    #[test]
    fn set_bindings_ignore_statement_order_across_nested_scopes() {
        let src = "\
            let seed = input 0;\n\
            outer: {\n\
                set a = seed + from_inner;\n\
                inner: { set c = a + c; var c; }\n\
                let from_inner = inner::c;\n\
                var a : ((int), ());\n\
                set b = seed + from_inner;\n\
                var b;\n\
            }\n\
            export \"out\" = outer::a;";
        let program = lower_tree(parse(src));
        let outer = subs(&program.root)[0];
        let inner = subs(outer)[0];
        assert_eq!(outer.vars.iter().map(|v| v.name.as_str()).collect::<Vec<_>>(), ["a", "b"]);
        assert_eq!(outer.binds.len(), 2);
        assert!(outer.vars[0].shape.is_some());
        assert!(outer.vars[1].shape.is_none());
        assert_eq!(inner.vars.len(), 1);
        assert_eq!(inner.binds.len(), 1);
    }

    #[test]
    fn rejects_invalid_set_and_var_names() {
        for (src, expected) in [
            ("set x = x;", "only valid in a named scope"),
            ("s: { set x = x; set x = x; }", "set `x` defined twice"),
            ("s: { var x; }", "var `x` has no set"),
            ("s: { var x; let y = x; }", "var `x` has no set"),
            ("s: { var x; var x; set x = x; }", "var `x` declared twice"),
            ("s: { let x = input 0; set x = x; }", "set `x` conflicts"),
            ("s: { x: {} set x = x; }", "set `x` conflicts"),
            ("s: { var x; child: { set x = x; } }", "var `x` has no set"),
        ] {
            let err = std::panic::catch_unwind(|| lower_tree(parse(src))).unwrap_err();
            let message = err.downcast_ref::<String>().expect("string panic");
            assert!(message.contains(expected), "{src}: {message}");
        }
    }

    #[test]
    fn lowers_reach_to_tree() {
        let src = "\
            let edges = input 0 | map(((src, dst), ()) -> (src, dst));\n\
            let roots = input 1 | map(((n), ()) -> (n, ()));\n\
            reach: {\n\
                let proposals = reach | join(edges, (_, _, dst) -> (dst, ()));\n\
                set reach = roots + proposals | distinct;\n\
            }\n\
            export \"result\" = reach::reach | map(_ -> ((), ())) | arrange | inspect(total);";
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
            let i = input 0 | map(((a, b), ()) -> (a, b));\n\
            outer: {\n\
                inner: {\n\
                    set x = i | distinct;\n\
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
        let src = include_str!("../examples/programs/scc.ddp");
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

    #[test]
    fn grandchild_reads_uncle_export_through_parent() {
        let src = "let source = input 0; uncle: { let value = source; } \
            parent: { child: { let result = uncle::value; } let result = child::result; } \
            export \"out\" = parent::result;";
        let prog = lower_tree(parse(src));
        let uncle = child(&prog.root, "uncle");
        let parent = child(&prog.root, "parent");
        let grandchild = child(parent, "child");
        assert_eq!(uncle.exports[0].name, "value");
        let parent_import = parent.imports.iter().find(|i| i.name == "::uncle::value").unwrap();
        assert!(matches!(parent_import.from, Source::Parent(Ref::ChildExport(..))));
        let child_import = grandchild.imports.iter().find(|i| i.name == "::uncle::value").unwrap();
        assert!(matches!(child_import.from, Source::Parent(Ref::Import(_))));
    }

    #[test]
    #[should_panic(expected = "scope `X` in `outer` (::outer) shadows visible scope `X` in `X` (::X)")]
    fn local_child_shadows_outer_scope() {
        let src = "let source0 = input 0; let source1 = input 1; \
            X: { let value = source0; } \
            outer: { X: { let value = source1; } \
                inner: { let picked = X::value; } let result = inner::picked; } \
            export \"out\" = outer::result;";
        lower_tree(parse(src));
    }

    #[test]
    fn var_body_reads_sibling_var() {
        let src = "let source = input 0; first: { set result = second::value; } \
            second: { set value = source; } export \"out\" = first::result;";
        let prog = lower_tree(parse(src));
        let first = child(&prog.root, "first");
        assert!(matches!(first.imports.iter().find(|i| i.name == "::second::value").unwrap().from,
            Source::Parent(Ref::ChildExport(..))));
        assert!(matches!(first.binds[0].value, Ref::Import(_)));
    }

    #[test]
    fn local_binding_does_not_hide_outer_scope_qualifier() {
        let src = "let source = input 0; X: { let value = source; } \
            middle: { let X = source; let result = X::value; } \
            export \"out\" = middle::result;";
        let prog = lower_tree(parse(src));
        let middle = child(&prog.root, "middle");
        assert!(middle.imports.iter().any(|i| i.name == "::X::value"
            && matches!(i.from, Source::Parent(Ref::ChildExport(..)))));
    }

    #[test]
    #[should_panic(expected = "cycle between scopes or lets in `root` (::): [\"first\", \"second\"]")]
    fn sibling_scope_cycle_is_rejected() {
        lower_tree(parse("first: { let x = second::y; } second: { let y = first::x; }"));
    }

    #[test]
    fn scope_cycle_names_both_paths() {
        let error = std::panic::catch_unwind(|| {
            lower_tree(parse("first: { let x = second::y; } second: { let y = first::x; }"));
        }).unwrap_err();
        let message = error.downcast_ref::<String>().unwrap();
        assert!(message.contains("first::x") && message.contains("second::y"), "{message}");
    }

    #[test]
    #[should_panic(expected = "path `second::missing`: scope `second` does not define field `missing`")]
    fn missing_sibling_field_names_scope_and_field() {
        lower_tree(parse("let source = input 0; first: { let x = second::missing; } second: { let value = source; }"));
    }

    #[test]
    #[should_panic(expected = "`X` is both a let and child scope")]
    fn binding_and_scope_cannot_share_a_name() {
        lower_tree(parse("let X = input 0; X: { let value = input 1; }"));
    }

    #[test]
    fn root_path_descends_three_scopes() {
        let src = "let source = input 0; a: { b: { c: { let value = source; } } } \
            export \"out\" = ::a::b::c::value;";
        let prog = lower_tree(parse(src));
        assert_eq!(child(child(child(&prog.root, "a"), "b"), "c").exports[0].name, "value");
        assert!(matches!(prog.root.exports[0].value, Ref::ChildExport(..)));
    }

    #[test]
    fn grandchild_reads_uncles_child() {
        let src = "let source = input 0; uncle: { branch: { let value = source; } } \
            parent: { child: { let result = uncle::branch::value; } let result = child::result; } \
            export \"out\" = parent::result;";
        let prog = lower_tree(parse(src));
        let parent = child(&prog.root, "parent");
        let grandchild = child(parent, "child");
        assert!(parent.imports.iter().any(|i| i.name == "::uncle::branch::value"
            && matches!(i.from, Source::Parent(Ref::ChildExport(..)))));
        assert!(grandchild.imports.iter().any(|i| i.name == "::uncle::branch::value"
            && matches!(i.from, Source::Parent(Ref::Import(_)))));
    }

    #[test]
    fn enclosing_scope_reads_the_rest_inside_it() {
        let src = "let source = input 0; outer: { fwd: { let labels = source; } \
            bwd: { let result = outer::fwd::labels; } let result = bwd::result; } \
            export \"out\" = outer::result;";
        let prog = lower_tree(parse(src));
        let outer = child(&prog.root, "outer");
        let bwd = child(outer, "bwd");
        assert!(bwd.imports.iter().any(|i| i.name == "::outer::fwd::labels"
            && matches!(i.from, Source::Parent(Ref::ChildExport(..)))));
    }

    #[test]
    fn one_field_can_be_read_at_two_anchors() {
        let src = "let source = input 0; outer: { fwd: { let labels = source; } \
            bwd: { let result = outer::fwd::labels; } let result = bwd::result; } \
            other: { let result = ::outer::fwd::labels; } \
            export \"out\" = outer::result + other::result;";
        let prog = lower_tree(parse(src));
        assert_eq!(child(child(&prog.root, "outer"), "fwd").exports.len(), 1);
        assert!(child(&prog.root, "other").imports.iter().any(|i| i.name == "::outer::fwd::labels"));
    }

    #[test]
    #[should_panic(expected = "path `outer::fwd::labels` descends into enclosing scope `fwd` from `outer`; inside `fwd`, write `labels`")]
    fn path_into_enclosing_scope_is_rejected() {
        lower_tree(parse("outer: { fwd: { let labels = input 0; let x = outer::fwd::labels; } }"));
    }

    #[test]
    fn disjoint_subtrees_can_reuse_scope_name() {
        let src = "let source = input 0; left: { shared: { let value = source; } \
            let result = shared::value; } right: { shared: { let value = source; } \
            let result = shared::value; } export \"out\" = left::result + right::result;";
        let prog = lower_tree(parse(src));
        assert_eq!(child(child(&prog.root, "left"), "shared").exports[0].name, "value");
        assert_eq!(child(child(&prog.root, "right"), "shared").exports[0].name, "value");
    }

    #[test]
    fn root_is_available_as_a_child_scope_name() {
        let prog = lower_tree(parse("let source = input 0; root: { let value = source; } export \"out\" = root::value;"));
        assert_eq!(child(&prog.root, "root").exports[0].name, "value");
    }

    #[test]
    #[should_panic(expected = "path `unknown::value` has unknown anchor `unknown` from scope `root`")]
    fn unknown_anchor_names_path_and_scope() {
        lower_tree(parse("export \"out\" = unknown::value;"));
    }

    #[test]
    #[should_panic(expected = "path `a::missing::value` has unknown child scope `missing` in `a`")]
    fn unknown_child_names_path_and_scope() {
        lower_tree(parse("a: { let value = input 0; } export \"out\" = a::missing::value;"));
    }

    #[test]
    #[should_panic(expected = "path `a::value::field` needs a scope at `value` in `a`, but it is a binding")]
    fn binding_cannot_be_path_scope() {
        lower_tree(parse("a: { let value = input 0; } export \"out\" = a::value::field;"));
    }

    #[test]
    #[should_panic(expected = "path `a::b` needs a binding at `b` in `a`, but it is a scope")]
    fn scope_cannot_be_path_field() {
        lower_tree(parse("a: { b: { let value = input 0; } } export \"out\" = a::b;"));
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
        let src = "let a = input 0 | map(((a, b), ()) -> (a, b)) | filter((a, b) -> (a, b) ne) | negate;\nexport \"result\" = a;";
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
            let a = input 0 | map(((a, b), ()) -> (a, b));\n\
            let b = input 0 | map(((a, b), ()) -> (b, a));\n\
            let j1 = a | join(b, (_, x, y) -> (x, y));\n\
            let j2 = a | join(b, (_, x, y) -> (y, x));\n\
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
            let a = input 0 | map(((a, b), ()) -> (a, b)) | distinct;\n\
            let b = input 0 | map(((a, b), ()) -> (a, b));\n\
            export \"result\" = a | join(b, (_, x, _) -> (x, ()));";
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
            let e = input 0 | map(((a, b), ()) -> (a, b));\n\
            s: {\n\
                set x = e | map((a, b) -> (a, b)) | filter((a, b) -> (a, b) ne) | distinct;\n\
            }\n\
            export \"result\" = s::x;";
        let mut p = lower_tree(parse(src));
        p.optimize();
        let Item::Sub(child) = p.root.items.iter().find(|i| matches!(i, Item::Sub(_))).unwrap() else { unreachable!() };
        let linears: Vec<usize> = ops(child).iter().filter_map(|n| match n { Node::Linear { ops, .. } => Some(ops.len()), _ => None }).collect();
        assert_eq!(linears, vec![3, 1], "the key, filter and distinct record map fuse; the inverse stays after reduce");
    }
}
