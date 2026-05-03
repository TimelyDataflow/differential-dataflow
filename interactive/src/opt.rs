//! Optimization-oriented IR: a tree of scopes, each holding a hash-consed term
//! graph over collection-level operators. See module docs for design intent.
//!
//! Status: term-graph stage with round-trip to/from `crate::ir` working.
//! Transforms (CSE, fusion, LICM) are stubs; analyses not yet started.

use std::collections::{BTreeMap, HashMap};

use crate::ir;
use crate::ir::LinearOp;
use crate::parse::{Projection, Reducer};

pub type EId = usize;
pub type ScopeName = String;

/// One node in a scope's term graph. Children are `EId`s into the same arena.
#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub enum Op {
    Input(usize),
    /// Chain of one or more linear (per-record) operations on a stream.
    /// Mirrors `ir::Node::Linear`. Multiple ops appear after fusion.
    Linear(EId, Vec<LinearOp>),
    Concat(Vec<EId>),
    Arrange(EId),
    Join(EId, EId, Projection),
    Reduce(EId, Reducer),
    Inspect(EId, String),
    /// Placeholder marker: a sub-scope by the given name appears here in the
    /// parent's arena order. The actual `Scope` lives in `Scope::subs[name]`.
    /// Has no children and is never the target of a child reference.
    SubScope(ScopeName),
    /// Reference to an export of a sub-scope: `scope::export`.
    ScopeRef(ScopeName, String),
    /// Unqualified name. Resolves first against this scope's `lets`/`vars`,
    /// then walks ancestors. Used for both intra-scope `var` references and
    /// references to ancestor `let` bindings.
    Name(String),
}

#[derive(Default)]
pub struct Arena {
    nodes: Vec<Op>,
    cons: HashMap<Op, EId>,
}

impl Arena {
    pub fn intern(&mut self, op: Op) -> EId {
        if let Some(&id) = self.cons.get(&op) { return id; }
        let id = self.nodes.len();
        self.nodes.push(op.clone());
        self.cons.insert(op, id);
        id
    }
    pub fn get(&self, id: EId) -> &Op { &self.nodes[id] }
    pub fn len(&self) -> usize { self.nodes.len() }
    pub fn iter(&self) -> impl Iterator<Item = (EId, &Op)> { self.nodes.iter().enumerate() }
}

pub struct Scope {
    pub name: String,
    pub arena: Arena,
    /// `let` bindings; `name = arena[eid]` holds.
    pub lets: BTreeMap<String, EId>,
    /// `var` bindings; equality holds only at the fixed point. Body is
    /// `arena[eid]`; the equation is *not* asserted into the arena.
    pub vars: BTreeMap<String, EId>,
    /// Child scopes, owned here. An `Op::SubScope(name)` in `arena`
    /// corresponds to `subs[name]`.
    pub subs: BTreeMap<ScopeName, Scope>,
    /// Substitutions produced by rewrites: every reference to `key` resolves
    /// to `value`. Today a degenerate union-find (paths kept short by passing
    /// through `canonical`); the seed of an e-graph's union-find later.
    pub rewrites: BTreeMap<EId, EId>,
}

impl Scope {
    pub fn new(name: impl Into<String>) -> Self {
        Scope {
            name: name.into(), arena: Arena::default(),
            lets: BTreeMap::new(), vars: BTreeMap::new(), subs: BTreeMap::new(),
            rewrites: BTreeMap::new(),
        }
    }
    pub fn resolve_local(&self, name: &str) -> Option<EId> {
        self.lets.get(name).copied().or_else(|| self.vars.get(name).copied())
    }
    /// Walk the rewrites chain to a fixed point. No path compression yet.
    pub fn canonical(&self, mut eid: EId) -> EId {
        while let Some(&next) = self.rewrites.get(&eid) { eid = next; }
        eid
    }

    /// Collapse `Arrange(x)` where `x` is itself `Arrange(_)` or `Reduce(_, _)`.
    /// Recurses into sub-scopes.
    pub fn collapse_arranges(&mut self) {
        let mut new_subs: Vec<(EId, EId)> = Vec::new();
        for (eid, op) in self.arena.iter() {
            if let Op::Arrange(child) = op {
                let child_canon = self.canonical(*child);
                match self.arena.get(child_canon) {
                    Op::Arrange(_) | Op::Reduce(_, _) => {
                        new_subs.push((eid, child_canon));
                    }
                    _ => {}
                }
            }
        }
        for (from, to) in new_subs { self.rewrites.insert(from, to); }
        for sub in self.subs.values_mut() { sub.collapse_arranges(); }
    }

    /// Count uses of each canonical EId from within this scope: arena
    /// children, lets, and vars. Sub-scope cross-references go through
    /// `Op::Name` in the child's arena (counted there), so this only sees
    /// intra-scope use.
    fn use_counts(&self) -> BTreeMap<EId, usize> {
        let mut counts: BTreeMap<EId, usize> = BTreeMap::new();
        let bump = |e: EId, c: &mut BTreeMap<EId, usize>| {
            *c.entry(self.canonical(e)).or_insert(0) += 1;
        };
        for (eid, op) in self.arena.iter() {
            if self.canonical(eid) != eid { continue; }   // dead/aliased
            match op {
                Op::Linear(c, _) | Op::Arrange(c) | Op::Reduce(c, _) | Op::Inspect(c, _)
                    => bump(*c, &mut counts),
                Op::Join(l, r, _) => { bump(*l, &mut counts); bump(*r, &mut counts); }
                Op::Concat(cs) => for c in cs { bump(*c, &mut counts); },
                Op::Input(_) | Op::SubScope(_) | Op::ScopeRef(_, _) | Op::Name(_) => {}
            }
        }
        for &e in self.lets.values() { bump(e, &mut counts); }
        for &e in self.vars.values() { bump(e, &mut counts); }
        counts
    }

    /// Fuse `Linear(child, outer_ops)` where canonical(child) is itself a
    /// `Linear(grandchild, inner_ops)` with intra-scope use-count 1.
    /// Recurses into sub-scopes. Iterates to a fixed point.
    pub fn fuse_linear(&mut self) {
        loop {
            let counts = self.use_counts();
            let mut new_subs: Vec<(EId, EId, Vec<LinearOp>)> = Vec::new();
            for (eid, op) in self.arena.iter() {
                if self.canonical(eid) != eid { continue; }
                if let Op::Linear(child, outer_ops) = op {
                    let child_canon = self.canonical(*child);
                    if counts.get(&child_canon).copied().unwrap_or(0) != 1 { continue; }
                    if let Op::Linear(grand, inner_ops) = self.arena.get(child_canon) {
                        let mut fused = inner_ops.clone();
                        fused.extend(outer_ops.iter().cloned());
                        new_subs.push((eid, *grand, fused));
                    }
                }
            }
            if new_subs.is_empty() { break; }
            for (outer, grandchild, fused_ops) in new_subs {
                let grandchild = self.canonical(grandchild);
                let new_eid = self.arena.intern(Op::Linear(grandchild, fused_ops));
                self.rewrites.insert(outer, new_eid);
            }
        }
        for sub in self.subs.values_mut() { sub.fuse_linear(); }
    }

    /// Compute the set of EIds reachable (in canonical form) from this
    /// scope's lets, vars, and an optional extra root (e.g. program result).
    pub fn reachable(&self, extra: Option<EId>) -> std::collections::BTreeSet<EId> {
        let mut seen: std::collections::BTreeSet<EId> = Default::default();
        let mut stack: Vec<EId> = Vec::new();
        stack.extend(self.lets.values().copied());
        stack.extend(self.vars.values().copied());
        if let Some(e) = extra { stack.push(e); }
        let mut stack: Vec<EId> = stack.into_iter().map(|e| self.canonical(e)).collect();
        while let Some(e) = stack.pop() {
            if !seen.insert(e) { continue; }
            match self.arena.get(e) {
                Op::Linear(c, _) | Op::Arrange(c) | Op::Reduce(c, _) | Op::Inspect(c, _)
                    => stack.push(self.canonical(*c)),
                Op::Join(l, r, _) => { stack.push(self.canonical(*l)); stack.push(self.canonical(*r)); }
                Op::Concat(cs) => for c in cs { stack.push(self.canonical(*c)); },
                Op::SubScope(_) | Op::ScopeRef(_, _) | Op::Input(_) | Op::Name(_) => {}
            }
        }
        seen
    }
}

pub struct Program {
    pub root: Scope,
    pub result: EId,
}

// ---------------------------------------------------------------------------
// Lift: flat ir -> tree of scopes.

fn scope_at_path<'a>(root: &'a mut Scope, path: &[String]) -> &'a mut Scope {
    let mut s = root;
    for n in path { s = s.subs.get_mut(n).expect("missing sub-scope on path"); }
    s
}

pub fn lift(program: &ir::Program) -> Program {
    let mut root = Scope::new("root");
    let mut path: Vec<String> = Vec::new();
    let mut sub_counter: usize = 0;
    // For each flat id, the scope path of its arena and the eid within.
    let mut home: BTreeMap<ir::Id, (Vec<String>, EId)> = BTreeMap::new();

    // Resolve a child reference: returns the eid to use *within the current
    // scope's arena*. If the home scope is the current scope, that's just the
    // home eid; if it's an ancestor, we synthesize a `let` in the ancestor
    // and return an `Op::Name(synth)` interned in the current arena.
    fn resolve_child(
        root: &mut Scope, path: &[String],
        home: &BTreeMap<ir::Id, (Vec<String>, EId)>,
        flat: ir::Id,
    ) -> EId {
        let (home_path, home_eid) = home.get(&flat).expect("unresolved flat id").clone();
        if home_path == *path {
            return home_eid;
        }
        // Ancestor: must be a strict prefix of `path`.
        debug_assert!(path.starts_with(&home_path));
        let synth = format!("_l{}", flat);
        let ancestor = scope_at_path(root, &home_path);
        ancestor.lets.entry(synth.clone()).or_insert(home_eid);
        let here = scope_at_path(root, path);
        here.arena.intern(Op::Name(synth))
    }

    for (&fid, node) in &program.nodes {
        match node {
            ir::Node::Scope => {
                let name = format!("s{}", sub_counter);
                sub_counter += 1;
                let parent = scope_at_path(&mut root, &path);
                parent.subs.insert(name.clone(), Scope::new(name.clone()));
                parent.arena.intern(Op::SubScope(name.clone()));
                path.push(name);
            }
            ir::Node::EndScope => { path.pop(); }
            ir::Node::Input(n) => {
                let scope = scope_at_path(&mut root, &path);
                let eid = scope.arena.intern(Op::Input(*n));
                home.insert(fid, (path.clone(), eid));
            }
            ir::Node::Variable => {
                let var_name = format!("_v{}", fid);
                let scope = scope_at_path(&mut root, &path);
                let eid = scope.arena.intern(Op::Name(var_name));
                home.insert(fid, (path.clone(), eid));
            }
            ir::Node::Bind { variable, value } => {
                let var_name = format!("_v{}", variable);
                let value_eid = resolve_child(&mut root, &path, &home, *value);
                let scope = scope_at_path(&mut root, &path);
                scope.vars.insert(var_name, value_eid);
            }
            ir::Node::Leave(inner_id, _level) => {
                let (inner_path, inner_eid) = home.get(inner_id).expect("Leave of unknown id").clone();
                debug_assert_eq!(inner_path.len(), path.len() + 1);
                let sub_name = inner_path.last().unwrap().clone();
                let export = format!("_e{}", inner_id);
                let parent = scope_at_path(&mut root, &path);
                let sub = parent.subs.get_mut(&sub_name).expect("Leave references missing sub-scope");
                sub.lets.entry(export.clone()).or_insert(inner_eid);
                let eid = parent.arena.intern(Op::ScopeRef(sub_name, export));
                home.insert(fid, (path.clone(), eid));
            }
            ir::Node::Linear { input, ops } => {
                let in_eid = resolve_child(&mut root, &path, &home, *input);
                let scope = scope_at_path(&mut root, &path);
                let eid = scope.arena.intern(Op::Linear(in_eid, ops.clone()));
                home.insert(fid, (path.clone(), eid));
            }
            ir::Node::Concat(ids) => {
                let eids: Vec<EId> = ids.iter().map(|i| resolve_child(&mut root, &path, &home, *i)).collect();
                let scope = scope_at_path(&mut root, &path);
                let eid = scope.arena.intern(Op::Concat(eids));
                home.insert(fid, (path.clone(), eid));
            }
            ir::Node::Arrange(input) => {
                let in_eid = resolve_child(&mut root, &path, &home, *input);
                let scope = scope_at_path(&mut root, &path);
                let eid = scope.arena.intern(Op::Arrange(in_eid));
                home.insert(fid, (path.clone(), eid));
            }
            ir::Node::Join { left, right, projection } => {
                let l = resolve_child(&mut root, &path, &home, *left);
                let r = resolve_child(&mut root, &path, &home, *right);
                let scope = scope_at_path(&mut root, &path);
                let eid = scope.arena.intern(Op::Join(l, r, projection.clone()));
                home.insert(fid, (path.clone(), eid));
            }
            ir::Node::Reduce { input, reducer } => {
                let in_eid = resolve_child(&mut root, &path, &home, *input);
                let scope = scope_at_path(&mut root, &path);
                let eid = scope.arena.intern(Op::Reduce(in_eid, reducer.clone()));
                home.insert(fid, (path.clone(), eid));
            }
            ir::Node::Inspect { input, label } => {
                let in_eid = resolve_child(&mut root, &path, &home, *input);
                let scope = scope_at_path(&mut root, &path);
                let eid = scope.arena.intern(Op::Inspect(in_eid, label.clone()));
                home.insert(fid, (path.clone(), eid));
            }
        }
    }

    let result_eid = home.get(&program.result).expect("result not lifted").1;
    Program { root, result: result_eid }
}

// ---------------------------------------------------------------------------
// Lower: tree of scopes -> flat ir.

struct LowerCtx {
    nodes: BTreeMap<ir::Id, ir::Node>,
    next_id: ir::Id,
}

impl LowerCtx {
    fn fresh(&mut self) -> ir::Id { let id = self.next_id; self.next_id += 1; id }
    fn emit(&mut self, node: ir::Node) -> ir::Id { let id = self.fresh(); self.nodes.insert(id, node); id }
}

/// Per-scope: mapping from name to the flat id that name references.
/// For lets, this is the body's flat id. For vars, this is the placeholder
/// `Variable` flat id (so recursive references resolve correctly).
type NameMap = BTreeMap<String, ir::Id>;

fn lower_scope(
    ctx: &mut LowerCtx,
    scope: &Scope,
    is_root: bool,
    program_result: Option<EId>,      // only Some(_) for the root scope
    ancestors: &[(usize, NameMap)],
) -> (NameMap, BTreeMap<EId, ir::Id>) {
    if !is_root { ctx.emit(ir::Node::Scope); }
    let reachable = scope.reachable(program_result);

    // Pre-emit Variable placeholders for every var; record name -> flat id.
    let mut names: NameMap = NameMap::new();
    let mut var_flat: BTreeMap<String, ir::Id> = BTreeMap::new();
    for var_name in scope.vars.keys() {
        let v = ctx.emit(ir::Node::Variable);
        names.insert(var_name.clone(), v);
        var_flat.insert(var_name.clone(), v);
    }

    // For each canonical EId, the let names whose body resolves to it. Used
    // to bind let names in `names` as their bodies are emitted.
    let mut canonical_to_lets: BTreeMap<EId, Vec<String>> = BTreeMap::new();
    for (n, &e) in &scope.lets {
        canonical_to_lets.entry(scope.canonical(e)).or_default().push(n.clone());
    }

    // Walk the arena; emit flat nodes; track eid -> flat id.
    let mut eid_to_flat: BTreeMap<EId, ir::Id> = BTreeMap::new();

    // Track sub-scopes' name maps as we lower them, so ScopeRef can look up
    // their exports.
    let mut sub_names: BTreeMap<String, NameMap> = BTreeMap::new();
    let mut sub_eid_maps: BTreeMap<String, BTreeMap<EId, ir::Id>> = BTreeMap::new();

    // Helper: resolve a name to a flat id by walking ancestors first to last
    // (innermost has been built into `names` already for the current scope).
    let resolve_name = |n: &str, names: &NameMap, ancestors: &[(usize, NameMap)]| -> ir::Id {
        if let Some(&fid) = names.get(n) { return fid; }
        for (_, nm) in ancestors.iter().rev() {
            if let Some(&fid) = nm.get(n) { return fid; }
        }
        panic!("unresolved name: {}", n);
    };

    let level = ancestors.len();
    let scope_level = if is_root { 0 } else { level };

    // Determine emission set: reachable canonicals, plus all SubScope markers
    // (which must emit brackets even if no one references them).
    let mut emission: std::collections::BTreeSet<EId> = reachable.clone();
    for (eid, op) in scope.arena.iter() {
        if matches!(op, Op::SubScope(_)) { emission.insert(eid); }
    }
    // Compute emission order: each canonical EId emits at the position of its
    // earliest substituter in arena order. This keeps children-before-parents
    // valid even when fusion has interned a fused node late in the arena.
    let mut earliest: BTreeMap<EId, EId> = BTreeMap::new();
    for (eid, _) in scope.arena.iter() {
        let c = scope.canonical(eid);
        if !emission.contains(&c) { continue; }
        let pos = earliest.entry(c).or_insert(eid);
        if eid < *pos { *pos = eid; }
    }
    let mut order: Vec<(EId, EId)> = earliest.iter().map(|(c, e)| (*e, *c)).collect();
    order.sort();

    for (_pos, eid) in &order {
        let eid = *eid;
        let op = scope.arena.get(eid);
        let child_flat = |c: &EId, e2f: &BTreeMap<EId, ir::Id>| e2f[&scope.canonical(*c)];
        match op {
            Op::Input(n) => {
                let f = ctx.emit(ir::Node::Input(*n));
                eid_to_flat.insert(eid, f);
            }
            Op::Linear(child, ops) => {
                let cf = child_flat(child, &eid_to_flat);
                let f = ctx.emit(ir::Node::Linear { input: cf, ops: ops.clone() });
                eid_to_flat.insert(eid, f);
            }
            Op::Concat(children) => {
                let cs: Vec<ir::Id> = children.iter().map(|c| child_flat(c, &eid_to_flat)).collect();
                let f = ctx.emit(ir::Node::Concat(cs));
                eid_to_flat.insert(eid, f);
            }
            Op::Arrange(child) => {
                let cf = child_flat(child, &eid_to_flat);
                let f = ctx.emit(ir::Node::Arrange(cf));
                eid_to_flat.insert(eid, f);
            }
            Op::Join(l, r, proj) => {
                let lf = child_flat(l, &eid_to_flat); let rf = child_flat(r, &eid_to_flat);
                let f = ctx.emit(ir::Node::Join { left: lf, right: rf, projection: proj.clone() });
                eid_to_flat.insert(eid, f);
            }
            Op::Reduce(child, red) => {
                let cf = child_flat(child, &eid_to_flat);
                let f = ctx.emit(ir::Node::Reduce { input: cf, reducer: red.clone() });
                eid_to_flat.insert(eid, f);
            }
            Op::Inspect(child, label) => {
                let cf = child_flat(child, &eid_to_flat);
                let f = ctx.emit(ir::Node::Inspect { input: cf, label: label.clone() });
                eid_to_flat.insert(eid, f);
            }
            Op::Name(n) => {
                // No flat node; resolve this eid to whatever name maps to.
                let fid = resolve_name(n, &names, ancestors);
                eid_to_flat.insert(eid, fid);
            }
            Op::SubScope(name) => {
                // Recurse. The sub-scope emits its own Scope/EndScope brackets.
                let sub = scope.subs.get(name).expect("missing sub-scope");
                let mut new_ancestors = ancestors.to_vec();
                new_ancestors.push((scope_level, names.clone()));
                let (sn, se) = lower_scope(ctx, sub, false, None, &new_ancestors);
                sub_names.insert(name.clone(), sn);
                sub_eid_maps.insert(name.clone(), se);
                // No eid_to_flat entry; SubScope marker is not addressed.
            }
            Op::ScopeRef(sub_name, export) => {
                let sn = sub_names.get(sub_name).expect("ScopeRef before SubScope emission");
                let se = sub_eid_maps.get(sub_name).expect("ScopeRef before SubScope emission");
                // The export's flat id sits in the sub-scope's arena (as a let body).
                let sub = scope.subs.get(sub_name).expect("missing sub-scope");
                let body_eid = sub.canonical(*sub.lets.get(export).expect("export not found"));
                let _ = sn;
                let body_flat = se[&body_eid];
                let f = ctx.emit(ir::Node::Leave(body_flat, scope_level + 1));
                eid_to_flat.insert(eid, f);
            }
        }
        // After emitting, bind any let names resolving to this canonical.
        if let Some(let_names) = canonical_to_lets.get(&eid) {
            if let Some(&f) = eid_to_flat.get(&eid) {
                for n in let_names { names.insert(n.clone(), f); }
            }
        }
    }

    // Emit Bind nodes for each var.
    for (var_name, body_eid) in &scope.vars {
        let var_id = var_flat[var_name];
        let value_id = eid_to_flat[&scope.canonical(*body_eid)];
        ctx.emit(ir::Node::Bind { variable: var_id, value: value_id });
    }

    if !is_root { ctx.emit(ir::Node::EndScope); }
    (names, eid_to_flat)
}

pub fn lower(program: &Program) -> ir::Program {
    let mut ctx = LowerCtx { nodes: BTreeMap::new(), next_id: 0 };
    let result_eid = program.root.canonical(program.result);
    let (_names, eid_to_flat) = lower_scope(&mut ctx, &program.root, true, Some(result_eid), &[]);
    let result = eid_to_flat[&result_eid];
    ir::Program { nodes: ctx.nodes, result }
}

// ---------------------------------------------------------------------------
// Transforms.

impl Program {
    /// Run all available rewrites to a (currently shallow) fixed point.
    pub fn optimize(&mut self) {
        self.root.collapse_arranges();
        self.root.fuse_linear();
    }
}
