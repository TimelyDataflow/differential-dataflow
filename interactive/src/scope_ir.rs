//! Scope-tree IR: a tree of scopes, each owning its IR, with cross-scope flow
//! as explicit import/export edges and feedback variables first-class.
//! `lower::lower_tree` (AST -> tree) builds it; the backends' `render_tree`
//! (tree -> dataflow) walk it.
//!
//! # Why a tree
//!
//! A scope boundary is a real semantic barrier: an operator cannot be hoisted
//! across it, and an arrangement cannot be shared across it freely. The flat
//! IR encodes boundaries *positionally* (`Scope`/`EndScope` markers in a node
//! list), so every consumer that cares reconstructs them by analysis — and
//! that reconstruction is where the explanation rewrite's level errors lived.
//! Here the boundary is structural: a `Scope` owns its items, and nothing
//! crosses except through an explicit import or export.
//!
//! The guiding principle throughout: make explicit anything a consumer would
//! otherwise have to analyze the IR to recover. Feedback variables are a
//! first-class list (the renderer never scans for them), `items` is in
//! dependency order (rendering is a straight walk, no topological pass), and
//! cross-scope references are edges (no reconstruction of who-refers-to-whom).
//!
//! # Two axes of a scope
//!
//! Structurally, every `{ .. }` renders as a timely region (`enter_region` /
//! `leave_region`): real dataflow nesting, timestamp *type* unchanged.
//! Temporally, an *iterating* scope additionally pushes a `PointStamp`
//! coordinate (`enter_dynamic` / `leave_dynamic`): a timestamp *value*, the
//! iteration counter. A scope iterates iff it has `vars`; there is no
//! separate kind field to fall out of sync with that. When acyclic scopes
//! earn distinct (region-only) rendering, `Scope` becomes an enum so each
//! variant carries only its valid fields.
//!
//! # Cross-scope names
//!
//! Names are local to a scope; the front-end's spanning names (`edges`,
//! `scope::field`) are resolved by lowering into local handles plus edges.
//! Identity is the *definition an edge points at*, not string equality: two
//! imports targeting the same definition are the same entity, so facts
//! learned at a definition propagate to every importer along its edges.

use crate::ir::LinearOp;
use crate::parse::{Projection, Reducer};

pub type ItemId = usize; // index into `Scope::items`
pub type ImportId = usize; // index into `Scope::imports`
pub type VarId = usize; // index into `Scope::vars`

/// A reference, resolved *within its own scope*. Nothing here can name an item
/// in another scope's interior — cross-scope flow goes through imports/exports.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum Ref {
    /// An earlier `Op` item in this scope.
    Local(ItemId),
    /// A value entered via this scope's imports.
    Import(ImportId),
    /// A feedback variable of this scope.
    Var(VarId),
    /// Export `usize` of a `Sub` item (a child scope) in this scope.
    ChildExport(ItemId, usize),
}

/// Where an import's value comes from. Inner scopes import from the parent; the
/// root imports the program's external sources — unifying `Input`/`Import`.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum Source {
    /// A reference in the parent scope.
    Parent(Ref),
    /// A positional program input (root only).
    Input(usize),
    /// A named external trace (root only).
    Trace(String),
}

/// A value brought into a scope — `enter_region` (+ `enter_dynamic` if iterating).
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Import {
    pub name: String,
    pub from: Source,
}

/// A value surrendered up — `leave_region` (+ `leave_dynamic` if iterating).
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Export {
    pub name: String,
    pub value: Ref,
}

/// A feedback variable, identified by its index in `Scope::vars`. Carries its
/// source name (readability, cross-scope name visibility). Its shape is *not*
/// stored — the shape pass derives every node/var shape when a consumer needs
/// it, so storing it here would cache a derivable (and currently unknown) value.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Var {
    pub name: String,
}

/// Closes a feedback loop: `var <- value`.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Bind {
    pub var: VarId,
    pub value: Ref,
}

/// A pure operator. Sources (`Input`/`Import`) are imports; structure
/// (`Scope`/`Variable`/`Leave`/`Bind`) is the scope itself — neither is a node.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum Node {
    Linear { input: Ref, ops: Vec<LinearOp> },
    Concat(Vec<Ref>),
    Arrange(Ref),
    Join { left: Ref, right: Ref, projection: Projection },
    Reduce { input: Ref, reducer: Reducer },
    Inspect { input: Ref, label: String },
}

/// An item in a scope's body: a pure operator or a nested child scope. The
/// `items` vec is in dependency order, so rendering is a straight walk — no
/// topological re-analysis. `Ref::Local`/`ChildExport` index into it.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum Item {
    Op(Node),
    Sub(Scope),
}

/// A scope owns its IR. It iterates iff it has `vars` (no `kind` field yet; see
/// the design doc). Children are owned, inline in `items`.
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct Scope {
    /// The source name of the `{ .. }` block ("root" for the program root).
    /// Names the timely region at render time and labels the scope in viz.
    pub name: String,
    pub imports: Vec<Import>,
    pub vars: Vec<Var>,
    pub items: Vec<Item>,
    pub binds: Vec<Bind>,
    pub exports: Vec<Export>,
}

/// A program is its root scope: root imports are the external sources, root
/// exports are the program's outputs.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Program {
    pub root: Scope,
}

impl Program {
    /// Optimize every scope in place. See `Scope::optimize`.
    pub fn optimize(&mut self) {
        self.root.optimize();
    }

    /// Print the tree as indented structural text (readability, not
    /// parseability): imports and vars first, items in order (`Sub`s nest),
    /// then binds and exports.
    pub fn dump(&self) {
        dump_scope(&self.root, 0);
    }

    /// Total operator (`Op`) count across all scopes.
    pub fn op_count(&self) -> usize {
        fn count(s: &Scope) -> usize {
            s.items.iter().map(|i| match i { Item::Op(_) => 1, Item::Sub(c) => count(c) }).sum()
        }
        count(&self.root)
    }
}

impl Scope {
    /// Optimize this scope (and, recursively, its children) in place, iterating
    /// three rewrites to a fixed point:
    ///
    /// - collapse an `Arrange` whose input is already an arrangement
    ///   (an `Arrange` or a `Reduce`);
    /// - fuse a `Linear` into its `Linear` input when it is that input's only
    ///   consumer;
    /// - deduplicate structurally identical operators.
    ///
    /// All three are *within-scope*: a scope boundary is a semantic barrier,
    /// so nothing merges or moves across one. (The flat IR's dedup was
    /// position-blind and could merge across boundaries — sound only because
    /// the dynamic model has no structural nesting; here the structure makes
    /// the restriction automatic.)
    pub fn optimize(&mut self) {
        for item in self.items.iter_mut() {
            if let Item::Sub(child) = item { child.optimize(); }
        }
        let mut dead: Vec<bool> = vec![false; self.items.len()];
        loop {
            let changed = self.collapse_arranges(&mut dead)
                | self.fuse_linear(&mut dead)
                | self.dedup(&mut dead);
            if !changed { break; }
        }
        self.compact(&dead);
    }

    /// Apply `f` to every `Ref` in this scope: operator inputs, child-scope
    /// import edges, binds, and exports.
    fn for_each_ref(&mut self, mut f: impl FnMut(&mut Ref)) {
        for item in self.items.iter_mut() {
            match item {
                Item::Op(node) => match node {
                    Node::Linear { input, .. } | Node::Arrange(input)
                    | Node::Reduce { input, .. } | Node::Inspect { input, .. } => f(input),
                    Node::Join { left, right, .. } => { f(left); f(right); },
                    Node::Concat(refs) => for r in refs { f(r); },
                },
                Item::Sub(child) => for imp in child.imports.iter_mut() {
                    if let Source::Parent(r) = &mut imp.from { f(r); }
                },
            }
        }
        for b in self.binds.iter_mut() { f(&mut b.value); }
        for e in self.exports.iter_mut() { f(&mut e.value); }
    }

    /// Count, for each item, the references to it (`Local` or `ChildExport`)
    /// from LIVE holders only. Dead items keep their (stale, already-redirected-
    /// around) input refs until `compact`, and counting those would make a
    /// single-consumer item look shared — blocking fusion after its consumer
    /// chain has partially fused (e.g. the middle of a three-op linear chain
    /// dies into its consumer, and its stale input ref still counts against
    /// the head, so the chain never finishes fusing).
    fn use_counts(&self, dead: &[bool]) -> Vec<usize> {
        let mut counts = vec![0usize; self.items.len()];
        let mut tally = |r: &Ref| match r {
            Ref::Local(i) | Ref::ChildExport(i, _) => counts[*i] += 1,
            Ref::Import(_) | Ref::Var(_) => {},
        };
        for (i, item) in self.items.iter().enumerate() {
            if dead[i] { continue; }
            match item {
                Item::Op(node) => match node {
                    Node::Linear { input, .. } | Node::Arrange(input)
                    | Node::Reduce { input, .. } | Node::Inspect { input, .. } => tally(input),
                    Node::Join { left, right, .. } => { tally(left); tally(right); },
                    Node::Concat(refs) => for r in refs { tally(r); },
                },
                Item::Sub(child) => for imp in &child.imports {
                    if let Source::Parent(r) = &imp.from { tally(r); }
                },
            }
        }
        for b in &self.binds { tally(&b.value); }
        for e in &self.exports { tally(&e.value); }
        counts
    }

    /// Redirect every `Ref::Local(from)` to `to`, and mark `from` dead.
    fn redirect(&mut self, from: usize, to: Ref, dead: &mut [bool]) {
        self.for_each_ref(|r| if matches!(r, Ref::Local(i) if *i == from) { *r = to.clone(); });
        dead[from] = true;
    }

    /// `Arrange` of an arrangement (an `Arrange` or `Reduce` item) is redundant.
    fn collapse_arranges(&mut self, dead: &mut [bool]) -> bool {
        let mut changed = false;
        for i in 0..self.items.len() {
            if dead[i] { continue; }
            let Item::Op(Node::Arrange(Ref::Local(j))) = &self.items[i] else { continue };
            let j = *j;
            if !dead[j] && matches!(&self.items[j], Item::Op(Node::Arrange(_) | Node::Reduce { .. })) {
                self.redirect(i, Ref::Local(j), dead);
                changed = true;
            }
        }
        changed
    }

    /// Fuse a `Linear` into its `Linear` input when it is the only consumer.
    fn fuse_linear(&mut self, dead: &mut [bool]) -> bool {
        let counts = self.use_counts(dead);
        let mut changed = false;
        for i in 0..self.items.len() {
            if dead[i] { continue; }
            let Item::Op(Node::Linear { input: Ref::Local(j), .. }) = &self.items[i] else { continue };
            let j = *j;
            if dead[j] || counts[j] != 1 { continue; }
            let Item::Op(Node::Linear { input: inner_input, ops: inner_ops }) = self.items[j].clone() else { continue };
            let Item::Op(Node::Linear { ops, .. }) = &mut self.items[i] else { unreachable!() };
            let mut fused = inner_ops;
            fused.append(ops);
            self.items[i] = Item::Op(Node::Linear { input: inner_input, ops: fused });
            dead[j] = true;
            changed = true;
        }
        changed
    }

    /// Deduplicate structurally identical operators (the later one redirects to
    /// the earlier, so references keep pointing backward).
    fn dedup(&mut self, dead: &mut [bool]) -> bool {
        use std::collections::HashMap;
        let mut seen: HashMap<String, usize> = HashMap::new();
        let mut redirects: Vec<(usize, usize)> = Vec::new();
        for i in 0..self.items.len() {
            if dead[i] { continue; }
            let Item::Op(node) = &self.items[i] else { continue };
            let key = format!("{:?}", node);
            match seen.get(&key) {
                Some(&first) => redirects.push((i, first)),
                None => { seen.insert(key, i); },
            }
        }
        let changed = !redirects.is_empty();
        for (i, first) in redirects {
            self.redirect(i, Ref::Local(first), dead);
        }
        changed
    }

    /// Drop dead items and remap every item-indexed `Ref` to the compacted
    /// positions. Dead items have no remaining references (their uses were
    /// redirected when they were marked).
    fn compact(&mut self, dead: &[bool]) {
        let mut remap = vec![usize::MAX; self.items.len()];
        let mut next = 0;
        for (i, &d) in dead.iter().enumerate() {
            if !d { remap[i] = next; next += 1; }
        }
        let mut keep = dead.iter().map(|d| !d);
        self.items.retain(|_| keep.next().unwrap());
        self.for_each_ref(|r| match r {
            Ref::Local(i) | Ref::ChildExport(i, _) => {
                assert!(remap[*i] != usize::MAX, "compact: a reference points at a dead item");
                *i = remap[*i];
            },
            Ref::Import(_) | Ref::Var(_) => {},
        });
    }
}

fn fmt_ref(r: &Ref) -> String {
    match r {
        Ref::Local(i) => format!("n{}", i),
        Ref::Import(k) => format!("in{}", k),
        Ref::Var(v) => format!("v{}", v),
        Ref::ChildExport(c, j) => format!("n{}::{}", c, j),
    }
}

fn dump_scope(s: &Scope, indent: usize) {
    let pad = "  ".repeat(indent);
    println!("{}{}: {{", pad, if s.name.is_empty() { "scope" } else { &s.name });
    dump_scope_body(s, indent);
    println!("{}}}", pad);
}

fn dump_scope_inner(s: &Scope, indent: usize) {
    // As `dump_scope`, but the caller began the opening line.
    println!("{}: {{", if s.name.is_empty() { "scope" } else { &s.name });
    dump_scope_body(s, indent);
    println!("{}}}", "  ".repeat(indent));
}

fn dump_scope_body(s: &Scope, indent: usize) {
    let pad2 = "  ".repeat(indent + 1);
    for (k, imp) in s.imports.iter().enumerate() {
        let from = match &imp.from {
            Source::Parent(r) => fmt_ref(r),
            Source::Input(i) => format!("input {}", i),
            Source::Trace(n) => format!("import {:?}", n),
        };
        println!("{}in{} = {} ({:?});", pad2, k, from, imp.name);
    }
    for (v, var) in s.vars.iter().enumerate() {
        println!("{}var v{} ({:?});", pad2, v, var.name);
    }
    for (i, item) in s.items.iter().enumerate() {
        match item {
            Item::Op(node) => {
                let desc = match node {
                    Node::Linear { input, ops } => {
                        let ops: Vec<&str> = ops.iter().map(|op| match op {
                            LinearOp::Project(_) => "project", LinearOp::Filter(_) => "filter",
                            LinearOp::Negate => "negate", LinearOp::EnterAt(_) => "enter_at",
                            LinearOp::LiftIter => "lift_iter", LinearOp::FlatMap(_) => "flatmap",
                        }).collect();
                        format!("{} | {}", fmt_ref(input), ops.join(" | "))
                    }
                    Node::Concat(refs) => refs.iter().map(fmt_ref).collect::<Vec<_>>().join(" + "),
                    Node::Arrange(r) => format!("{} | arrange", fmt_ref(r)),
                    Node::Join { left, right, .. } => format!("join({}, {})", fmt_ref(left), fmt_ref(right)),
                    Node::Reduce { input, reducer } => format!("{} | {:?}", fmt_ref(input), reducer),
                    Node::Inspect { input, label } => format!("{} | inspect({})", fmt_ref(input), label),
                };
                println!("{}n{} = {};", pad2, i, desc);
            }
            Item::Sub(child) => {
                print!("{}n{} = ", pad2, i);
                dump_scope_inner(child, indent + 1);
            }
        }
    }
    for b in &s.binds {
        println!("{}bind v{} = {};", pad2, b.var, fmt_ref(&b.value));
    }
    for e in &s.exports {
        println!("{}export {:?} = {};", pad2, e.name, fmt_ref(&e.value));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse::Term;

    // Express a reach-shaped program by hand: it exercises items (Op + Sub),
    // every `Ref` variant, and both relevant `Source` variants — proving the
    // shape can represent a real scoped, iterative program.
    #[test]
    fn expresses_a_scoped_iterative_program() {
        let noproj = Projection { key: Term::Tuple(vec![]), val: Term::Tuple(vec![]) };
        let inner = Scope {
            name: "reach".into(),
            imports: vec![
                Import { name: "edges".into(), from: Source::Parent(Ref::Import(0)) },
                Import { name: "roots".into(), from: Source::Parent(Ref::Import(1)) },
            ],
            vars: vec![Var { name: "reach".into() }],
            items: vec![
                // proposals = reach JOIN edges  — references Var + Import
                Item::Op(Node::Join { left: Ref::Var(0), right: Ref::Import(0), projection: noproj.clone() }),
                // body = roots + proposals       — references Import + Local
                Item::Op(Node::Concat(vec![Ref::Import(1), Ref::Local(0)])),
            ],
            binds: vec![Bind { var: 0, value: Ref::Local(1) }], // reach <- body
            exports: vec![Export { name: "reach".into(), value: Ref::Var(0) }],
        };
        let root = Scope {
            name: "root".into(),
            imports: vec![
                Import { name: "in0".into(), from: Source::Input(0) },
                Import { name: "in1".into(), from: Source::Input(1) },
            ],
            items: vec![Item::Sub(inner)], // item 0 = the reach sub-scope
            exports: vec![Export { name: "result".into(), value: Ref::ChildExport(0, 0) }],
            ..Scope::default()
        };
        let prog = Program { root };

        assert_eq!(prog.root.items.len(), 1);
        assert!(matches!(prog.root.items[0], Item::Sub(_)));
        let Item::Sub(reach) = &prog.root.items[0] else { unreachable!() };
        assert_eq!(reach.vars.len(), 1);
        assert!(matches!(prog.root.exports[0].value, Ref::ChildExport(0, 0)));
        assert!(matches!(prog.root.imports[0].from, Source::Input(0)));
        assert!(matches!(reach.binds[0].value, Ref::Local(1)));
    }
}
